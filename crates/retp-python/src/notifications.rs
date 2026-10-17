//! `Session.subscribe` - **conversion, and nothing else**.
//!
//! Behind the `python` feature, like [`crate::bindings`]. The subscription itself is the
//! live session's (`LiveSession::subscribe_to`), and what an arrival looks like is
//! `retp_client::live::destination::as_json`'s: this file turns that JSON into a Python
//! dictionary, hands it to a callable or a queue, and releases the GIL while it waits.
//!
//! Where an arrival goes is chosen by `to`:
//!
//! - `None`: a bounded queue the returned [`PySubscription`] reads (`for event in sub`,
//!   `sub.get(timeout)`);
//! - a `str` or an `os.PathLike`: a JSON Lines file (`JsonLines::create`, truncating);
//! - a callable: called with the dictionary of each arrival, on the session's dispatcher
//!   thread. A Python exception it raises ends that destination (the session says so
//!   once); it never panics.
//!
//! It follows pyo3 0.29 as the crate locks it (`Python::attach`, `Python::detach`, `Py::get`,
//! `Python::check_signals`).

use std::path::PathBuf;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use retp_client::live::destination::as_json;
use retp_client::live::{Destination, JsonLines, Queue, RouteId};
use retp_client::native::random_uuid;
use retp_client::notifications::{Watch, uuid_text};
use retp_proto::connection::Event;
use retp_types::datatypes::Uuid;

use crate::bindings::{PySession, failed};
use crate::session::{Failure, Kind, parse_scope};

/// How many arrivals a subscription without a destination keeps for the reader before the
/// newest are dropped and counted (`Subscription.lost`).
const QUEUE_CAPACITY: usize = 1024;

/// The longest the GIL stays released in one wait: after this the interpreter is asked
/// whether a signal (Ctrl-C) is pending, so that a blocked `get()` can be interrupted.
const SLICE: Duration = Duration::from_millis(100);

/// Where `to` says the arrivals go.
#[derive(Debug, PartialEq, Eq)]
enum Target {
    /// A queue the subscription reads.
    Queue,
    /// A JSON Lines file.
    File(PathBuf),
    /// A Python callable.
    Callable,
}

/// Which target `to` names, given what it is: a path (a `str` or an `os.PathLike`), a
/// callable, or nothing. A path wins over a callable. Plain Rust, so that the dispatch is
/// tested with no interpreter.
fn choose(path: Option<PathBuf>, callable: bool, given: bool) -> Result<Target, String> {
    match (path, callable, given) {
        (Some(path), _, _) => Ok(Target::File(path)),
        (None, true, _) => Ok(Target::Callable),
        (None, false, false) => Ok(Target::Queue),
        (None, false, true) => Err(
            "`to` must be None (read the subscription), a path (a JSON Lines file) or a callable".to_string(),
        ),
    }
}

/// The target of `to`.
fn target_of(to: Option<&Bound<'_, PyAny>>) -> PyResult<Target> {
    let Some(to) = to.filter(|to| !to.is_none()) else {
        return choose(None, false, false).map_err(PyTypeError::new_err);
    };
    // `PathBuf` extraction goes through `os.fspath`: a `str` and any `os.PathLike`.
    let path = to.extract::<PathBuf>().ok();
    choose(path, to.is_callable(), true).map_err(PyTypeError::new_err)
}

/// The JSON text of an arrival (`as_json`, written by the Rust library) as the Python
/// object `json.loads` makes of it: Python's own decoder, not a second conversion (the way
/// `bindings.rs` turns EnergyML JSON into dictionaries).
fn to_python<'py>(py: Python<'py>, json: &str) -> PyResult<Bound<'py, PyAny>> {
    py.import("json")?.call_method1("loads", (json,))
}

/// A [`Destination`] that calls a Python callable with the dictionary of each arrival.
pub struct PyCallback {
    callable: Py<PyAny>,
}

impl PyCallback {
    /// Call `callable` for each arrival.
    pub fn new(callable: Py<PyAny>) -> PyCallback {
        PyCallback { callable }
    }
}

impl Destination for PyCallback {
    /// Takes the GIL, builds the dictionary, calls. A Python exception is returned as its
    /// text (the session then removes this destination and says so once), never raised here.
    fn deliver(&self, event: &Event) -> Result<(), String> {
        let json = as_json(event).to_string();
        Python::attach(|py| {
            let arrival = to_python(py, &json).map_err(|e| format!("cannot build the arrival: {e}"))?;
            self.callable
                .call1(py, (arrival,))
                .map(|_| ())
                .map_err(|e| format!("the subscription callback raised: {e}"))
        })
    }
}

/// A subscription to notifications on a live session.
///
/// Opened by `Session.subscribe`. With no destination it is an iterator of the arrivals
/// (dictionaries, the JSON of the message: `received`, `protocol`, `message`,
/// `subscription`, `body`), blocking until one comes:
///
///     with session.subscribe(uri) as sub:
///         for event in sub:
///             print(event["message"])
///
/// With a path or a callable it only holds the subscription: `close()` ends it.
#[pyclass(name = "Subscription")]
pub struct PySubscription {
    /// The subscription's uuid, as text.
    id: String,
    /// The context it watches.
    uri: String,
    /// The session it runs on, to end it.
    session: Py<PySession>,
    /// What ending it takes; `None` once ended.
    open: Mutex<Option<(Uuid, RouteId)>>,
    /// The queue's reading end, when no destination was given. A receiver is `Send` but not
    /// `Sync`, which a `#[pyclass]` must be: the mutex.
    arrivals: Option<Mutex<Receiver<Event>>>,
    /// The queue's counters, without keeping its sender alive: when the route goes, the
    /// reader sees the end of the stream.
    queue: Weak<Queue>,
}

impl PySubscription {
    fn open(&self) -> MutexGuard<'_, Option<(Uuid, RouteId)>> {
        self.open.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The next arrival within `timeout` (`None`: no limit), the GIL released while it
    /// waits, a pending signal raised between slices. `Ok(None)` at the timeout or at the end
    /// of the stream.
    fn next_within(&self, py: Python<'_>, timeout: Option<Duration>) -> PyResult<Option<Event>> {
        let Some(arrivals) = &self.arrivals else {
            return Err(PyTypeError::new_err(
                "this subscription sends its arrivals to a file or a callable; there is nothing to read",
            ));
        };
        let until = timeout.map(|t| Instant::now() + t);
        loop {
            let slice = match until {
                Some(until) => until.saturating_duration_since(Instant::now()).min(SLICE),
                None => SLICE,
            };
            let step = py.detach(|| {
                arrivals
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .recv_timeout(slice)
            });
            match step {
                Ok(event) => return Ok(Some(event)),
                Err(RecvTimeoutError::Disconnected) => return Ok(None),
                Err(RecvTimeoutError::Timeout) => {
                    py.check_signals()?;
                    if until.is_some_and(|until| Instant::now() >= until) {
                        return Ok(None);
                    }
                }
            }
        }
    }
}

#[pymethods]
impl PySubscription {
    /// The subscription's uuid, as text.
    #[getter]
    fn id(&self) -> String {
        self.id.clone()
    }

    /// The context URI it watches.
    #[getter]
    fn uri(&self) -> String {
        self.uri.clone()
    }

    /// Whether `close()` was called (or the session ended it).
    #[getter]
    fn closed(&self) -> bool {
        self.open().is_none()
    }

    /// How many arrivals were dropped because the reader was behind (0 when the arrivals
    /// go to a file or a callable).
    #[getter]
    fn lost(&self) -> u64 {
        self.queue.upgrade().map_or(0, |queue| queue.lost())
    }

    /// End the subscription (`UnsubscribeNotifications`) and its route. Closing twice does
    /// nothing. A refusal by the server is raised as `ServerError`.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let Some((uuid, route)) = self.open().take() else {
            return Ok(());
        };
        // `Session` is not frozen (its methods take `&mut self`): borrowed, and the
        // borrow kept while the GIL is released - one call at a time per session.
        let guard = self.session.borrow(py);
        let session: &PySession = &guard;
        let outcome = py.detach(|| session.with_live(|live| live.unsubscribe_from(uuid, route)));
        match outcome.map_err(failed)? {
            Err(client) => Err(failed(Failure::from(client))),
            Ok(Err(refusal)) => Err(failed(Failure {
                kind: Kind::Server,
                message: refusal.message,
                code: Some(refusal.code),
            })),
            Ok(Ok(_)) => Ok(()),
        }
    }

    /// The next arrival as a dictionary, waiting at most `timeout` seconds (for ever when
    /// `None`) with the GIL released; `None` when the time ran out or the stream ended.
    #[pyo3(signature = (timeout = None))]
    fn get<'py>(&self, py: Python<'py>, timeout: Option<f64>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let timeout = match timeout {
            None => None,
            Some(seconds) => Some(Duration::try_from_secs_f64(seconds).map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("`timeout` must be a non-negative number of seconds")
            })?),
        };
        match self.next_within(py, timeout)? {
            Some(event) => Ok(Some(to_python(py, &as_json(&event).to_string())?)),
            None => Ok(None),
        }
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// The next arrival; blocks until one comes. `StopIteration` when the stream ended.
    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match self.next_within(py, None)? {
            Some(event) => Ok(Some(to_python(py, &as_json(&event).to_string())?)),
            None => Ok(None),
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_args))]
    fn __exit__(&self, py: Python<'_>, _args: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!("Subscription(id={}, uri={})", self.id, self.uri)
    }
}

/// Subscribe to the notifications of `uri` on a live session.
///
/// `scope` is one of the five ETP names (`parse_scope`); `to` as the module doc says. Refuses
/// by name on a session that is not live (`PySession::with_live`). The GIL is released
/// around the network call.
#[allow(clippy::too_many_arguments)]
pub fn subscribe(
    py: Python<'_>,
    session: &Py<PySession>,
    uri: &str,
    depth: i32,
    scope: &str,
    types: Vec<String>,
    with_data: bool,
    to: Option<&Bound<'_, PyAny>>,
) -> PyResult<PySubscription> {
    let target = target_of(to)?;
    let scope = parse_scope(scope).map_err(failed)?;
    let (destination, arrivals, queue): (Arc<dyn Destination>, _, _) = match &target {
        Target::Queue => {
            let (queue, receiver) = Queue::bounded(QUEUE_CAPACITY);
            let queue = Arc::new(queue);
            (
                Arc::clone(&queue) as Arc<dyn Destination>,
                Some(Mutex::new(receiver)),
                Arc::downgrade(&queue),
            )
        }
        Target::File(path) => {
            let file = JsonLines::create(path).map_err(|e| {
                failed(Failure {
                    kind: Kind::Local,
                    message: format!("cannot create {}: {e}", path.display()),
                    code: None,
                })
            })?;
            (Arc::new(file) as Arc<dyn Destination>, None, Weak::new())
        }
        Target::Callable => {
            let callable = to.expect("a callable target has a `to`").clone().unbind();
            (
                Arc::new(PyCallback::new(callable)) as Arc<dyn Destination>,
                None,
                Weak::new(),
            )
        }
    };
    let mut watch = Watch::of(uri, random_uuid());
    watch.depth = depth;
    watch.scope = scope;
    watch.data_object_types = types;
    watch.include_object_data = with_data;
    let uuid = watch.request_uuid;

    let guard = session.borrow(py);
    let owner: &PySession = &guard;
    let outcome = py.detach(|| owner.with_live(|live| live.subscribe_to(&watch, destination)));
    let route = match outcome.map_err(failed)? {
        Err(client) => return Err(failed(Failure::from(client))),
        Ok(Err(refusal)) => {
            return Err(failed(Failure {
                kind: Kind::Server,
                message: refusal.message,
                code: Some(refusal.code),
            }));
        }
        Ok(Ok(route)) => route,
    };
    Ok(PySubscription {
        id: uuid_text(&uuid),
        uri: uri.to_string(),
        session: session.clone_ref(py),
        open: Mutex::new(Some((uuid, route))),
        arrivals,
        queue,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_is_a_queue() {
        assert_eq!(choose(None, false, false), Ok(Target::Queue));
    }

    #[test]
    fn a_path_is_a_file_even_when_it_is_also_callable() {
        assert_eq!(
            choose(Some(PathBuf::from("out.jsonl")), false, true),
            Ok(Target::File(PathBuf::from("out.jsonl")))
        );
        assert_eq!(
            choose(Some(PathBuf::from("out.jsonl")), true, true),
            Ok(Target::File(PathBuf::from("out.jsonl")))
        );
    }

    #[test]
    fn a_callable_is_a_callable() {
        assert_eq!(choose(None, true, true), Ok(Target::Callable));
    }

    #[test]
    fn anything_else_is_refused_by_name() {
        let message = choose(None, false, true).unwrap_err();
        assert!(message.contains("`to` must be"), "{message}");
    }
}
