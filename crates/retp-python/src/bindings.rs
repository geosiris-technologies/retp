//! The pyo3 dressing - **conversion, and nothing else**.
//!
//! Behind the `python` feature. This file is only reachable after a
//! `maturin develop`: `cargo test` never runs it, which is exactly why it
//! decides nothing. Everything that can be wrong lives in [`crate::session`].
//!
//! TODO(verify): this file has never been compiled. The pyo3/numpy 0.29 calls
//! follow `energyml-python`'s `bindings.rs` (`detach`, `Bound`, `into_pyarray`);
//! the first build will say which one drifted.

use numpy::IntoPyArray;
use std::sync::{Mutex, MutexGuard};

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use retp_client::arrays::Decoded;

use crate::session::{
    ArrayInfos, ArrayRead, ConnectOptions, DataspaceRow, Failure, Kind, Objects, PushReport, Resources,
    Session,
};

// The exception tree. One class per thing a caller does differently (see
// `session::Kind`), all under `RetpError`.
create_exception!(
    _core,
    RetpError,
    PyException,
    "Base class of every failure of the retp bridge."
);
create_exception!(
    _core,
    Unsupported,
    RetpError,
    "The case is not supported; the message names it."
);
create_exception!(
    _core,
    SessionRefused,
    RetpError,
    "The session could not be opened."
);
create_exception!(
    _core,
    AuthorizationNeeded,
    RetpError,
    "The server wants credentials."
);
create_exception!(
    _core,
    ConnectionLost,
    RetpError,
    "The connection is gone: open a new session."
);
create_exception!(
    _core,
    OutcomeUnknown,
    ConnectionLost,
    "The connection fell while a request was out: whether it was applied is unknown."
);
create_exception!(
    _core,
    NotSent,
    RetpError,
    "The request was not sent; the session is untouched."
);
create_exception!(
    _core,
    ServerError,
    RetpError,
    "The server answered with an error (attribute `code`)."
);
create_exception!(_core, TimedOut, RetpError, "No answer in time.");
create_exception!(
    _core,
    AnswerRefused,
    RetpError,
    "The answer arrived and could not be taken."
);
create_exception!(
    _core,
    LocalError,
    RetpError,
    "An object or a package could not be read or written locally."
);
create_exception!(
    _core,
    PushIncomplete,
    RetpError,
    "A push did not go through in full (attribute `report`)."
);

/// A failure of the session layer becomes the exception of its kind.
fn failed(failure: Failure) -> PyErr {
    let Failure { kind, message, code } = failure;
    let error = match kind {
        Kind::BadArgument => return PyValueError::new_err(message),
        Kind::Unsupported => Unsupported::new_err(message),
        Kind::SessionRefused => SessionRefused::new_err(message),
        Kind::AuthorizationNeeded => AuthorizationNeeded::new_err(message),
        Kind::ConnectionLost => ConnectionLost::new_err(message),
        Kind::OutcomeUnknown => OutcomeUnknown::new_err(message),
        Kind::NotSent => NotSent::new_err(message),
        Kind::Server => ServerError::new_err(message),
        Kind::TimedOut => TimedOut::new_err(message),
        Kind::AnswerRefused => AnswerRefused::new_err(message),
        Kind::Local => LocalError::new_err(message),
    };
    if let Some(code) = code {
        Python::attach(|py| {
            let _ = error.value(py).setattr("code", code);
        });
    }
    error
}

/// `{"code": 11, "message": "..."}`.
fn failure_into_py<'py>(py: Python<'py>, failure: &Failure) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("kind", format!("{:?}", failure.kind))?;
    out.set_item("code", failure.code)?;
    out.set_item("message", &failure.message)?;
    Ok(out)
}

/// An EnergyML JSON text becomes a dictionary through Python's own `json`:
/// rebuilding it key by key through pyo3 would be a second implementation of
/// the conversion (`energyml-rust`'s bridge does the same).
fn loads<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyAny>> {
    py.import("json")?.call_method1("loads", (text,))
}

/// A dictionary (or JSON text already) becomes JSON text.
fn dumps(py: Python<'_>, object: &Bound<'_, PyAny>) -> PyResult<String> {
    match object.extract::<String>() {
        Ok(text) => Ok(text),
        Err(_) => py.import("json")?.call_method1("dumps", (object,))?.extract(),
    }
}

fn dataspace_into_py<'py>(py: Python<'py>, one: &DataspaceRow) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("uri", &one.uri)?;
    out.set_item("path", &one.path)?;
    out.set_item("store_last_write", one.store_last_write)?;
    out.set_item("store_created", one.store_created)?;
    Ok(out)
}

fn resources_into_py<'py>(py: Python<'py>, listed: &Resources) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    let resources = PyList::empty(py);
    for one in &listed.resources {
        let row = PyDict::new(py);
        row.set_item("uri", &one.uri)?;
        row.set_item("name", &one.name)?;
        row.set_item("source_count", one.source_count)?;
        row.set_item("target_count", one.target_count)?;
        row.set_item("last_changed", one.last_changed)?;
        row.set_item("store_last_write", one.store_last_write)?;
        row.set_item("store_created", one.store_created)?;
        resources.append(row)?;
    }
    let edges = PyList::empty(py);
    for edge in &listed.edges {
        let row = PyDict::new(py);
        row.set_item("source_uri", &edge.source_uri)?;
        row.set_item("target_uri", &edge.target_uri)?;
        row.set_item("relationship_kind", &edge.relationship_kind)?;
        edges.append(row)?;
    }
    out.set_item("resources", resources)?;
    out.set_item("edges", edges)?;
    Ok(out)
}

fn report_into_py<'py>(py: Python<'py>, report: &PushReport) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    let islands = PyList::empty(py);
    for island in &report.islands {
        let row = PyDict::new(py);
        row.set_item("state", island.state)?;
        row.set_item("uris", &island.uris)?;
        row.set_item("arrays", island.arrays)?;
        row.set_item("bytes", island.bytes)?;
        islands.append(row)?;
    }
    out.set_item("islands", islands)?;
    out.set_item("sent", report.sent)?;
    out.set_item("complete", report.complete())?;
    out.set_item("left_out", &report.left_out)?;
    out.set_item("dangling", &report.dangling)?;
    out.set_item("without_transactions", report.without_transactions)?;
    out.set_item("stopped", &report.stopped)?;
    Ok(out)
}

/// An array's values into numpy, **flat**, by copy (the reason is
/// `energyml-rust`'s: a view would outlive the memory it looks at).
fn values_into_py(py: Python<'_>, values: Decoded) -> PyResult<Py<PyAny>> {
    Ok(match values {
        Decoded::F64(v) => v.into_pyarray(py).into_any().unbind(),
        Decoded::F32(v) => v.into_pyarray(py).into_any().unbind(),
        Decoded::I64(v) => v.into_pyarray(py).into_any().unbind(),
        Decoded::I32(v) => v.into_pyarray(py).into_any().unbind(),
        Decoded::I16(v) => v.into_pyarray(py).into_any().unbind(),
        Decoded::I8(v) => v.into_pyarray(py).into_any().unbind(),
        Decoded::U64(v) => v.into_pyarray(py).into_any().unbind(),
        Decoded::U32(v) => v.into_pyarray(py).into_any().unbind(),
        Decoded::U16(v) => v.into_pyarray(py).into_any().unbind(),
        Decoded::U8(v) => v.into_pyarray(py).into_any().unbind(),
        Decoded::Booleans(v) => v.into_pyarray(py).into_any().unbind(),
        // `session::Session::read_array` already refuses strings; this arm
        // stays a refusal in case that ever changes.
        Decoded::Strings(_) => {
            return Err(Unsupported::new_err(
                "string arrays do not cross as numpy arrays in this bridge yet",
            ));
        }
    })
}

/// An open session with an ETP server.
///
/// Use it as a context manager so that the session is closed:
///
///     with retp.connect("wss://host/etp", token=token) as session:
///         print(session.dataspaces())
// A `#[pyclass]` must be `Sync`, and a `Session` is `Send` but not `Sync` (the
// client holds its protocol handlers as `Box<dyn Handler>`). The mutex makes it
// shareable; a method that holds `&mut self` reaches it by `get_mut`, without
// locking, since that borrow is already exclusive. One call at a time per
// session is what an ETP connection does anyway.
#[pyclass(name = "Session")]
pub struct PySession {
    inner: Mutex<Session>,
}

impl PySession {
    /// The session, for a method that holds `&mut self`.
    fn session(&mut self) -> &mut Session {
        self.inner
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The session, for a method that holds `&self` (the getters).
    fn shared(&self) -> MutexGuard<'_, Session> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[pymethods]
impl PySession {
    /// The URL the session was opened on.
    #[getter]
    fn url(&self) -> String {
        self.shared().url().to_string()
    }

    /// Whether the session announced that it writes.
    #[getter]
    fn writing(&self) -> bool {
        self.shared().writing()
    }

    /// What the library had to say about the connection: plain text, an
    /// unchecked certificate.
    #[getter]
    fn warnings(&self) -> Vec<String> {
        self.shared().warnings().to_vec()
    }

    /// A Ping and its Pong; the round trip in seconds.
    fn ping(&mut self, py: Python<'_>) -> PyResult<f64> {
        py.detach(|| self.session().ping()).map_err(failed)
    }

    /// End the session. Closing twice does nothing.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.session().close()).map_err(failed)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_args))]
    fn __exit__(&mut self, py: Python<'_>, _args: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<bool> {
        py.detach(|| self.session().close()).map_err(failed)?;
        Ok(false)
    }

    /// The dataspaces of the store, as dictionaries.
    fn dataspaces<'py>(&mut self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let rows = py.detach(|| self.session().dataspaces()).map_err(failed)?;
        rows.iter().map(|one| dataspace_into_py(py, one)).collect()
    }

    /// The resources under a dataspace (a path like `demo`, or a URI) or an
    /// object: `{"resources": [...], "edges": [...]}`.
    ///
    /// `scope` is one of the five ETP names: self, sources, targets,
    /// sourcesOrSelf, targetsOrSelf.
    #[pyo3(signature = (dataspace = "", *, depth = 1, scope = "targets", types = None, edges = false, count = false))]
    // The keyword arguments of one Python call, as ETP's GetResources has them: a
    // struct would make the Python side spell a builder for what is one call.
    #[allow(clippy::too_many_arguments)]
    fn resources<'py>(
        &mut self,
        py: Python<'py>,
        dataspace: &str,
        depth: i32,
        scope: &str,
        types: Option<Vec<String>>,
        edges: bool,
        count: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        let types = types.unwrap_or_default();
        let listed = py
            .detach(|| {
                self.session()
                    .resources(dataspace, depth, scope, types, edges, count)
            })
            .map_err(failed)?;
        resources_into_py(py, &listed)
    }

    /// One object, as a dictionary carrying its `$type`. Raises when the
    /// object cannot be had.
    fn get_object<'py>(&mut self, py: Python<'py>, uri: &str) -> PyResult<Bound<'py, PyAny>> {
        let uris = vec![uri.to_string()];
        let Objects {
            mut found,
            failed: mut bad,
        } = py.detach(|| self.session().get_objects(&uris)).map_err(failed)?;
        if let Some((_, failure)) = bad.pop() {
            return Err(failed(failure));
        }
        match found.pop() {
            Some((_, fetched)) => loads(py, &fetched.json),
            None => Err(AnswerRefused::new_err(
                "the server answered nothing for this object",
            )),
        }
    }

    /// Objects by URI: `(objects, errors)`. `objects` maps a URI to its
    /// dictionary; `errors` maps a URI to `{"kind", "code", "message"}`. A URI
    /// that failed is in `errors`, never silently missing.
    fn get_objects<'py>(
        &mut self,
        py: Python<'py>,
        uris: Vec<String>,
    ) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyDict>)> {
        let answered = py.detach(|| self.session().get_objects(&uris)).map_err(failed)?;
        let objects = PyDict::new(py);
        for (uri, fetched) in &answered.found {
            objects.set_item(uri, loads(py, &fetched.json)?)?;
        }
        let errors = PyDict::new(py);
        for (uri, failure) in &answered.failed {
            errors.set_item(uri, failure_into_py(py, failure)?)?;
        }
        Ok((objects, errors))
    }

    /// Put objects (dictionaries carrying `$type`, or their JSON text) into a
    /// dataspace. Returns the push report; raises `PushIncomplete` (with the
    /// report as its `report` attribute) when the push did not go through in
    /// full. Needs a session opened with `writing=True`.
    fn put_objects<'py>(
        &mut self,
        py: Python<'py>,
        dataspace: &str,
        objects: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let texts = objects
            .iter()
            .map(|one| dumps(py, one))
            .collect::<PyResult<Vec<_>>>()?;
        let report = py
            .detach(|| self.session().put_objects(dataspace, &texts))
            .map_err(failed)?;
        pushed(py, report)
    }

    /// Send the EPC file at `path` (objects and arrays) into a dataspace. The
    /// file is read by Rust: to move an `energyml.rust.Epc`, save it first.
    fn push_epc<'py>(
        &mut self,
        py: Python<'py>,
        path: &str,
        dataspace: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        let report = py
            .detach(|| self.session().push_epc(path, dataspace))
            .map_err(failed)?;
        pushed(py, report)
    }

    /// The metadata of arrays of one object: `(found, errors)`, `found` a list
    /// of `{"path", "dimensions", "logical", "transport",
    /// "preferred_subarray_dimensions"}`.
    fn array_metadata<'py>(
        &mut self,
        py: Python<'py>,
        uri: &str,
        paths: Vec<String>,
    ) -> PyResult<(Vec<Bound<'py, PyDict>>, Bound<'py, PyDict>)> {
        let ArrayInfos { found, failed: bad } = py
            .detach(|| self.session().array_metadata(uri, &paths))
            .map_err(failed)?;
        let mut rows = Vec::new();
        for one in &found {
            let row = PyDict::new(py);
            row.set_item("path", &one.path)?;
            row.set_item("dimensions", &one.dimensions)?;
            row.set_item("logical", &one.logical)?;
            row.set_item("transport", &one.transport)?;
            row.set_item(
                "preferred_subarray_dimensions",
                &one.preferred_subarray_dimensions,
            )?;
            rows.push(row);
        }
        let errors = PyDict::new(py);
        for (path, failure) in &bad {
            errors.set_item(path, failure_into_py(py, failure)?)?;
        }
        Ok((rows, errors))
    }

    /// One array as `(values, dimensions)`: `values` a flat numpy array in the
    /// array's logical type, row-major (`values.reshape(dimensions)` is the
    /// caller's line). With `start` and `count` (one entry per dimension), only
    /// that block - ETP's subarray.
    #[pyo3(signature = (uri, path, *, start = None, count = None))]
    fn read_array(
        &mut self,
        py: Python<'_>,
        uri: &str,
        path: &str,
        start: Option<Vec<u64>>,
        count: Option<Vec<u64>>,
    ) -> PyResult<(Py<PyAny>, Vec<u64>)> {
        let region = match (start, count) {
            (Some(start), Some(count)) => Some((start, count)),
            (None, None) => None,
            _ => return Err(PyValueError::new_err("give both start and count, or neither")),
        };
        let ArrayRead { dimensions, values } = py
            .detach(|| self.session().read_array(uri, path, region))
            .map_err(failed)?;
        Ok((values_into_py(py, values)?, dimensions))
    }

    fn __repr__(&self) -> String {
        {
            let session = self.shared();
            format!("Session({:?}, writing={})", session.url(), session.writing())
        }
    }
}

/// A push's report, raising when it is not complete.
fn pushed(py: Python<'_>, report: PushReport) -> PyResult<Bound<'_, PyDict>> {
    let as_dict = report_into_py(py, &report)?;
    if report.complete() {
        return Ok(as_dict);
    }
    let why = match (&report.stopped, report.left_out.len()) {
        (Some(stopped), 0) => stopped.clone(),
        (Some(stopped), n) => format!("{stopped}; and {n} object(s) were left out"),
        (None, n) => format!("{n} object(s) were left out"),
    };
    let error = PushIncomplete::new_err(format!(
        "the push did not go through in full: {why} ({} of {} island(s) committed)",
        report.sent,
        report.islands.len()
    ));
    error.value(py).setattr("report", &as_dict)?;
    Err(error)
}

/// Connect to an ETP server and open a session.
///
/// `token` is an OAuth 2.0 bearer token, without its scheme. It is handed to
/// the Rust client and kept nowhere else: not in `repr`, not in `url`, not in
/// an exception message.
#[pyfunction]
#[pyo3(signature = (url, token = None, *, partition = None, insecure = false, writing = false, types = None, keepalive = None, at_once = None))]
#[allow(clippy::too_many_arguments)]
fn connect(
    py: Python<'_>,
    url: String,
    token: Option<String>,
    partition: Option<String>,
    insecure: bool,
    writing: bool,
    types: Option<Vec<String>>,
    keepalive: Option<u64>,
    at_once: Option<usize>,
) -> PyResult<PySession> {
    let options = ConnectOptions {
        url,
        token,
        partition,
        insecure,
        writing,
        types: types.unwrap_or_default(),
        keepalive_seconds: keepalive,
        at_once,
    };
    let inner = py.detach(|| Session::open(options)).map_err(failed)?;
    Ok(PySession {
        inner: Mutex::new(inner),
    })
}

/// The compiled core of `retp`.
///
/// Everything is exported flat from here, and `python/retp/*.py` arranges it
/// into named modules (the reason is `energyml-rust`'s: the arrangement lives
/// in Python because that is where it can be read).
#[pymodule]
fn _core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add_class::<PySession>()?;
    module.add_function(wrap_pyfunction!(connect, module)?)?;
    module.add("RetpError", py.get_type::<RetpError>())?;
    module.add("Unsupported", py.get_type::<Unsupported>())?;
    module.add("SessionRefused", py.get_type::<SessionRefused>())?;
    module.add("AuthorizationNeeded", py.get_type::<AuthorizationNeeded>())?;
    module.add("ConnectionLost", py.get_type::<ConnectionLost>())?;
    module.add("OutcomeUnknown", py.get_type::<OutcomeUnknown>())?;
    module.add("NotSent", py.get_type::<NotSent>())?;
    module.add("ServerError", py.get_type::<ServerError>())?;
    module.add("TimedOut", py.get_type::<TimedOut>())?;
    module.add("AnswerRefused", py.get_type::<AnswerRefused>())?;
    module.add("LocalError", py.get_type::<LocalError>())?;
    module.add("PushIncomplete", py.get_type::<PushIncomplete>())?;
    Ok(())
}
