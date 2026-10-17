//! The pyo3 dressing - **conversion, and nothing else**.
//!
//! Behind the `python` feature. This file is only reachable after a
//! `maturin develop`: `cargo test` never runs it, which is exactly why it
//! decides nothing. Everything that can be wrong lives in [`crate::session`].
//!
//! The pyo3/numpy 0.29 calls follow `energyml-python`'s `bindings.rs`
//! (`detach`, `Bound`, `into_pyarray`). `scripts/wheel-smoke.py` calls into
//! this file through an installed wheel, with no server.

use numpy::IntoPyArray;
use std::sync::{Mutex, MutexGuard};

use energyml_etp_store::pack::{Packed, Wanted};
use energyml_storage::array::ArrayValues as StoredValues;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use retp_client::Transaction;
use retp_client::arrays::Decoded;
use retp_client::notifications::uuid_text;

use crate::session::{
    ArrayInfos, ArrayRead, ChannelRecords, ConnectOptions, DataspaceRow, Failure, Kind, Objects, Outcomes,
    PushReport, Resources, Session, SupportedTypeRow,
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
pub(crate) fn failed(failure: Failure) -> PyErr {
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

/// `{uri: failure}` for the URIs of a plural call that did not go through.
fn errors_into_py<'py>(py: Python<'py>, failed: &[(String, Failure)]) -> PyResult<Bound<'py, PyDict>> {
    let errors = PyDict::new(py);
    for (uri, failure) in failed {
        errors.set_item(uri, failure_into_py(py, failure)?)?;
    }
    Ok(errors)
}

/// `([uri, ...], errors)`: the URIs the store took, in the order asked.
fn taken_into_py<'py>(
    py: Python<'py>,
    outcomes: &Outcomes<String>,
) -> PyResult<(Vec<String>, Bound<'py, PyDict>)> {
    let done = outcomes.done.iter().map(|(uri, _)| uri.clone()).collect();
    Ok((done, errors_into_py(py, &outcomes.failed)?))
}

fn supported_type_into_py<'py>(py: Python<'py>, one: &SupportedTypeRow) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("data_object_type", &one.data_object_type)?;
    out.set_item("object_count", one.object_count)?;
    out.set_item("relationship_kind", &one.relationship_kind)?;
    Ok(out)
}

/// A stored array's values into numpy, flat, by copy - as [`values_into_py`].
fn stored_values_into_py(py: Python<'_>, values: StoredValues) -> PyResult<Py<PyAny>> {
    Ok(match values {
        StoredValues::F64(v) => v.into_pyarray(py).into_any().unbind(),
        StoredValues::F32(v) => v.into_pyarray(py).into_any().unbind(),
        StoredValues::I64(v) => v.into_pyarray(py).into_any().unbind(),
        StoredValues::I32(v) => v.into_pyarray(py).into_any().unbind(),
        StoredValues::I16(v) => v.into_pyarray(py).into_any().unbind(),
        StoredValues::I8(v) => v.into_pyarray(py).into_any().unbind(),
        StoredValues::U64(v) => v.into_pyarray(py).into_any().unbind(),
        StoredValues::U32(v) => v.into_pyarray(py).into_any().unbind(),
        StoredValues::U16(v) => v.into_pyarray(py).into_any().unbind(),
        StoredValues::U8(v) => v.into_pyarray(py).into_any().unbind(),
        // `ArrayValues` is `#[non_exhaustive]`: a kind of values a later
        // `energyml-storage` adds is refused by name, not dropped.
        other => {
            return Err(Unsupported::new_err(format!(
                "{} arrays do not cross as numpy arrays in this bridge yet",
                other.element_type()
            )));
        }
    })
}

/// A packed dataspace: `{"epc": bytes, "objects", "arrays": [...], "left_out",
/// "taken"}`.
fn packed_into_py(py: Python<'_>, packed: Packed) -> PyResult<Bound<'_, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("epc", PyBytes::new(py, &packed.epc))?;
    out.set_item("objects", packed.objects)?;
    let arrays = PyList::empty(py);
    for array in packed.arrays {
        let row = PyDict::new(py);
        row.set_item("file", &array.file)?;
        row.set_item("path", &array.path)?;
        row.set_item("dimensions", &array.dimensions)?;
        row.set_item("values", stored_values_into_py(py, array.values)?)?;
        arrays.append(row)?;
    }
    out.set_item("arrays", arrays)?;
    out.set_item("left_out", &packed.left_out)?;
    let taken = PyDict::new(py);
    for (uri, when) in &packed.taken {
        taken.set_item(uri, when)?;
    }
    out.set_item("taken", taken)?;
    Ok(out)
}

/// A transaction a store opened (`Session.start_transaction`). Hand it back to
/// `Session.commit_transaction` or `Session.rollback_transaction`: nothing
/// else ends it, and nothing ends it on its own.
#[pyclass(name = "Transaction", frozen)]
pub struct PyTransaction {
    inner: Transaction,
}

#[pymethods]
impl PyTransaction {
    /// The uuid the store gave it, as text.
    #[getter]
    fn uuid(&self) -> String {
        uuid_text(&self.inner.uuid)
    }

    /// The dataspace URIs it spans.
    #[getter]
    fn dataspaces(&self) -> Vec<String> {
        self.inner.dataspaces.clone()
    }

    /// Whether it only reads.
    #[getter]
    fn read_only(&self) -> bool {
        self.inner.read_only
    }

    fn __repr__(&self) -> String {
        format!(
            "Transaction(uuid={}, dataspaces={:?}, read_only={})",
            uuid_text(&self.inner.uuid),
            self.inner.dataspaces,
            self.inner.read_only
        )
    }
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

    /// Run `f` on the live session; refused by name on any other (REST).
    pub(crate) fn with_live<R>(
        &self,
        f: impl FnOnce(&retp_client::live::LiveSession) -> R,
    ) -> Result<R, Failure> {
        let session = self.shared();
        match session.live() {
            Some(live) => Ok(f(live)),
            None => Err(Failure {
                kind: crate::session::Kind::Unsupported,
                message: "subscribe needs an ETP WebSocket session: the REST API sends no notifications"
                    .to_string(),
                code: None,
            }),
        }
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

    /// Subscribe to the changes of `uri` (StoreNotification). `to` is where they go as they
    /// arrive: a path (one JSON line each), a callable (called with a dict), or nothing - then
    /// iterate the returned subscription. Requests go on meanwhile.
    #[pyo3(signature = (uri, *, depth = 1, scope = "targetsOrSelf", types = None, with_data = false, to = None))]
    #[allow(clippy::too_many_arguments)]
    fn subscribe(
        slf: Py<Self>,
        py: Python<'_>,
        uri: &str,
        depth: i32,
        scope: &str,
        types: Option<Vec<String>>,
        with_data: bool,
        to: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<crate::notifications::PySubscription> {
        crate::notifications::subscribe(
            py,
            &slf,
            uri,
            depth,
            scope,
            types.unwrap_or_default(),
            with_data,
            to,
        )
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

    /// Delete objects by URI: `(deleted, errors)`. `deleted` maps each URI
    /// the store deleted to the list of URIs it says it deleted with it
    /// (contained objects included); `errors` maps a URI it did not delete to
    /// `{"kind", "code", "message"}`. `prune_contained` asks the store to drop
    /// the contained objects the deletion would orphan. Needs a session opened
    /// with `writing=True`.
    #[pyo3(signature = (uris, *, prune_contained = false))]
    fn delete_objects<'py>(
        &mut self,
        py: Python<'py>,
        uris: Vec<String>,
        prune_contained: bool,
    ) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyDict>)> {
        let outcomes = py
            .detach(|| self.session().delete_objects(&uris, prune_contained))
            .map_err(failed)?;
        let deleted = PyDict::new(py);
        for (uri, with_it) in &outcomes.done {
            deleted.set_item(uri, with_it)?;
        }
        Ok((deleted, errors_into_py(py, &outcomes.failed)?))
    }

    /// Create or replace dataspaces, each given by its path (`demo`) or URI:
    /// `(created, errors)`, `created` the URIs the store took, in the order
    /// given. `custom_data` goes with each, every value as the command line's
    /// `--custom KEY=VALUE` reads it: `"a,b"` is a list of strings, any other
    /// text one string (an OSDU store reads its ACLs and legal tags there).
    /// Needs a session opened with `writing=True`.
    #[pyo3(signature = (dataspaces, *, custom_data = None))]
    fn put_dataspaces<'py>(
        &mut self,
        py: Python<'py>,
        dataspaces: Vec<String>,
        custom_data: Option<std::collections::BTreeMap<String, String>>,
    ) -> PyResult<(Vec<String>, Bound<'py, PyDict>)> {
        let custom: Vec<(String, String)> = custom_data.unwrap_or_default().into_iter().collect();
        let outcomes = py
            .detach(|| self.session().put_dataspaces(&dataspaces, &custom))
            .map_err(failed)?;
        taken_into_py(py, &outcomes)
    }

    /// Delete dataspaces, each given by its path or URI: `(deleted, errors)`,
    /// in the order given. Needs a session opened with `writing=True`.
    fn delete_dataspaces<'py>(
        &mut self,
        py: Python<'py>,
        dataspaces: Vec<String>,
    ) -> PyResult<(Vec<String>, Bound<'py, PyDict>)> {
        let outcomes = py
            .detach(|| self.session().delete_dataspaces(&dataspaces))
            .map_err(failed)?;
        taken_into_py(py, &outcomes)
    }

    /// Open a transaction over dataspaces (paths or URIs; none means the
    /// store's default). Returns a `Transaction`, ended only by
    /// `commit_transaction` or `rollback_transaction`. A store that refuses
    /// raises `ServerError`; a store without transactions raises
    /// `Unsupported`. Needs a session opened with `writing=True`.
    #[pyo3(signature = (dataspaces = None, *, read_only = false, message = ""))]
    fn start_transaction(
        &mut self,
        py: Python<'_>,
        dataspaces: Option<Vec<String>>,
        read_only: bool,
        message: &str,
    ) -> PyResult<PyTransaction> {
        let dataspaces = dataspaces.unwrap_or_default();
        let inner = py
            .detach(|| self.session().start_transaction(&dataspaces, read_only, message))
            .map_err(failed)?;
        Ok(PyTransaction { inner })
    }

    /// Commit a transaction: everything written in it becomes visible, or
    /// none of it. A refusal raises `ServerError`, and then the writes are not
    /// in the store; a connection lost meanwhile raises `OutcomeUnknown`.
    fn commit_transaction(&mut self, py: Python<'_>, transaction: PyRef<'_, PyTransaction>) -> PyResult<()> {
        let transaction = transaction.inner.clone();
        py.detach(|| self.session().commit_transaction(&transaction))
            .map_err(failed)
    }

    /// Roll a transaction back: nothing written in it stays. A refusal raises
    /// `ServerError`, and then its writes may still be there.
    fn rollback_transaction(
        &mut self,
        py: Python<'_>,
        transaction: PyRef<'_, PyTransaction>,
    ) -> PyResult<()> {
        let transaction = transaction.inner.clone();
        py.detach(|| self.session().rollback_transaction(&transaction))
            .map_err(failed)
    }

    /// The data object types under a dataspace (a path or a URI) or an
    /// object, as dictionaries: `data_object_type`, `object_count` (when
    /// `count`), `relationship_kind`. `scope` is `targets` or `sources`;
    /// `empty` also lists the types the store supports and holds none of.
    #[pyo3(signature = (dataspace = "", *, scope = "targets", count = false, empty = false))]
    fn supported_types<'py>(
        &mut self,
        py: Python<'py>,
        dataspace: &str,
        scope: &str,
        count: bool,
        empty: bool,
    ) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let rows = py
            .detach(|| self.session().supported_types(dataspace, scope, count, empty))
            .map_err(failed)?;
        rows.iter().map(|one| supported_type_into_py(py, one)).collect()
    }

    /// The channels at these URIs (a log, a channel set, a channel):
    /// `(found, errors)`. `found` maps a URI to its `ChannelMetadataRecord`,
    /// a dictionary in the ETP Avro JSON encoding (`id`, `channelName`,
    /// `uom`, `indexes`...); the `id` is what a range asks for.
    fn channel_metadata<'py>(
        &mut self,
        py: Python<'py>,
        uris: Vec<String>,
    ) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyDict>)> {
        let ChannelRecords { found, failed: bad } = py
            .detach(|| self.session().channel_metadata(&uris))
            .map_err(failed)?;
        let records = PyDict::new(py);
        for (uri, json) in &found {
            records.set_item(uri, loads(py, json)?)?;
        }
        Ok((records, errors_into_py(py, &bad)?))
    }

    /// Channel values over index intervals: each range a dictionary (or its
    /// JSON text) in the ETP Avro JSON encoding of `ChannelRangeInfo`
    /// (`channelIds`, `interval`, `secondaryIntervals`); the answer a list of
    /// `DataItem` dictionaries in the same encoding. A refusal raises
    /// `ServerError`.
    fn get_ranges<'py>(
        &mut self,
        py: Python<'py>,
        ranges: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<Vec<Bound<'py, PyAny>>> {
        let texts = ranges
            .iter()
            .map(|one| dumps(py, one))
            .collect::<PyResult<Vec<_>>>()?;
        let items = py.detach(|| self.session().get_ranges(&texts)).map_err(failed)?;
        items.iter().map(|json| loads(py, json)).collect()
    }

    /// A dataspace (a path or a URI) as an EPC, in memory: `{"epc": bytes,
    /// "objects", "arrays", "left_out", "taken"}`. `arrays` lists the arrays
    /// the objects name, each `{"file", "path", "dimensions", "values"}`
    /// (`values` a flat numpy array), for the caller to write into the HDF5
    /// file the package names; `left_out` says what could not be taken;
    /// `taken` maps each listed URI to its `storeLastWrite`. `types` and
    /// `uris` narrow what is taken; `references` also takes what the kept
    /// objects reference, while the dataspace holds it.
    #[pyo3(signature = (dataspace = "", *, types = None, uris = None, arrays = true, references = false))]
    fn download_epc<'py>(
        &mut self,
        py: Python<'py>,
        dataspace: &str,
        types: Option<Vec<String>>,
        uris: Option<Vec<String>>,
        arrays: bool,
        references: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        let wanted = Wanted {
            types: types.unwrap_or_default(),
            uris: uris.unwrap_or_default(),
            arrays,
            references,
        };
        let packed = py
            .detach(|| self.session().download_epc(dataspace, &wanted))
            .map_err(failed)?;
        packed_into_py(py, packed)
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
    module.add_class::<crate::notifications::PySubscription>()?;
    module.add_class::<PyTransaction>()?;
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
