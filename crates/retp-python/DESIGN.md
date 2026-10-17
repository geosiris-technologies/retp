# Design note: the Python bridge of retp

Status: built and published (`retp` 0.2.0 on PyPI since 2026-10-15). A statement
about an API that no test or build exercised still carries a `TODO(verify)`.
Decisions that belong to the maintainers are marked **Decision**.

## 1. What this is for

Python users of ETP (reservoir engineers, data engineers feeding or reading an
RDDMS or any ETP v1.2 server) get a client that is the Rust one: the same wire
rules, the same tolerance, the same refusals, instead of a second protocol
implementation in Python. The earlier pure-Python projects on PyPI (`etpclient`,
`etpproto`, `etptypes`) stay what they are.

The rule that commands the rest: **the bridge carries no feature that Rust does
not have**. It is only a bridge. Each operation is one call into the Rust
library; the bridge converts arguments and results. Where Python needs something
Rust lacks, it is listed in section 9 and added in Rust first.

## 2. Scope

Bound, function by function (Python name, then the Rust item it calls):

| Python | Rust |
|---|---|
| `retp.connect(url, token, partition=, insecure=, writing=, types=, keepalive=, at_once=)` | `retp_client::live::LiveSession::connect` over `reading_client` / `writing_client` and `native::fit_to_server`; `ServerProfile`; `plain_text_warning`, `CertificateCheck::warning` |
| `retp.connect(..., user=, password=, token_url=, azure_tenant=, cognito_region=, client_id=, client_secret=, oauth_scope=)` | `retp_client::login::Choice::login` (given no environment), then `Login::obtain`; a token alone is `Login::Bearer`, so `Credentials::bearer` |
| `retp.connect(..., rest=True)` | `native::rest_client`, `RestClient::with_at_once`; `Remote::Rest` |
| `Session.dataspaces()` | `retp_client::list_dataspaces` |
| `Session.resources(dataspace, depth=, scope=, types=, edges=, count=)` | `retp_client::list_resources` with a `ResourceQuery`; `dataspace_uri` |
| `Session.get_object(uri)`, `get_objects(uris)` | `retp_client::get_data_objects` (format `xml`), then `energyml_etp_store::read_object`, then `energyml_core::json::write_string` |
| `Session.put_objects(dataspace, objects)` | `energyml_core::json::read_string`, `EpcStore::put` (in memory), then `energyml_etp_store::push::push` (see gap G1) |
| `Session.push_epc(path, dataspace)` | `energyml_storage::EpcStore::open`, then `energyml_etp_store::push::push` |
| `Session.array_metadata(uri, paths)` | `retp_client::arrays::array_metadata` |
| `Session.read_array(uri, path, start=, count=)` | `retp_client::arrays::read_arrays`, or `read_regions` with a `Block`; `ArrayValues::decode` |
| `Session.ping()`, `close()` | `Remote::ping`, `Remote::close` |
| `Session.subscribe(uri, ...)` | `retp_client::live::LiveSession::subscribe_to`, `unsubscribe_from`; `live::as_json` |
| `Session.supported_types(dataspace, scope=, count=, empty=)` | `retp_client::list_supported_types`; `dataspace_uri` |
| `Session.delete_objects(uris, prune_contained=)` | `retp_client::delete_data_objects` |
| `Session.put_dataspaces(dataspaces, custom_data=)` | `retp_client::put_dataspaces`; `dataspace_uri`, `EtpUri::parse` for the path, `custom_data_value` |
| `Session.delete_dataspaces(dataspaces)` | `retp_client::delete_dataspaces`; `dataspace_uri` |
| `Session.start_transaction(dataspaces, read_only=, message=)`, `commit_transaction(t)`, `rollback_transaction(t)` | `retp_client::start_transaction`, `commit_transaction`, `rollback_transaction`; `retp.Transaction` holds the library's `Transaction` |
| `Session.channel_metadata(uris)` | `retp_client::channels::channel_metadata`; `retp_avro::Avro::to_json` |
| `Session.get_ranges(ranges)` | `retp_client::channels::get_ranges`; `Avro::from_json_str`, `to_json`; `native::random_uuid` |
| `Session.download_epc(dataspace, types=, uris=, arrays=, references=)` | `energyml_etp_store::pack::pack` |

Each operation added after the first scope is a free function of `session.rs`
over any `retp_client::Blocking` client (`delete_objects`, `create_dataspaces`,
`open_transaction`...), which the `Session` method of the same purpose calls on
its remote, and which `tests/operations.rs` runs on a client whose other end is
a server connection in memory (`retp_client::memory`).

Not bound, though Rust has them: catch-up (`changes_since`) and resumable runs
(`retp_client::resume`, the directory download). Not bound because Rust lacks a
piece first: writing arrays (G2), channel subscriptions and ChannelStreaming
(G11), writing a packed dataspace to disk (G12); section 9.

### Moving objects between `energyml.rust` and a dataspace

The two wheels are two separate native modules; a Rust object cannot cross from
one to the other (no stable Rust ABI, and no shared type). What crosses is what
both already speak:

- **An EPC file to a dataspace** (works with what exists today):
  `epc.save_as("model.epc")` in `energyml.rust`, then
  `session.push_epc("model.epc", "demo")`. Rust reads the file and sends objects
  and arrays, planned in islands of transactions by `push`.
- **Objects, one at a time** (works with what exists today):
  `session.put_objects("demo", [epc.get(uri) for uri in uris])`. The dictionaries
  are the same convention on both sides. Arrays do not follow (they need an
  array-writing binding, gap G2), and dependencies between the objects are the
  caller's business unless they go in the same call.
- **A dataspace to an `Epc`**: `session.get_object(uri)` gives a dictionary that
  `Epc.put` accepts; arrays do not follow until `read_array` is paired with
  `Epc.write_array`, which is the caller's loop (nothing is lost, but nothing is
  planned either). The proper path is `session.download_epc(dataspace)`
  (`energyml_etp_store::pack`): the package's bytes and its arrays, in memory;
  the arrays are written by the caller (G12).
- **In memory, with no file in between**: `energyml.rust` has no
  `Epc.to_bytes()` (gap G3 on that side).

TODO(verify): that a dictionary from `Epc.get` is accepted by `put_objects`
as is (both claim the EnergyML JSON encoding of `energyml_core::json`).

## 3. Blocking, not async

`retp-client` has two faces over one `engine`: a blocking `Client` that waits on
its transport, and an `AsyncClient` fed by whoever holds the socket (the form a
browser needs). Every operation is written once as an `async fn` over `Caller`
and `retp_client::blocking` runs it on the blocking client.

The bridge binds the blocking form:

- Python calls are synchronous; there is no event loop to feed an
  `AsyncClient` from, and writing one in Python would put protocol logic in the
  bridge.
- The bridge needs no async runtime, no thread and no `tokio`: the blocking
  client needs nothing the standard library does not give. That keeps the wheel
  small and its build the same on 8 targets.
- The GIL is released around every call (`Python::detach`), so a Python program
  with threads is not frozen by a slow server. One session serves one request at
  a time; a second thread calling the same session while a request is out gets
  pyo3's "already borrowed" error, not corrupted state.
- For `asyncio` programs: `await asyncio.to_thread(session.dataspaces)`.

What it costs: no concurrent requests on one session from the caller's side
(inside one operation the library already keeps `at_once` requests in flight,
default 4), and no push of unsolicited messages (notifications, channel data)
until an events binding exists. If an `AsyncSession` is ever wanted, it must be
a Rust-side `AsyncClient` fed by a Rust-side socket task, so that the protocol
stays in Rust; that is a project, not a binding.

TODO(verify): that `Session` (so `Remote`, so the client's boxed handlers) is
`Send`. `EtpStore` implements `EnergymlStorage` for `T: Send, C: Send` over a
`Mutex<Client>`, which suggests it is. If not, `#[pyclass(unsendable)]` and no
`detach`.

## 4. Error mapping

`ClientError` (the library's) is classified once, in `session::Failure::from`,
then each `Kind` becomes an exception class in `bindings.rs`:

| Rust | Kind | Python | What the caller does |
|---|---|---|---|
| bad argument (bridge) | `BadArgument` | `ValueError` | fix the call |
| `Invalid`, `Send` | `NotSent` | `NotSent` | fix the request; the session is untouched |
| `Offer`, `Refused` | `SessionRefused` | `SessionRefused` | change the offer or server |
| `AuthorizationNeeded` | `AuthorizationNeeded` | `AuthorizationNeeded` | log in again |
| `Transport`, `Closed` | `ConnectionLost` | `ConnectionLost` | open a new session |
| `Interrupted` | `OutcomeUnknown` | `OutcomeUnknown` (a `ConnectionLost`) | check before retrying a write |
| `TimedOut` | `TimedOut` | `TimedOut` | retry or give up |
| `AnswerRefused` | `AnswerRefused` | `AnswerRefused` | report |
| server `ErrorInfo` (per object or array) | `Server` | `ServerError` with `.code`, or an entry of the `errors` dict | depends on the code |
| unsupported case | `Unsupported` | `Unsupported` | use another route |
| object does not read/write, package does not open | `Local` | `LocalError` | fix the data |
| push not in full | (report) | `PushIncomplete` with `.report` | read the report |
| `LoginError::Invalid` | `BadArgument` | `ValueError` | fix the login |
| `LoginError::Http` | `SessionRefused` | `SessionRefused` | reach the authorization server |
| `LoginError::Refused` | `AuthorizationNeeded` | `AuthorizationNeeded` | log in again |
| `LoginError::Challenge` | `Unsupported` | `Unsupported` | settle it in another client |
| `Opened::Refused`, `Ended::Refused` | `Server` (no code) | `ServerError` | read the store's reason |
| `Opened::Unsupported` | `Unsupported` | `Unsupported` | write without a transaction |

Rules kept:

- The library's own message is kept as is.
- A plural read (`get_objects`, `array_metadata`, `channel_metadata`) returns
  `(found, errors)`, and a plural write that is not a push (`delete_objects`,
  `put_dataspaces`, `delete_dataspaces`) `(done, errors)`: a URI that failed is
  in `errors`, never silently missing. The singular (`get_object`,
  `read_array`) raises. The same URI twice in one keyed request is refused
  (`ValueError`): the answer is keyed by it, and one of the two would read as
  unanswered.
- A listing a server cut short with an error is refused (`ServerError`), not
  returned as a complete list. The partial list is lost by this (gap G5).
- `OutcomeUnknown` keeps the library's statement that the outcome is unknown:
  neither a success (data may be lost) nor a failure (a retry may write twice).
- A server error code is `.code` on the exception; mapping codes to ETP names is
  a Rust-side matter (gap G8).

## 5. Naming (Decision)

**Decided: distribution `retp`, import `retp`.** The reasoning and the alternatives
that were weighed follow.

Reasons: the stack is called `retp` (crates `retp-*`, command `retp`, and the
name is reserved for it); the ETP stack is not specific to EnergyML objects (the
Python user may only list dataspaces); a short, one-word import is what a client
library wants; and it reads as the successor of the earlier `etpclient` family.

Alternatives:

- **`energyml-etp`, import `energyml.etp`**: a namespace sibling of
  `energyml.rust`, consistent for users of the `energyml.*` family and for the
  wheel that carries the objects. Costs: the namespace is shared with
  several distributions that overwrite each other's `energyml/__init__.py`
  (`energyml-rust` avoids it by shipping none; this one would do the same);
  it ties the name of an ETP client to EnergyML; a longer import.
- **`retp-client`, import `retp_client`**: free of any clash with the Rust crate
  names on PyPI, but it repeats `client` and differs from the command name.
- **Two names**: distribution `retp`, with `energyml.etp` as a thin alias
  package. Two ways to import the same thing is a maintenance cost; not
  recommended.

Whatever is chosen, one place to change it in this draft: `name` in
`pyproject.toml`, `module-name` there, the directory `python/retp/`, and the
docstrings. TODO(verify): availability of `retp` on PyPI and TestPyPI before the
first upload (the public documentation says the name is reserved; check again).

The Rust library is named `retp_python` whatever the Python name, to avoid the
Windows `.pdb` clash with `retp-cli` (see `ARCHITECTURE.md`).

## 6. Building and publishing

It follows `energyml-rust`'s wheel pipeline (`.github/workflows/python.yml`, and
`docs/releasing.md`):

- maturin wheels, abi3 (`abi3-py39`: one wheel per platform), 8 targets:
  Linux x86_64 and aarch64 on manylinux_2_28, the same two on musllinux_1_2,
  macOS aarch64 and x86_64, Windows x64 and aarch64; the cross builds are built
  and not smoke-tested; plus an sdist;
- the `dist` profile (thin LTO) for the distributed binaries, as in the workspace
  manifest;
- pull request touching the crates: one Linux wheel, installed and smoke-tested;
- push to `main` or `dev`: the full matrix at a development version
  `X.Y.(Z+1).devN`, published to TestPyPI (environment `testpypi`, trusted
  publishing, gated by a repository variable);
- a published GitHub release, or a `python-vX.Y.Z` tag: the matrix at the crate's
  own version, published to PyPI (environment `pypi`, trusted publishing, gated
  by a repository variable). Nothing is published by creating the repository;
- the version is the workspace's (`dynamic = ["version"]`, read from
  `Cargo.toml`); a development version replaces that line at build time.

What retp's CI will need that it does not have:

1. `crates/retp-python` in the workspace members; its path dependencies on
   `energyml-schemas` and `energyml-storage` must resolve to their crates.io
   versions in the public repository (the other `retp` crates already take
   `energyml-*` from crates.io, 0.1.x). TODO(verify): that the published
   `energyml-storage` has `extract::empty_package`, `EpcStore::from_reader` and
   the `hdf5-static` feature used here.
2. A `python.yml` workflow derived from `energyml-rust`'s, with `MANIFEST` and
   `PYPROJECT` pointing at `crates/retp-python`. The feature list is given in
   full on the command line (`python,pyo3/extension-module`, plus
   `hdf5-static` if kept): a `--features` on the command line replaces
   pyproject's list instead of adding to it, and without `python` maturin builds
   an empty module.
3. A smoke script for the wheel that needs **no server**: import `retp`, check
   the exception tree, check that `retp.connect("https://...")` raises
   `retp.Unsupported`, and that `retp.connect("wss://127.0.0.1:1/")` raises
   `retp.ConnectionLost` (TODO(verify): which exception a refused TCP connection
   gives). A live check against a real server belongs to the existing
   interoperability workflow, not to the wheel smoke test.
4. The Rust tests (`cargo test --workspace --test python`, no server, no
   interpreter) in the ordinary CI; `live-server` never in CI without a server.
5. Python-side tests of the wheel (pytest) once the first build exists: the
   draft has none, because nothing was run. TODO(verify).
6. LICENSE and NOTICE in the crate directory (copied here), because maturin
   writes `License-File` entries for them and the index refuses an archive that
   lacks the files.

**Decision: HDF5 in the wheel or not.** `push_epc` sends the arrays an EPC
references, which the storage layer reads from `.h5` files, so a useful
`push_epc` needs the `hdf5` feature, and the published wheel `hdf5-static`
(HDF5 built from sources and linked in; cmake and a C compiler at build time, as
`energyml-rust`'s wheels do). Without it the wheel is lighter and simpler to
build, but `push_epc` of a package with arrays refuses by name. The draft
mirrors `energyml-rust` (HDF5 on). Risk to check: two wheels with their own
statically linked HDF5 loaded in one process (`energyml.rust` and `retp`).
TODO(verify): that this works on Linux, macOS and Windows.

## 7. Security

- The token is a function argument, consumed into `Credentials` (whose `Debug`
  hides the secret), and kept nowhere in the bridge: not in `repr`, not in
  `url`, not in any message. Tokens belong in an environment variable or a
  secret store, not on a command line; the examples read `RETP_TOKEN`.
- `insecure=True` (no certificate check) is allowed, and said in
  `session.warnings`, as the command line says it. Clear text to a host other
  than this machine is allowed by the specification, and said in the same list.
- Nothing is logged by the bridge. The library logs through the `log` facade; no
  logger is installed here. TODO(verify): whether the bridge should forward `log`
  to Python's `logging` (a Rust-side `log` to `logging` bridge would be a
  dependency, `pyo3-log`; undecided).

## 8. Tests in this draft

- `tests/errors.rs`: each `ClientError` variant gets its kind; the bridge agrees
  with the library's `is_connection_lost`.
- `tests/connect.rs`: URL, token, scope and `at_once` refusals, the warnings,
  that a refused token is never echoed, the REST checks (an HTTP URL needs
  `rest=True`, `keepalive` and `types` mean nothing over REST, a REST session
  opens with no network), and the logins (HTTP Basic, a token first, a login
  lacking a part refused before any request).
- `tests/operations.rs`: deleting objects and dataspaces, creating dataspaces,
  transactions, supported types, channel metadata and ranges, and packing a
  dataspace, on a client whose other end is a server connection in memory.
- `tests/stage.rs`: refusals of staging objects for a push (no object, not JSON,
  no `$type`, unknown type). TODO(verify): the positive test, with an object built
  by the library.
- `tests/scope.rs`: the dependency allow-list.
- `tests/live.rs`: three read-only checks against a real server, behind the
  `live-server` feature and `#[ignore]`.

## 9. Gaps: what Rust must grow first

None of these is worked around in the bridge, except G1, which is composed from
existing Rust and flagged where it is.

- **G1. Put objects without a source package.** `push` takes an
  `EnergymlStorage` as its source; there is no function that takes objects and
  a dataspace. `Session::put_objects` stages the objects in an in-memory
  `EpcStore` and pushes that. A direct `energyml_etp_store::put_objects(client,
  dataspace, objects, options)` would remove the staging, give the CLI and the
  front end the same entry, and say what an object put alone should do with its
  dangling references.
- **G2. Writing an array from numbers.** `retp_client::arrays::write_arrays`
  takes an `Array` (dimensions, logical type, transport type, `ArrayValues`).
  The one conversion from stored numbers to that `Array` exists, as
  `energyml_etp_store::push::to_wire` (`push` and the writer call it), but it is
  `pub(crate)`; the public `convert::arrays::array_data_to_etp` gives a
  `DataArray` without the logical type, and turning its union back into
  `ArrayValues` is private too. Rebuilding either in the bridge would be a second
  spelling of how a number is widened. Making `to_wire` public (or an
  `Array::from_stored(&ArrayData)`) lifts this; until then `write_array` is not
  bound.
- **G3. Bytes of an EPC.** `energyml.rust.Epc` has no save-to-bytes, and
  `push` only reads an `EnergymlStorage` that lives in the same module, so the
  hand-off between the two wheels goes through a file.
- **G4. Selecting what to push.** `Options::only` takes URIs; matching a uuid
  or checking that a URI exists in the source is done in the command line
  (`retp-cli`, `push_epc`). A library function would give `push_epc(only=[...])`
  without logic in the bridge.
- **G5. Partial listings.** `Listed` has `ended_by`; the bridge refuses a list
  that was cut short, which loses the partial list. A typed result that carries
  both would let a caller choose.
- **G6. JSON for protocol data.** `Dataspace.custom_data`,
  `Resource.custom_data`, `active_status`, `DataArrayMetadata.custom_data` are
  not exposed. Every generated ETP type has the Avro JSON encoding
  (`retp_avro::Avro::to_json`, ETP section 3.8.1, which the specification keeps
  for testing and debugging), and the channel bindings use it; whether it is
  also the form custom data takes for a Python caller is still to decide, in
  Rust, for both bridges. The active-status filter of `ResourceQuery` is
  likewise not exposed.
- **G7. Objects sent as JSON.** `read_object` reads XML only; a server that
  answers `format = "json"` is refused by name. A JSON reader beside it (the
  EnergyML JSON is `energyml_core::json`) would lift that.
- **G8. Names for server error codes.** An `ErrorInfo` carries a numeric code;
  the ETP error table lives in `retp-proto` (`EtpError`). A function from code to
  name and description would make `.code` readable without a table in Python.
- **G9. Strings and booleans.** `Decoded::Strings` does not become numpy in
  this draft (refused by name). Whether string arrays cross as a list or as a
  numpy `U` array is a convention to fix in `energyml-rust` as well, so both
  bridges agree.
- **G10. A Rust-side check for what "complete" means for a push.** The
  CLI says it in its exit code (`stopped.is_none() && left_out.is_empty()`);
  `PushReport::complete` repeats that line. It should be a method of
  `push::Report`.
- **G11. Subscribing to channels.** `retp_client::channels::subscribe_channels`
  and `start_streaming` exist, and a live session can route protocol 21 or 1
  to a destination (`LiveSession::route`), but the two are separate calls. For
  notifications the library joins them (`LiveSession::subscribe_to`: the route
  first, so that nothing arriving between the answer and the route is lost; a
  refusal removes it); for channels nothing does, and doing it in the bridge
  would be that ordering written a second time. A
  `LiveSession::subscribe_channels_to` (and its `unsubscribe`) lifts this;
  `channel_metadata` and `get_ranges` are bound meanwhile.
- **G12. A packed dataspace to disk.** `pack` returns the package's bytes and
  its arrays in memory; writing the arrays into the HDF5 files the package names
  is done by the command line itself (`retp-cli`'s `download_epc` and
  `write_array`, over `Hdf5Backend`), not by a library function. A
  `pack::save(packed, path)` in `energyml-etp-store` would give
  `download_epc(path=...)` without a second writer in the bridge; until then the
  bridge returns what `pack` returns.

Not gaps, but worth recording: the REST transport is bound (`rest=True`,
`Remote::Rest`), and what REST has no counterpart for is refused by the Rust
REST client by name. Subscriptions are bound (`Session.subscribe`): the session
is a live one (`retp_client::live`), whose task receives what arrives and hands
it to a destination - nobody polls.

## 10. Open questions

- The PyPI name (section 5).
- HDF5 in the wheel (section 6).
- Whether `retp` and `energyml.rust` should be one wheel one day. They are two
  now so that the ETP stack does not weigh on the people who only read EPC files,
  and the other way round; the dependency allow-list test keeps it that way.
- The shape of a channel subscription binding, once G11 is in Rust.
