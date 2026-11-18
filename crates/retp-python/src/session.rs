//! The plain-Rust layer of the bridge: **conversion, and nothing else**.
//!
//! Everything that can be wrong at the boundary lives here, in code
//! `cargo test` runs without an interpreter: the shape of an argument, the name
//! a failure is given, the form a result takes. `bindings.rs` only turns these
//! types into Python objects.
//!
//! # Nothing here decides anything about ETP
//!
//! Every operation is one call into `retp-client` (or `energyml-etp-store`, for
//! what crosses between an EnergyML object and a data object). Where this file
//! would need to *decide* something that the Rust API does not already
//! decide, it refuses by name and the gap is listed in `DESIGN.md` section 9
//! instead. Two places come close to the line and say so where they are:
//! [`Session::put_objects`] (staging objects in an in-memory package so that
//! `push` can plan them) and [`Session::open`] (the offer is the CLI's).
//!
//! # The blocking API, on purpose
//!
//! `retp_client` writes each operation once as an `async fn` over `Caller` and
//! runs it on the blocking `Client` through `retp_client::blocking`. Python
//! calls are synchronous, so the bridge uses exactly that: no runtime, no
//! thread of its own, and the GIL is released around each call by the binding.
//!
//! # Operations on any blocking client
//!
//! The operations added after the first scope (deleting, dataspaces,
//! transactions, supported types, channels, packing a dataspace) are free
//! functions over any [`Blocking`] client, and the [`Session`] method of the
//! same name calls them on its remote. The suite runs the same functions on a
//! client whose other end is a server connection in memory
//! (`retp_client::memory`), so what the bridge makes of an answer is tested
//! without a socket.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use energyml_core::identifiers::{EtpUri, QualifiedType, normalize_uuid};
use energyml_core::registry::{ReadMode, Registry};
use energyml_etp_store::pack::{Packed, Wanted, pack};
use energyml_etp_store::push::{
    Budget, Diagnosis, Island, NoPlan, Options, Plan, Refusal, SkipReason, Skipped, Split, SplitIsland, Stop,
    WellKnown, WriteFormat, chosen, dry_run, push, push_resumable, reconnecting,
};
use energyml_etp_store::{DEFAULT_MESSAGE, read_object};
use energyml_storage::EpcStore;
use energyml_storage::storage::EnergymlStorage;
use retp_avro::Avro;
use retp_client::arrays::{
    Array, ArrayFailure, Block, Decoded, Grouping, array_metadata, read_arrays, read_regions,
};
use retp_client::channels::{channel_metadata, get_ranges};
use retp_client::login::{Choice, LoginError};
use retp_client::native::{self, CertificateCheck, Connect, Endpoint, Remote, Resuming};
use retp_client::resume::{Came, Retry};
use retp_client::{
    Blocking, ClientError, Ended, Opened, ResourceQuery, Transaction, blocking, commit_transaction,
    custom_data_json, custom_data_strings, custom_data_value, dataspace_uri, delete_data_objects,
    delete_dataspaces, get_data_objects, list_dataspaces, list_resources, list_supported_types,
    plain_text_warning, put_dataspaces, reading_client, rollback_transaction, start_transaction,
    writing_client,
};
use retp_proto::connection::Resumable;
use retp_proto::credentials::Credentials;
use retp_proto::probe::Profile;
use retp_types::datatypes::ErrorInfo;
use retp_types::datatypes::channel_data::ChannelRangeInfo;
use retp_types::datatypes::data_array_types::DataArrayIdentifier;
use retp_types::datatypes::object::{ContextScopeKind, Dataspace};

/// The name this client announces in `RequestSession`.
pub const APPLICATION_NAME: &str = "retp-python";

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

/// What kind of failure it was - the Python exception it becomes.
///
/// One variant per thing a caller can *do* differently: reconnect
/// ([`Kind::ConnectionLost`]), log in again ([`Kind::AuthorizationNeeded`]),
/// check what happened before retrying ([`Kind::OutcomeUnknown`]), fix the
/// request ([`Kind::NotSent`], [`Kind::BadArgument`]), or give up on a feature
/// ([`Kind::Unsupported`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The argument is wrong; nothing was sent. A Python `ValueError`.
    BadArgument,
    /// The bridge, or the Rust library behind it, does not support the case.
    /// Named, never an empty result.
    Unsupported,
    /// The session could not be opened: the offer, or the server's refusal.
    SessionRefused,
    /// The server wants credentials, and said which.
    AuthorizationNeeded,
    /// The connection is gone: a new session has to be opened.
    ConnectionLost,
    /// The connection fell while a request was out: its outcome is **unknown**
    /// (neither a success nor a failure - a retry of a write may write twice).
    OutcomeUnknown,
    /// The request was not sent: the library would see the server refuse it.
    /// The session is untouched.
    NotSent,
    /// The session is serving another request, from another thread: a session
    /// takes one request at a time. Nothing was sent; the session is
    /// untouched. Open one session per thread, or wait.
    Busy,
    /// The server answered with an error (its code in [`Failure::code`]).
    Server,
    /// No answer in time.
    TimedOut,
    /// The answer arrived and could not be taken.
    AnswerRefused,
    /// Something went wrong locally: an object that does not read or write, a
    /// package that does not open.
    Local,
    /// A failure this version of the bridge has no name for (a case a later
    /// version of the library added). The base `RetpError`, never a success.
    Other,
}

/// A failure, as the bridge reports it: a kind, the library's own words, and
/// the server's error code when the server gave one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// What kind.
    pub kind: Kind,
    /// The text, which names what failed. Never contains a credential.
    pub message: String,
    /// The ETP error code, when the server sent one.
    pub code: Option<i32>,
}

impl Failure {
    fn new(kind: Kind, message: impl Into<String>) -> Self {
        Failure {
            kind,
            message: message.into(),
            code: None,
        }
    }

    /// An argument the caller got wrong.
    pub fn bad_argument(message: impl Into<String>) -> Self {
        Failure::new(Kind::BadArgument, message)
    }

    /// A case that is not supported, and says so.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Failure::new(Kind::Unsupported, message)
    }

    fn local(message: impl std::fmt::Display) -> Self {
        Failure::new(Kind::Local, message.to_string())
    }

    fn server(code: i32, message: impl Into<String>) -> Self {
        Failure {
            kind: Kind::Server,
            message: message.into(),
            code: Some(code),
        }
    }

    /// The error a server sent for one item of a plural request.
    fn of_error(error: ErrorInfo) -> Self {
        Failure::server(error.code, error.message)
    }

    /// A store's "no" that carries a reason and no error code (a refused
    /// transaction, a refused commit): a server answer, so `Kind::Server`,
    /// without a code.
    fn said_no(message: impl Into<String>) -> Self {
        Failure::new(Kind::Server, message)
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for Failure {}

/// The library's own text is kept as it is: it is written to be read, and a
/// second wording here would be a second thing to keep in step.
impl From<ClientError> for Failure {
    fn from(error: ClientError) -> Self {
        let message = error.to_string();
        let kind = match &error {
            // No session opened: the server refused the upgrade (the status is in the message).
            ClientError::Offer(_) | ClientError::Refused(_) | ClientError::HandshakeRefused { .. } => {
                Kind::SessionRefused
            }
            ClientError::AuthorizationNeeded { .. } => Kind::AuthorizationNeeded,
            ClientError::Transport(_) | ClientError::Closed { .. } => Kind::ConnectionLost,
            ClientError::Interrupted { .. } => Kind::OutcomeUnknown,
            ClientError::Send(_) | ClientError::Invalid(_) => Kind::NotSent,
            ClientError::TimedOut { .. } => Kind::TimedOut,
            ClientError::AnswerRefused { .. } => Kind::AnswerRefused,
            // `ClientError` is non-exhaustive: a case added by a later version of the
            // library. It is a failure of no named kind, and it keeps the library's text.
            _ => Kind::Other,
        };
        Failure::new(kind, message)
    }
}

impl From<ArrayFailure> for Failure {
    fn from(failure: ArrayFailure) -> Self {
        match failure {
            ArrayFailure::Store(error) => Failure::server(error.code, error.message),
            other => Failure::new(Kind::AnswerRefused, other.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Connecting
// ---------------------------------------------------------------------------

/// How to connect - the arguments of `retp.connect`.
///
/// **The token is consumed by [`ConnectOptions::endpoint`]**, which turns it
/// into `retp_proto::credentials::Credentials` (whose `Debug` hides the
/// secret) and keeps nothing else. There is deliberately no `Debug` here.
/// [`Session`] keeps those credentials, and only those, to open the session
/// again for a push that reconnects; it keeps no password nor client secret
/// of [`ConnectOptions::login`], and nothing of it crosses to Python.
#[derive(Default)]
pub struct ConnectOptions {
    /// `ws://` or `wss://` URL of the ETP endpoint; with [`ConnectOptions::rest`],
    /// the `http(s)://` URL of the Reservoir DDMS REST API.
    pub url: String,
    /// An OAuth 2.0 bearer token, without its scheme.
    pub token: Option<String>,
    /// Another way in than a token in hand - HTTP Basic, an OAuth 2.0 token
    /// endpoint (password or client credentials, Azure AD's included), AWS
    /// Cognito: `retp_client::login::Choice`, the command line's own, which
    /// decides which login the fields given ask for. Its `token` is replaced by
    /// [`ConnectOptions::token`].
    pub login: Choice,
    /// Talk to the Reservoir DDMS REST API instead of an ETP WebSocket: the
    /// command line's `--rest`, and `native::rest_client`.
    pub rest: bool,
    /// An OSDU data partition: talks to the server as OSDU's Reservoir DDMS.
    pub partition: Option<String>,
    /// Do not check the server's certificate. Said in [`Session::warnings`].
    pub insecure: bool,
    /// Announce that this session will write (adds the Transaction protocol).
    pub writing: bool,
    /// Qualified types to ask for (`resqml22.*`); every release when empty.
    pub types: Vec<String>,
    /// Ping when the session has been quiet this many seconds; 0 never.
    pub keepalive_seconds: Option<u64>,
    /// How many requests of one operation are in flight at once.
    pub at_once: Option<usize>,
}

impl ConnectOptions {
    /// The URL, validated, the login turned into credentials, and the
    /// warnings the library has about the connection.
    ///
    /// Opens no session, so every refusal here is tested without a server. A
    /// token in hand or HTTP Basic needs no network either; a login at a token
    /// endpoint or at Cognito makes its one request here
    /// (`retp_client::login::Login::obtain`), after every check that needs
    /// none.
    pub fn endpoint(self) -> Result<(Endpoint, Vec<String>), Failure> {
        self.check_transport()?;
        if self.at_once == Some(0) {
            return Err(Failure::bad_argument(
                "at_once must be at least 1 (1 sends the requests one after the other)",
            ));
        }
        let credentials = self.credentials()?;
        let certificates = if self.insecure {
            CertificateCheck::Skip
        } else {
            CertificateCheck::Verify
        };
        let warnings = [plain_text_warning(&self.url), certificates.warning(&self.url)]
            .into_iter()
            .flatten()
            .collect();
        // The data partition of OSDU's Reservoir DDMS is a header of the server, like any other.
        let headers = match self.partition {
            Some(partition) => retp_client::ExtraHeaders::new()
                .with("data-partition-id", partition)
                .map_err(|why| Failure::bad_argument(format!("the partition is refused: {why}")))?,
            None => retp_client::ExtraHeaders::new(),
        };
        Ok((
            Endpoint {
                url: self.url,
                // The other headers are not bridged yet: the partition is the only one.
                headers,
                credentials,
                certificates,
                handshake: Profile::Native,
            },
            warnings,
        ))
    }

    /// The URL matches the transport asked for, and nothing is given that the
    /// transport has no use for - the command line's checks (`retp-cli`'s
    /// `Server::open_rest`), refused by name rather than ignored.
    fn check_transport(&self) -> Result<(), Failure> {
        let url = &self.url;
        let websocket = url.starts_with("ws://") || url.starts_with("wss://");
        let http = url.starts_with("http://") || url.starts_with("https://");
        match (self.rest, websocket, http) {
            (false, true, _) => Ok(()),
            (false, _, true) => Err(Failure::bad_argument(format!(
                "{url} is an HTTP URL: pass rest=True to talk to the Reservoir DDMS REST API, \
                 or give the ETP WebSocket's ws:// or wss:// URL"
            ))),
            (false, _, _) => Err(Failure::bad_argument(format!(
                "{url}: an ETP WebSocket URL is ws:// or wss:// (with rest=True, the REST API's http:// or https://)"
            ))),
            (true, _, true) => {
                for (name, given) in [
                    ("keepalive", self.keepalive_seconds.is_some()),
                    ("types", !self.types.is_empty()),
                ] {
                    if given {
                        return Err(Failure::bad_argument(format!(
                            "{name} means nothing over REST: there is no session to negotiate or keep alive"
                        )));
                    }
                }
                Ok(())
            }
            (true, _, _) => Err(Failure::bad_argument(format!(
                "{url}: rest=True takes the REST API's URL (https://.../api/reservoir-ddms/v2), not a WebSocket's"
            ))),
        }
    }

    /// The credentials the login gives: `Choice::login` decides which login
    /// the fields ask for, in the command line's order (a token first), and
    /// `Login::obtain` makes them.
    ///
    /// **No environment variable is read**: `Choice::login` takes a lookup,
    /// and the bridge gives it none, so that what a session logs in with is
    /// what the Python call says (a caller passes what `os.environ` holds).
    fn credentials(&self) -> Result<Option<Credentials>, Failure> {
        let mut choice = self.login.clone();
        choice.token = self.token.clone();
        let Some(login) = choice.login(|_| None).map_err(login_failure)? else {
            return Ok(None);
        };
        Ok(Some(login.obtain().map_err(login_failure)?.credentials))
    }
}

/// Why no credentials came out, as the failure a caller acts on. The
/// library's words are kept; none of them carries a secret.
fn login_failure(error: LoginError) -> Failure {
    let kind = match &error {
        // What was given cannot make credentials: fix the call.
        LoginError::Invalid(_) => Kind::BadArgument,
        // The authorization server could not be reached: no session.
        LoginError::Http(_) => Kind::SessionRefused,
        // It answered no: log in again, with other credentials.
        LoginError::Refused { .. } => Kind::AuthorizationNeeded,
        // A step this client does not take (a new password, a second factor).
        LoginError::Challenge { .. } => Kind::Unsupported,
    };
    Failure::new(kind, format!("the login failed: {error}"))
}

/// Which way from a URI in Discovery's graph: the five names of the ETP
/// `ContextScopeKind`, as the specification spells them.
pub fn parse_scope(name: &str) -> Result<ContextScopeKind, Failure> {
    Ok(match name {
        "self" => ContextScopeKind::Self_,
        "sources" => ContextScopeKind::Sources,
        "targets" => ContextScopeKind::Targets,
        "sourcesOrSelf" => ContextScopeKind::SourcesOrSelf,
        "targetsOrSelf" => ContextScopeKind::TargetsOrSelf,
        other => {
            return Err(Failure::bad_argument(format!(
                "scope `{other}` is not one of self, sources, targets, sourcesOrSelf, targetsOrSelf"
            )));
        }
    })
}

// ---------------------------------------------------------------------------
// What crosses
// ---------------------------------------------------------------------------

/// A dataspace, as listed (`Dataspace` protocol, `GetDataspaces`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataspaceRow {
    /// The canonical URI.
    pub uri: String,
    /// The path (`demo`, `carl/teapot`).
    pub path: String,
    /// `storeLastWrite`, microseconds since the epoch as the server sent it.
    pub store_last_write: i64,
    /// `storeCreated`.
    pub store_created: i64,
    /// Its `customData`, each value as JSON text - a flag, a number, a
    /// string, a list of strings, `null` (`retp_client::custom_data_json`):
    /// what an OSDU store keeps there, `owners`, `viewers`, `legaltags`.
    pub custom_data: Vec<(String, String)>,
    /// The keys of `customData` whose value is of a kind plain JSON does not
    /// carry, named rather than dropped.
    pub notes: Vec<String>,
}

/// A resource, as listed (`Discovery` protocol, `GetResources`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRow {
    /// The canonical URI.
    pub uri: String,
    /// The name the store gives it.
    pub name: String,
    /// `sourceCount`, when the store counted.
    pub source_count: Option<i32>,
    /// `targetCount`, when the store counted.
    pub target_count: Option<i32>,
    /// `lastChanged`.
    pub last_changed: i64,
    /// `storeLastWrite`.
    pub store_last_write: i64,
    /// `storeCreated`.
    pub store_created: i64,
}

/// An edge between two resources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeRow {
    /// The source.
    pub source_uri: String,
    /// The target.
    pub target_uri: String,
    /// The relationship kind, by its ETP name.
    pub relationship_kind: String,
}

/// What `GetResources` answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resources {
    /// The resources.
    pub resources: Vec<ResourceRow>,
    /// The edges, when they were asked for.
    pub edges: Vec<EdgeRow>,
}

/// One object read: the JSON text of the object (the EnergyML JSON encoding,
/// every non-primitive node carrying `$type`), and what the reader said of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    /// The JSON text.
    pub json: String,
    /// What the reading had to assume (a type read under another name, a field
    /// defaulted or repaired). A reader that keeps quiet about it cannot be
    /// checked.
    pub notes: Vec<String>,
}

/// An object or a failure, per URI, in the order asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Objects {
    /// The objects that came.
    pub found: Vec<(String, Fetched)>,
    /// The ones that did not, with why.
    pub failed: Vec<(String, Failure)>,
}

/// An array's metadata (`GetDataArrayMetadata`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayInfo {
    /// The array's path in its resource.
    pub path: String,
    /// Its dimensions, outermost first.
    pub dimensions: Vec<i64>,
    /// The logical type, by its ETP name (`arrayOfDouble64LE`).
    pub logical: String,
    /// How it travels, by its ETP name.
    pub transport: String,
    /// The block shape the store prefers to be read in, when it says.
    pub preferred_subarray_dimensions: Vec<i64>,
}

/// Metadata or failure per path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayInfos {
    /// The arrays whose metadata came.
    pub found: Vec<ArrayInfo>,
    /// The ones that did not, with why.
    pub failed: Vec<(String, Failure)>,
}

/// An array read: its dimensions, and its values **flat**, in row-major order,
/// in the type the logical type says (not the one it travelled in).
///
/// Flat with the shape beside it, as `energyml-rust`'s bridge does: reshaping
/// is one line in numpy, and the dimensions are the server's statement, not
/// ours.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayRead {
    /// Dimensions, outermost first.
    pub dimensions: Vec<u64>,
    /// The values.
    pub values: Decoded,
}

/// One island of a push, as planned and as it went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IslandRow {
    /// `committed`, `refused` or `not sent` in a report; `planned` in a
    /// plan.
    pub state: &'static str,
    /// The objects in it, by their URI in the source.
    pub uris: Vec<String>,
    /// How many objects (`uris.len()`).
    pub objects: usize,
    /// How many arrays go with them.
    pub arrays: usize,
    /// What they weigh.
    pub bytes: u64,
    /// The sizes of its units, in order: one object, or one cycle
    /// (`energyml_etp_store::push::Island::units`).
    pub units: Vec<usize>,
    /// Its cycles, each by its URIs in the source
    /// (`energyml_etp_store::push::Island::cycles`).
    pub cycles: Vec<Vec<String>>,
    /// The objects by their URI in the destination, in the order of
    /// `uris` - in a plan only.
    pub destinations: Option<Vec<String>>,
}

impl IslandRow {
    /// The library's island, with its state.
    fn of(island: &Island, state: &'static str) -> Self {
        IslandRow {
            state,
            uris: island.uris.clone(),
            objects: island.uris.len(),
            arrays: island.arrays,
            bytes: island.bytes,
            units: island.units.clone(),
            cycles: island.cycles().into_iter().map(<[String]>::to_vec).collect(),
            destinations: None,
        }
    }
}

/// The reasons of the skipped objects as words: `present`, `journal`.
fn skipped_rows(skipped: Vec<Skipped>) -> Vec<(String, &'static str)> {
    skipped
        .into_iter()
        .map(|one| {
            let why = match one.why {
                SkipReason::Present => "present",
                SkipReason::Journal => "journal",
            };
            (one.uri, why)
        })
        .collect()
}

/// What a push would send - `energyml_etp_store::push::Plan`, flattened, its
/// islands in the rows of a report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushPlan {
    /// The destination, as a dataspace URI.
    pub dataspace: String,
    /// The islands in the order a push sends them, each `planned`, with
    /// its destination URIs.
    pub islands: Vec<IslandRow>,
    /// Objects that could not be sent at all, with why.
    pub left_out: Vec<String>,
    /// References that leave the source, `(from, to)`.
    pub dangling: Vec<(String, String)>,
    /// What the journal records, left out: `(uri, "journal")`.
    pub skipped: Vec<(String, &'static str)>,
}

impl PushPlan {
    /// The library's plan, flattened.
    pub fn of(plan: Plan) -> Self {
        let islands = plan
            .islands
            .iter()
            .map(|island| IslandRow {
                destinations: Some(
                    plan.destinations_of(island)
                        .into_iter()
                        .map(str::to_string)
                        .collect(),
                ),
                ..IslandRow::of(island, "planned")
            })
            .collect();
        PushPlan {
            dataspace: plan.dataspace,
            islands,
            left_out: plan.left_out,
            dangling: plan.dangling,
            skipped: skipped_rows(plan.skipped),
        }
    }
}

impl From<NoPlan> for Failure {
    fn from(refused: NoPlan) -> Self {
        let message = refused.to_string();
        match refused {
            // What the caller asked for, or the journal it named.
            NoPlan::NeedsDestination(_) | NoPlan::Journal(_) => Failure::bad_argument(message),
            NoPlan::Source(_) => Failure::local(message),
        }
    }
}

/// What a push of the EPC at `path` into `dataspace` would send, with no
/// session: `energyml_etp_store::push::dry_run`, with the options `push_epc`
/// builds from the same words (`PushChoices::options`). The words that act
/// on the session alone (`one_by_one`, `split`, `reconnect`) change nothing
/// in it; `skip_present` is refused by the library, by name.
pub fn plan_epc(path: &str, dataspace: &str, choices: &PushChoices) -> Result<PushPlan, Failure> {
    choices.check()?;
    let registry = Arc::new(energyml_schemas::all());
    let store = EpcStore::open(path, registry.clone())
        .map_err(|why| Failure::local(format!("cannot open {path}: {why}")))?;
    plan_store(&store, &registry, dataspace, choices)
}

/// [`plan_epc`] of a package already open - and what tests call with a
/// package staged in memory.
pub fn plan_store(
    store: &EpcStore,
    registry: &Registry,
    dataspace: &str,
    choices: &PushChoices,
) -> Result<PushPlan, Failure> {
    choices.check()?;
    let source: &(dyn EnergymlStorage + Sync) = store;
    let only = only_of(source, choices)?;
    let options = choices.options(registry, only.as_deref());
    let plan = blocking(dry_run(source, dataspace, &options))?;
    Ok(PushPlan::of(plan))
}

/// `only=`, matched against the source's own URIs (`push::chosen`).
fn only_of(
    source: &(dyn EnergymlStorage + Sync),
    choices: &PushChoices,
) -> Result<Option<Vec<String>>, Failure> {
    match &choices.only {
        Some(names) => Ok(Some(
            blocking(chosen(source, names)).map_err(Failure::bad_argument)?,
        )),
        None => Ok(None),
    }
}

/// What a push came to - `energyml_etp_store::push::Report`, flattened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushReport {
    /// The plan, in the order it was followed, with the state of each island.
    pub islands: Vec<IslandRow>,
    /// How many islands were committed.
    pub sent: usize,
    /// Objects that could not be sent at all, with why.
    pub left_out: Vec<String>,
    /// References that leave the source, `(from, to)`.
    pub dangling: Vec<(String, String)>,
    /// The session agreed no Transaction: a refused island may be partly there.
    pub without_transactions: bool,
    /// Why the push stopped, `None` when every island went through.
    pub stopped: Option<String>,
    /// The same stop, sorted (`energyml_etp_store::push::Report::diagnosis`).
    pub stop: Option<Diagnosis>,
    /// Every object or array of the island that stopped that the store
    /// refused, with its code (`energyml_etp_store::push::Report::refusals`).
    pub refusals: Vec<Refusal>,
    /// The objects not sent because they are there already, by their URI in
    /// the destination, with how that is known: `present` (the dataspace
    /// holds them) or `journal` (the journal records them).
    pub skipped: Vec<(String, &'static str)>,
    /// Each island split and sent as pieces in its place
    /// (`energyml_etp_store::push::Report::splits`).
    pub splits: Vec<SplitIsland>,
    /// How many new sessions were opened after the connection fell.
    pub reconnections: usize,
    /// How each of them came back, in order
    /// (`energyml_etp_store::push::Report::reopened`): resumed, or a new
    /// session with why the resume did not serve, when one was asked.
    pub reopened: Vec<Came>,
}

impl PushReport {
    /// Every island committed and nothing left out - the CLI's own test for
    /// "the push is done in full".
    pub fn complete(&self) -> bool {
        self.stopped.is_none() && self.left_out.is_empty()
    }

    /// The library's report, flattened: each island with its state, the
    /// stop in a sentence, the reasons of the skipped objects as words.
    pub fn of(report: energyml_etp_store::push::Report) -> Self {
        let islands = report
            .islands
            .iter()
            .enumerate()
            .map(|(rank, island)| {
                let state = if rank < report.sent {
                    "committed"
                } else if report
                    .stopped
                    .as_ref()
                    .is_some_and(|stopped| stopped.island == rank)
                {
                    "refused"
                } else {
                    "not sent"
                };
                IslandRow::of(island, state)
            })
            .collect();
        let stop = report.diagnosis();
        let stopped = report.stopped.as_ref().map(|stopped| match &stopped.why {
            Stop::Refused(why) => format!("island {} was refused: {why}", stopped.island + 1),
            Stop::Failed(why) => format!(
                "island {} stopped: {why} (the island's outcome is unknown)",
                stopped.island + 1
            ),
            Stop::GaveUp { attempts, last } => format!(
                "island {} stopped: the connection fell {attempts} time(s) in a row, and the push gave up: {last} (the island's outcome is unknown)",
                stopped.island + 1
            ),
            Stop::Journal(why) => format!("stopped before island {}: the journal: {why}", stopped.island + 1),
        });
        PushReport {
            islands,
            sent: report.sent,
            left_out: report.left_out,
            dangling: report.dangling,
            without_transactions: report.without_transactions,
            stopped,
            stop,
            refusals: report.refusals,
            skipped: skipped_rows(report.skipped),
            splits: report.splits,
            reconnections: report.reconnections,
            reopened: report.reopened,
        }
    }
}

// ---------------------------------------------------------------------------
// The session
// ---------------------------------------------------------------------------

/// An open session with an ETP server.
pub struct Session {
    remote: Option<Remote>,
    registry: Arc<Registry>,
    url: String,
    writing: bool,
    warnings: Vec<String>,
    /// What it was opened from, for a push that opens it again
    /// (`PushChoices::reconnect`).
    recipe: Recipe,
}

/// What a session is opened from: the endpoint - with the credentials it
/// was opened with, or the last ones [`Session::reauthorize`] had accepted -
/// and the choices of `connect`. Kept so that a push that reconnects opens
/// the same session again, the whole way; nothing of it crosses to Python.
struct Recipe {
    endpoint: Endpoint,
    types: Vec<String>,
    writing: bool,
    keepalive: Option<u64>,
    at_once: Option<usize>,
    rest: bool,
}

/// Open a session from its recipe: the CLI's own steps (`retp-cli`'s
/// `Server::open_for`) - an offer (`reading_client` / `writing_client`),
/// fitted to what the server announces when no type was named, then a live
/// session. With `rest`, the REST client instead (`native::rest_client`, as
/// the command line's `open_rest`): nothing is opened, each request goes on
/// its own.
///
/// With `lost`, the live session asks the server for that session first -
/// OSDU's `ResumeSession`, `LiveSession::connect_resuming` - over a
/// connection opened the same way, with the recipe's own credentials.
fn open_remote(recipe: &Recipe, lost: Option<Resumable>) -> Result<Remote, Failure> {
    let endpoint = recipe.endpoint.clone();
    if recipe.rest {
        if lost.is_some() {
            return Err(Failure::bad_argument(
                "resume_session means nothing over REST: there is no session to ask back",
            ));
        }
        let mut client = native::rest_client(&endpoint);
        if let Some(at_once) = recipe.at_once {
            client = client.with_at_once(at_once);
        }
        return Ok(Remote::Rest(Box::new(client)));
    }
    let types = &recipe.types;
    let (name, version, id) = (APPLICATION_NAME, env!("CARGO_PKG_VERSION"), native::random_uuid());
    let (offer, handlers) = if recipe.writing {
        writing_client(name, version, id, types)
    } else {
        reading_client(name, version, id, types)
    };
    // No type named: whatever the server announces is asked for too.
    let offer = if types.is_empty() {
        native::fit_to_server(&endpoint, offer).0
    } else {
        offer
    };
    // A live session: driven by a task of its own, so that what the server
    // sends unasked (`Session.subscribe`) arrives while Python does
    // something else, and nothing has to poll.
    let mut settings = retp_client::live::LiveSettings::default();
    if let Some(at_once) = recipe.at_once {
        settings.at_once = at_once;
    }
    let connect = Connect {
        endpoint,
        offer,
        handlers,
        max_message: None,
    };
    let live = match lost {
        None => retp_client::live::LiveSession::connect(connect, settings)?,
        Some(lost) => retp_client::live::LiveSession::connect_resuming(connect, lost, settings)?,
    };
    if let Some(seconds) = recipe.keepalive {
        live.client()
            .set_keepalive((seconds > 0).then(|| Duration::from_secs(seconds)));
    }
    Ok(Remote::Live(Box::new(live)))
}

impl Session {
    /// Open a session ([`open_remote`]), and keep what it was opened from.
    pub fn open(options: ConnectOptions) -> Result<Self, Failure> {
        let (types, keepalive, at_once, writing, rest) = (
            options.types.clone(),
            options.keepalive_seconds,
            options.at_once,
            options.writing,
            options.rest,
        );
        let url = options.url.clone();
        let (endpoint, warnings) = options.endpoint()?;
        let recipe = Recipe {
            endpoint,
            types,
            writing,
            keepalive,
            at_once,
            rest,
        };
        let remote = open_remote(&recipe, None)?;
        Ok(Session {
            remote: Some(remote),
            registry: Arc::new(energyml_schemas::all()),
            url,
            writing,
            warnings,
            recipe,
        })
    }

    /// The URL this session was opened on (never carries the token: it is not
    /// part of the URL).
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Whether the session announced that it writes.
    pub fn writing(&self) -> bool {
        self.writing
    }

    /// What the library had to say about the connection (plain text, an
    /// unchecked certificate).
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// The live session, when this is one (an ETP WebSocket); `None` over REST.
    pub fn live(&self) -> Option<&retp_client::live::LiveSession> {
        match &self.remote {
            Some(Remote::Live(live)) => Some(live),
            _ => None,
        }
    }

    fn remote(&mut self) -> Result<&mut Remote, Failure> {
        self.remote
            .as_mut()
            .ok_or_else(|| Failure::new(Kind::ConnectionLost, "the session is closed"))
    }

    /// A `Ping` and its `Pong`, in seconds. `Remote::ping`.
    pub fn ping(&mut self) -> Result<f64, Failure> {
        Ok(self.remote()?.ping()?.as_secs_f64())
    }

    /// End the session. `Remote::close`. Closing twice is a no-op.
    pub fn close(&mut self) -> Result<(), Failure> {
        match self.remote.take() {
            Some(remote) => Ok(remote.close("closed by the Python bridge")?),
            None => Ok(()),
        }
    }

    /// The dataspaces of the store. `retp_client::list_dataspaces`.
    ///
    /// An answer a server cut short with an error is **refused**, not returned
    /// as if it were the whole list.
    pub fn dataspaces(&mut self) -> Result<Vec<DataspaceRow>, Failure> {
        let list = list_dataspaces(self.remote()?)?;
        if let Some(error) = list.ended_by {
            return Err(Failure::server(
                error.code,
                format!("the dataspace list was cut short: {}", error.message),
            ));
        }
        Ok(list.items.into_iter().map(dataspace_row).collect())
    }

    /// The resources under a dataspace (a path or a URI) or an object.
    /// `retp_client::list_resources` with a `ResourceQuery`; the path is made
    /// a URI by `retp_client::dataspace_uri`.
    pub fn resources(
        &mut self,
        uri_or_path: &str,
        depth: i32,
        scope: &str,
        types: Vec<String>,
        edges: bool,
        count: bool,
    ) -> Result<Resources, Failure> {
        if depth < 1 {
            return Err(Failure::bad_argument("depth must be at least 1"));
        }
        let query = ResourceQuery {
            uri: dataspace_uri(uri_or_path),
            depth,
            scope: parse_scope(scope)?,
            data_object_types: types,
            include_edges: edges,
            count_objects: count,
            active_status: None,
        };
        let list = list_resources(self.remote()?, &query)?;
        if let Some(error) = list.ended_by {
            return Err(Failure::server(
                error.code,
                format!("the resource list was cut short: {}", error.message),
            ));
        }
        Ok(Resources {
            resources: list
                .resources
                .into_iter()
                .map(|one| ResourceRow {
                    uri: one.uri,
                    name: one.name,
                    source_count: one.source_count,
                    target_count: one.target_count,
                    last_changed: one.last_changed,
                    store_last_write: one.store_last_write,
                    store_created: one.store_created,
                })
                .collect(),
            edges: list
                .edges
                .into_iter()
                .map(|edge| EdgeRow {
                    source_uri: edge.source_uri,
                    target_uri: edge.target_uri,
                    relationship_kind: format!("{:?}", edge.relationship_kind),
                })
                .collect(),
        })
    }

    /// Objects by canonical URI. `retp_client::get_data_objects` in `xml`, then
    /// `energyml_etp_store::read_object` (the registry), then
    /// `energyml_core::json::write_string`.
    ///
    /// A server that answers in another format is **refused by name** for that
    /// object, not guessed at.
    pub fn get_objects(&mut self, uris: &[String]) -> Result<Objects, Failure> {
        if uris.is_empty() {
            return Err(Failure::bad_argument("no URI given"));
        }
        let registry = self.registry.clone();
        let mut answers = get_data_objects(self.remote()?, uris, "xml")?;
        let mut out = Objects {
            found: Vec::new(),
            failed: Vec::new(),
        };
        // In the order asked.
        for uri in uris {
            match answers.remove(uri) {
                None => out.failed.push((
                    uri.clone(),
                    Failure::new(
                        Kind::AnswerRefused,
                        "the server's answer does not mention this URI",
                    ),
                )),
                Some(Err(error)) => out
                    .failed
                    .push((uri.clone(), Failure::server(error.code, error.message))),
                Some(Ok(object)) if object.format != "xml" => out.failed.push((
                    uri.clone(),
                    Failure::unsupported(format!(
                        "the server sent this object as `{}`; only `xml` is read by this bridge for now",
                        object.format
                    )),
                )),
                Some(Ok(object)) => match read_object(&registry, ReadMode::Tolerant, uri, &object.data) {
                    Ok((read, notes)) => match energyml_core::json::write_string(read.as_reflect(), false) {
                        Ok(json) => out.found.push((uri.clone(), Fetched { json, notes })),
                        Err(why) => out.failed.push((uri.clone(), Failure::local(why))),
                    },
                    Err(why) => out.failed.push((uri.clone(), Failure::local(why))),
                },
            }
        }
        Ok(out)
    }

    fn require_writing(&self, what: &str) -> Result<(), Failure> {
        refuse_unless_writing(self.writing, what)
    }

    /// Put objects, given as EnergyML JSON texts (each with its `$type`), into
    /// a dataspace.
    ///
    /// **This is the nearest the bridge comes to composing**: there is no
    /// `put these objects` function in the Rust API, only `push`, which plans
    /// and sends *a source that is an `EnergymlStorage`*. The objects are
    /// therefore read with the registry (`energyml_core::json::read_string`),
    /// put in an in-memory `EpcStore`, and that store is pushed - so the
    /// ordering (targets before sources), the islands and the transactions are
    /// the library's, not ours. `DESIGN.md` section 9 lists the direct function
    /// as a gap to add in `energyml-etp-store`.
    ///
    /// Objects carrying `_data` (referenced objects expanded in place) are
    /// refused: which of the two a caller means is not ours to guess.
    pub fn put_objects(
        &mut self,
        dataspace: &str,
        jsons: &[String],
        choices: &PushChoices,
    ) -> Result<PushReport, Failure> {
        self.require_writing("put objects")?;
        choices.check()?;
        let store = stage(&self.registry, jsons)?;
        self.push_store(&store, dataspace, choices)
    }

    /// Send the EPC at `path` into a dataspace: `energyml_storage::EpcStore::open`
    /// then `energyml_etp_store::push::push`, as `retp etp push-epc` does.
    ///
    /// The arrays travel too, and reading them needs the `hdf5` feature of this
    /// build: without it the storage layer refuses an array by name.
    pub fn push_epc(
        &mut self,
        path: &str,
        dataspace: &str,
        choices: &PushChoices,
    ) -> Result<PushReport, Failure> {
        self.require_writing("push a package")?;
        choices.check()?;
        let store = EpcStore::open(path, self.registry.clone())
            .map_err(|why| Failure::local(format!("cannot open {path}: {why}")))?;
        self.push_store(&store, dataspace, choices)
    }

    /// `energyml_etp_store::push::push` with the options the command line's
    /// `push` builds from the same words (`retp-cli`'s `push_options`) - or,
    /// with `reconnect`, `push::push_resumable`, which opens this session
    /// again from what it was opened with when the connection falls, and
    /// leaves the last one here; with `resume_session` too, over
    /// `native::Resuming`, which first asks the server for the lost session.
    fn push_store(
        &mut self,
        store: &EpcStore,
        dataspace: &str,
        choices: &PushChoices,
    ) -> Result<PushReport, Failure> {
        let source: &(dyn EnergymlStorage + Sync) = store;
        let only = only_of(source, choices)?;
        // Its own handle: the options borrow it while the session is borrowed to send.
        let registry = self.registry.clone();
        let options = choices.options(&registry, only.as_deref());
        // A closed session is not opened again, reconnecting or not.
        self.remote()?;
        if choices.resume_session && self.recipe.rest {
            return Err(Failure::bad_argument(
                "resume_session means nothing over REST: there is no session to ask back",
            ));
        }
        let report = if choices.reconnect == 0 {
            blocking(push(self.remote()?, source, dataspace, &options, |_progress| {}))?
        } else {
            let retry = reconnecting(
                choices.reconnect,
                choices.reconnect_pause.unwrap_or(Retry::default().first_pause),
            );
            let recipe = &self.recipe;
            let mut connector = choices.resuming(
                || open_remote(recipe, None),
                |lost| open_remote(recipe, Some(lost)),
            );
            push_resumable(
                &mut connector,
                &mut self.remote,
                source,
                dataspace,
                &options,
                &retry,
                |_event| {},
            )?
        };
        Ok(PushReport::of(report))
    }

    /// Renew the authorization of this session with a new bearer token:
    /// `retp_client::native::Remote::reauthorize`. Over ETP, an `Authorize`
    /// on the open session (ETP v1.2 section 3.13.5) - a refusal raises
    /// `AuthorizationNeeded` and the session stays open; over REST, the token
    /// of every request from now on.
    ///
    /// Accepted, the token is also what a push that reconnects opens its new
    /// sessions with.
    pub fn reauthorize(&mut self, token: &str) -> Result<(), Failure> {
        let credentials = Credentials::bearer(token)
            .map_err(|why| Failure::bad_argument(format!("the token is refused: {why}")))?;
        self.remote()?.reauthorize(&credentials)?;
        self.recipe.endpoint.credentials = Some(credentials);
        Ok(())
    }

    /// The metadata of arrays of one resource. `retp_client::arrays::array_metadata`.
    pub fn array_metadata(&mut self, uri: &str, paths: &[String]) -> Result<ArrayInfos, Failure> {
        if paths.is_empty() {
            return Err(Failure::bad_argument("no array path given"));
        }
        let ids = identifiers(uri, paths);
        let answers = array_metadata(self.remote()?, &ids)?;
        let mut out = ArrayInfos {
            found: Vec::new(),
            failed: Vec::new(),
        };
        for (path, answer) in paths.iter().zip(answers) {
            match answer {
                Ok(meta) => out.found.push(ArrayInfo {
                    path: path.clone(),
                    dimensions: meta.dimensions,
                    logical: format!("{:?}", meta.logical_array_type),
                    transport: format!("{:?}", meta.transport_array_type),
                    preferred_subarray_dimensions: meta.preferred_subarray_dimensions,
                }),
                Err(error) => out
                    .failed
                    .push((path.clone(), Failure::server(error.code, error.message))),
            }
        }
        Ok(out)
    }

    /// One array, whole - `retp_client::arrays::read_arrays` - or the block
    /// `start`/`count` of it - `read_regions`. Both or neither of `start` and
    /// `count`.
    ///
    /// Values come in their **logical** type (`ArrayValues::decode`). A string
    /// array, or bytes of a type without a fixed size, is refused by name:
    /// this bridge hands numpy arrays of numbers.
    pub fn read_array(
        &mut self,
        uri: &str,
        path: &str,
        region: Option<(Vec<u64>, Vec<u64>)>,
    ) -> Result<ArrayRead, Failure> {
        let id = identifiers(uri, &[path.to_string()]).remove(0);
        let mut answers = match region {
            None => read_arrays(self.remote()?, &[id], DEFAULT_MESSAGE, |_| {})?,
            Some((starts, counts)) => {
                if starts.len() != counts.len() {
                    return Err(Failure::bad_argument(
                        "start and count must have the same length (one entry per dimension)",
                    ));
                }
                read_regions(
                    self.remote()?,
                    &[(id, Some(Block { starts, counts }))],
                    DEFAULT_MESSAGE,
                    |_| {},
                )?
            }
        };
        let array: Array = answers
            .pop()
            .ok_or_else(|| Failure::new(Kind::AnswerRefused, "no answer for the array"))?
            .map_err(Failure::from)?;
        let values = array.values.decode(array.logical).ok_or_else(|| {
            Failure::unsupported(format!(
                "the array is {:?} bytes of a type without a fixed element size: it cannot be decoded into numbers",
                array.logical
            ))
        })?;
        if let Decoded::Strings(_) = values {
            return Err(Failure::unsupported(
                "string arrays do not cross as numpy arrays in this bridge yet",
            ));
        }
        Ok(ArrayRead {
            dimensions: array.dimensions,
            values,
        })
    }

    /// [`delete_objects`] on this session; it writes, so needs `writing`.
    pub fn delete_objects(
        &mut self,
        uris: &[String],
        prune_contained: bool,
    ) -> Result<Outcomes<Vec<String>>, Failure> {
        self.require_writing("delete objects")?;
        delete_objects(self.remote()?, uris, prune_contained)
    }

    /// [`create_dataspaces`] on this session; it writes, so needs `writing`.
    pub fn put_dataspaces(
        &mut self,
        dataspaces: &[String],
        custom_data: &[(String, CustomValue)],
    ) -> Result<Outcomes<String>, Failure> {
        self.require_writing("create dataspaces")?;
        create_dataspaces(self.remote()?, dataspaces, custom_data)
    }

    /// [`remove_dataspaces`] on this session; it writes, so needs `writing`.
    pub fn delete_dataspaces(&mut self, dataspaces: &[String]) -> Result<Outcomes<String>, Failure> {
        self.require_writing("delete dataspaces")?;
        remove_dataspaces(self.remote()?, dataspaces)
    }

    /// [`open_transaction`] on this session. The Transaction protocol is only
    /// announced by a session that writes (`writing_client`), so a read-only
    /// session refuses by name, even for a read-only transaction.
    pub fn start_transaction(
        &mut self,
        dataspaces: &[String],
        read_only: bool,
        message: &str,
    ) -> Result<Transaction, Failure> {
        self.require_writing("open a transaction")?;
        open_transaction(self.remote()?, dataspaces, read_only, message)
    }

    /// [`commit`] on this session.
    pub fn commit_transaction(&mut self, transaction: &Transaction) -> Result<(), Failure> {
        self.require_writing("commit a transaction")?;
        commit(self.remote()?, transaction)
    }

    /// [`rollback`] on this session.
    pub fn rollback_transaction(&mut self, transaction: &Transaction) -> Result<(), Failure> {
        self.require_writing("roll a transaction back")?;
        rollback(self.remote()?, transaction)
    }

    /// [`supported_types`] on this session.
    pub fn supported_types(
        &mut self,
        uri_or_path: &str,
        scope: &str,
        count: bool,
        empty: bool,
    ) -> Result<Vec<SupportedTypeRow>, Failure> {
        supported_types(self.remote()?, uri_or_path, scope, count, empty)
    }

    /// [`channels`] on this session.
    pub fn channel_metadata(&mut self, uris: &[String]) -> Result<ChannelRecords, Failure> {
        channels(self.remote()?, uris)
    }

    /// [`ranges`] on this session.
    pub fn get_ranges(&mut self, ranges_json: &[String]) -> Result<Vec<String>, Failure> {
        ranges(self.remote()?, ranges_json)
    }

    /// [`pack_dataspace`] on this session, read with the schemas the bridge
    /// carries.
    pub fn download_epc(&mut self, dataspace: &str, wanted: &Wanted) -> Result<Packed, Failure> {
        let registry = self.registry.clone();
        pack_dataspace(self.remote()?, &registry, dataspace, wanted)
    }
}

fn identifiers(uri: &str, paths: &[String]) -> Vec<DataArrayIdentifier> {
    paths
        .iter()
        .map(|path| DataArrayIdentifier {
            uri: uri.to_string(),
            path_in_resource: path.clone(),
        })
        .collect()
}

/// A read-only session cannot write: a named refusal rather than the server's
/// later one (the Transaction protocol was never announced). `what` is what
/// the caller tried, for the message.
pub fn refuse_unless_writing(writing: bool, what: &str) -> Result<(), Failure> {
    if writing {
        Ok(())
    } else {
        Err(Failure::unsupported(format!(
            "this session was opened read-only; open it with writing=True to {what}"
        )))
    }
}

// ---------------------------------------------------------------------------
// The operations beyond the first scope, on any blocking client
// ---------------------------------------------------------------------------

/// What a plural write said of each item, **in the order asked**: what the
/// store did, or why it did not. Nothing asked is missing from both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcomes<T> {
    /// The items the store took, each with what it said of it.
    pub done: Vec<(String, T)>,
    /// The ones it did not, with why.
    pub failed: Vec<(String, Failure)>,
}

/// One supported type (SupportedTypes, `GetSupportedTypes`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportedTypeRow {
    /// The qualified type (`resqml20.obj_TriangulatedSetRepresentation`).
    pub data_object_type: String,
    /// How many objects of it, when they were counted.
    pub object_count: Option<i32>,
    /// The relationship kind, by its ETP name.
    pub relationship_kind: String,
}

/// The channels at some URIs (`GetChannelMetadata`), each record as the ETP
/// Avro JSON encoding the Rust library writes (ETP v1.2 section 3.8.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRecords {
    /// URI and the JSON text of its `ChannelMetadataRecord`.
    pub found: Vec<(String, String)>,
    /// The URIs the store did not answer with a record, with why.
    pub failed: Vec<(String, Failure)>,
}

/// The same URI twice in one keyed request would be one key in the answer:
/// refused by name, rather than one of the two reported as unanswered.
fn refuse_repeats(uris: &[String]) -> Result<(), Failure> {
    if uris.is_empty() {
        return Err(Failure::bad_argument("no URI given"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for uri in uris {
        if !seen.insert(uri.as_str()) {
            return Err(Failure::bad_argument(format!("{uri} is given twice")));
        }
    }
    Ok(())
}

/// A keyed answer, back in the order asked. A label the answer does not
/// carry is said, not skipped.
fn in_order<T>(asked: &[String], mut answers: BTreeMap<String, Result<T, ErrorInfo>>) -> Outcomes<T> {
    let mut out = Outcomes {
        done: Vec::new(),
        failed: Vec::new(),
    };
    for uri in asked {
        match answers.remove(uri) {
            Some(Ok(value)) => out.done.push((uri.clone(), value)),
            Some(Err(error)) => out.failed.push((uri.clone(), Failure::of_error(error))),
            None => out.failed.push((
                uri.clone(),
                Failure::new(
                    Kind::AnswerRefused,
                    "the server's answer does not mention this URI",
                ),
            )),
        }
    }
    out
}

/// Delete objects by URI: `retp_client::delete_data_objects`. What each one
/// took with it is the store's statement (the URIs it deleted, contained
/// objects included); `prune_contained` asks it to drop the contained objects
/// the deletion would orphan.
pub fn delete_objects<K: Blocking>(
    client: &mut K,
    uris: &[String],
    prune_contained: bool,
) -> Result<Outcomes<Vec<String>>, Failure> {
    refuse_repeats(uris)?;
    let answers = delete_data_objects(client, uris, prune_contained)?;
    Ok(in_order(uris, answers))
}

/// A listed dataspace as it crosses: its `customData` each value as JSON
/// text through `retp_client::custom_data_json`, the keys it cannot carry
/// named in `notes`.
pub fn dataspace_row(one: Dataspace) -> DataspaceRow {
    let mut custom_data = Vec::new();
    let mut notes = Vec::new();
    for (key, value) in &one.custom_data {
        match custom_data_json(value) {
            Some(json) => custom_data.push((key.clone(), json.to_string())),
            None => notes.push(format!(
                "customData `{key}` is of a kind plain JSON does not carry: left out"
            )),
        }
    }
    DataspaceRow {
        uri: one.uri,
        path: one.path,
        store_last_write: one.store_last_write,
        store_created: one.store_created,
        custom_data,
        notes,
    }
}

/// The canonical URI of a data object (ETP v1.2 section 25.3.7, p.489):
/// `energyml_core::identifiers::EtpUri::object_versioned`, which doubles a
/// quote inside the dataspace or the version (section 25.3.10).
///
/// `dataspace` is a path (`demo`, `osdu/project`), a dataspace URI, or empty
/// for the default dataspace. The uuid is checked and lowercased
/// (`normalize_uuid`), the qualified type read (`QualifiedType::parse`), and
/// the URI built is read back by `EtpUri::parse`: what does not come back as
/// the same object is refused, not returned.
pub fn object_uri(
    dataspace: &str,
    qualified_type: &str,
    uuid: &str,
    version: Option<&str>,
) -> Result<String, Failure> {
    let space = if dataspace.is_empty() {
        None
    } else {
        let parsed = EtpUri::parse(&dataspace_uri(dataspace))
            .map_err(|why| Failure::bad_argument(format!("{dataspace} is not a dataspace: {why}")))?;
        if parsed.uuid.is_some() {
            return Err(Failure::bad_argument(format!(
                "{dataspace} names an object, not a dataspace"
            )));
        }
        parsed.dataspace
    };
    let kind = QualifiedType::parse(qualified_type)
        .map_err(|why| Failure::bad_argument(format!("{qualified_type} is not a qualified type: {why}")))?;
    let uuid =
        normalize_uuid(uuid).map_err(|why| Failure::bad_argument(format!("{uuid} is not a uuid: {why}")))?;
    let uri = EtpUri::object_versioned(space.as_deref(), &kind.to_string(), &uuid, version);
    let back = EtpUri::parse(&uri)
        .map_err(|why| Failure::bad_argument(format!("{uri} does not read back: {why}")))?;
    let same = back.dataspace == space
        && back.uuid.as_deref() == Some(uuid.as_str())
        && back.qualified_type().map(|q| q.to_string()) == Some(kind.to_string())
        && back.object_version.as_deref() == version;
    if !same {
        return Err(Failure::bad_argument(format!(
            "{uri} does not read back as the object asked for"
        )));
    }
    Ok(uri)
}

/// What a push is told, as a caller gives it: the words of the command
/// line's `push-epc`, which builds the same `energyml_etp_store::push::Options`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PushChoices {
    /// At most this many objects in one island (`--objects`).
    pub objects: Option<usize>,
    /// At most this many bytes in one island (`--bytes`).
    pub bytes: Option<u64>,
    /// Only these objects, by URI or uuid (`--only`, `push::chosen`); `None`
    /// is everything.
    pub only: Option<Vec<String>>,
    /// Push the official dictionary's `PropertyKind`s the source references
    /// without holding them (`--well-known`).
    pub well_known: bool,
    /// One array per message, one after the other (`--one-by-one`).
    pub one_by_one: bool,
    /// Send the objects as JSON Energistics, what the Reservoir DDMS REST API
    /// takes (`--json`); XML otherwise.
    pub json: bool,
    /// Leave out what the dataspace holds already (`--skip-present`).
    pub skip_present: bool,
    /// Record each committed island in this file, and leave out what it
    /// records (`--journal`).
    pub journal: Option<PathBuf>,
    /// Send an island the store refuses as a whole again in pieces
    /// (`--split`, [`parse_split`]).
    pub split: Split,
    /// How many times the pieces of one island may be split again
    /// (`--split-limit`); the library's default when `None`.
    pub split_limit: Option<u32>,
    /// Open a new session at most this many times in a row when the
    /// connection falls (`--reconnect`); 0 stops at the fall.
    pub reconnect: u32,
    /// The pause before the first new session (`--reconnect-pause`,
    /// [`pause_of`]); the runner's default when `None`.
    pub reconnect_pause: Option<Duration>,
    /// On each new session, first ask the server to give the lost one back
    /// (OSDU's `ResumeSession`, an extension a server may not support;
    /// `--resume-session`). Off by default; needs `reconnect`.
    pub resume_session: bool,
}

/// `split=` as a caller gives it: `None`, `"halve"` or `"single"`, the
/// command line's `--split` values. Another word is refused by name.
pub fn parse_split(name: Option<&str>) -> Result<Split, Failure> {
    match name {
        None => Ok(Split::Off),
        Some("halve") => Ok(Split::Halve),
        Some("single") => Ok(Split::Single),
        Some(other) => Err(Failure::bad_argument(format!(
            "split `{other}` is not one of halve, single (or None, to stop at a refused island)"
        ))),
    }
}

/// `reconnect_pause=` in seconds, as a caller gives it: a negative, infinite
/// or not-a-number pause is refused by name.
pub fn pause_of(seconds: Option<f64>) -> Result<Option<Duration>, Failure> {
    seconds
        .map(|seconds| {
            Duration::try_from_secs_f64(seconds).map_err(|why| {
                Failure::bad_argument(format!("reconnect_pause {seconds} is not a pause: {why}"))
            })
        })
        .transpose()
}

impl PushChoices {
    /// What opens the sessions of a push that reconnects: the library's
    /// `native::Resuming` over `open` and `resume`, which asks the server for
    /// the lost session first when `resume_session` says so - the conversion
    /// `retp-cli`'s `push_connector` makes of `--resume-session`.
    pub fn resuming<F, G>(&self, open: F, resume: G) -> Resuming<F, G> {
        Resuming {
            open,
            resume,
            ask: self.resume_session,
        }
    }

    /// The library's options, from the words of the same names - the
    /// conversion `retp-cli`'s `push_options` makes of its flags.
    pub fn options<'a>(&'a self, registry: &'a Registry, only: Option<&'a [String]>) -> Options<'a> {
        let mut budget = Budget::default();
        if let Some(objects) = self.objects {
            budget.objects = objects;
        }
        if let Some(bytes) = self.bytes {
            budget.bytes = bytes;
        }
        let default = Options::default();
        Options {
            budget,
            format: if self.json {
                WriteFormat::Json
            } else {
                WriteFormat::Xml
            },
            well_known: if self.well_known {
                WellKnown::Push(registry)
            } else {
                WellKnown::Leave
            },
            only,
            grouping: if self.one_by_one {
                Grouping::OneByOne
            } else {
                Grouping::Together
            },
            // A file knows no `storeLastWrite`: nothing to compare against.
            expected: None,
            overwrite: false,
            skip_present: self.skip_present,
            journal: self.journal.as_deref(),
            split: self.split,
            split_limit: self.split_limit.unwrap_or(default.split_limit),
        }
    }

    /// A budget of nothing, or an `only` that names nothing, is refused by
    /// name: neither is "everything", and neither can send anything.
    pub fn check(&self) -> Result<(), Failure> {
        if self.objects == Some(0) {
            return Err(Failure::bad_argument("objects must be at least 1"));
        }
        if self.bytes == Some(0) {
            return Err(Failure::bad_argument("bytes must be at least 1"));
        }
        if self.only.as_ref().is_some_and(Vec::is_empty) {
            return Err(Failure::bad_argument(
                "only names no object: leave it out to push everything",
            ));
        }
        if self
            .journal
            .as_deref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(Failure::bad_argument(
                "journal names no file: leave it out to keep no journal",
            ));
        }
        if self.resume_session && self.reconnect == 0 {
            return Err(Failure::bad_argument(
                "resume_session asks the server for the lost session when the push reconnects: give reconnect too",
            ));
        }
        Ok(())
    }
}

/// One `customData` value as a caller gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CustomValue {
    /// Text, read as the command line's `--custom KEY=VALUE` reads it
    /// (`retp_client::custom_data_value`): `a,b` is a list of strings, any
    /// other text one string.
    Text(String),
    /// A list of strings, as given (`retp_client::custom_data_strings`): a
    /// value that holds a comma stays one value.
    List(Vec<String>),
}

/// The dataspaces as `retp_client::put_dataspaces` sends them: each path or
/// URI made a URI by `dataspace_uri`, its path read back from that URI by
/// `EtpUri::parse`, and the same `custom_data` on each ([`CustomValue`]).
pub fn dataspaces_to_put(
    dataspaces: &[String],
    custom_data: &[(String, CustomValue)],
) -> Result<Vec<Dataspace>, Failure> {
    if dataspaces.is_empty() {
        return Err(Failure::bad_argument("no dataspace given"));
    }
    dataspaces
        .iter()
        .map(|given| {
            let uri = dataspace_uri(given);
            let parsed = EtpUri::parse(&uri)
                .map_err(|why| Failure::bad_argument(format!("{given} is not a dataspace: {why}")))?;
            if parsed.object_type.is_some() || parsed.uuid.is_some() {
                return Err(Failure::bad_argument(format!(
                    "{given} names an object, not a dataspace"
                )));
            }
            Ok(Dataspace {
                uri,
                path: parsed.dataspace.unwrap_or_default(),
                custom_data: custom_data
                    .iter()
                    .map(|(key, value)| {
                        let value = match value {
                            CustomValue::Text(text) => custom_data_value(text),
                            CustomValue::List(values) => custom_data_strings(values),
                        };
                        (key.clone(), value)
                    })
                    .collect(),
                ..Default::default()
            })
        })
        .collect()
}

/// Create or replace dataspaces: `retp_client::put_dataspaces` with
/// [`dataspaces_to_put`]. Each dataspace answers for itself, by its URI.
pub fn create_dataspaces<K: Blocking>(
    client: &mut K,
    dataspaces: &[String],
    custom_data: &[(String, CustomValue)],
) -> Result<Outcomes<String>, Failure> {
    let spaces = dataspaces_to_put(dataspaces, custom_data)?;
    let uris: Vec<String> = spaces.iter().map(|space| space.uri.clone()).collect();
    refuse_repeats(&uris)?;
    let answers = put_dataspaces(client, &spaces)?;
    Ok(in_order(&uris, answers))
}

/// Delete dataspaces, each given by its path or URI (`dataspace_uri`):
/// `retp_client::delete_dataspaces`.
pub fn remove_dataspaces<K: Blocking>(
    client: &mut K,
    dataspaces: &[String],
) -> Result<Outcomes<String>, Failure> {
    let uris: Vec<String> = dataspaces.iter().map(|given| dataspace_uri(given)).collect();
    refuse_repeats(&uris)?;
    let answers = delete_dataspaces(client, &uris)?;
    Ok(in_order(&uris, answers))
}

/// Open a transaction over dataspaces (paths or URIs, `dataspace_uri`):
/// `retp_client::start_transaction`.
///
/// The store's two kinds of no stay apart, as the library keeps them: a
/// refusal (*not now*) is a server answer, an absent Transaction protocol
/// (*never here*) is [`Kind::Unsupported`].
pub fn open_transaction<K: Blocking>(
    client: &mut K,
    dataspaces: &[String],
    read_only: bool,
    message: &str,
) -> Result<Transaction, Failure> {
    let uris: Vec<String> = dataspaces.iter().map(|given| dataspace_uri(given)).collect();
    match start_transaction(client, &uris, read_only, message)? {
        Opened::Open(transaction) => Ok(transaction),
        Opened::Refused(why) => Err(Failure::said_no(format!(
            "the store did not open the transaction: {why}"
        ))),
        Opened::Unsupported(why) => Err(Failure::unsupported(format!(
            "the store has no transactions: {why}"
        ))),
    }
}

/// Commit a transaction: `retp_client::commit_transaction`. A refused commit
/// means the writes of the transaction are **not** in the store, and says so.
pub fn commit<K: Blocking>(client: &mut K, transaction: &Transaction) -> Result<(), Failure> {
    match commit_transaction(client, transaction)? {
        Ended::Done => Ok(()),
        Ended::Refused(why) => Err(Failure::said_no(format!(
            "the store refused the commit: {why} (the writes of the transaction are not in the store)"
        ))),
    }
}

/// Roll a transaction back: `retp_client::rollback_transaction`. A refused
/// rollback means its writes may still be there, and says so.
pub fn rollback<K: Blocking>(client: &mut K, transaction: &Transaction) -> Result<(), Failure> {
    match rollback_transaction(client, transaction)? {
        Ended::Done => Ok(()),
        Ended::Refused(why) => Err(Failure::said_no(format!(
            "the store refused the rollback: {why} (the writes of the transaction may still be there)"
        ))),
    }
}

/// The data object types under a dataspace (a path or a URI) or an object:
/// `retp_client::list_supported_types`. The library refuses, before sending,
/// any scope but `sources` and `targets`.
///
/// An answer a server cut short with an error is **refused**, not returned as
/// if it were the whole list.
pub fn supported_types<K: Blocking>(
    client: &mut K,
    uri_or_path: &str,
    scope: &str,
    count: bool,
    empty: bool,
) -> Result<Vec<SupportedTypeRow>, Failure> {
    let list = list_supported_types(
        client,
        &dataspace_uri(uri_or_path),
        parse_scope(scope)?,
        count,
        empty,
    )?;
    if let Some(error) = list.ended_by {
        return Err(Failure::server(
            error.code,
            format!("the supported-type list was cut short: {}", error.message),
        ));
    }
    Ok(list
        .items
        .into_iter()
        .map(|one| SupportedTypeRow {
            data_object_type: one.data_object_type,
            object_count: one.object_count,
            relationship_kind: format!("{:?}", one.relationship_kind),
        })
        .collect())
}

/// The channels at these URIs (a log, a channel set, a channel):
/// `retp_client::channels::channel_metadata`, each record in its Avro JSON
/// encoding (`retp_avro::Avro::to_json`).
pub fn channels<K: Blocking>(client: &mut K, uris: &[String]) -> Result<ChannelRecords, Failure> {
    refuse_repeats(uris)?;
    let answers = channel_metadata(client, uris)?;
    let mut out = ChannelRecords {
        found: Vec::new(),
        failed: Vec::new(),
    };
    for (uri, answer) in uris.iter().zip(answers) {
        match answer {
            Ok(record) => out.found.push((uri.clone(), record.to_json().to_string())),
            Err(error) => out.failed.push((uri.clone(), Failure::of_error(error))),
        }
    }
    Ok(out)
}

/// Channel values over index intervals: `retp_client::channels::get_ranges`,
/// each range read from its Avro JSON encoding
/// (`ChannelRangeInfo::from_json_str`), each data item written back in it.
/// The request's uuid is a fresh one (`native::random_uuid`).
pub fn ranges<K: Blocking>(client: &mut K, ranges: &[String]) -> Result<Vec<String>, Failure> {
    let asked = ranges_from_json(ranges)?;
    match get_ranges(client, native::random_uuid(), &asked)? {
        Ok(items) => Ok(items.iter().map(|item| item.to_json().to_string()).collect()),
        Err(error) => Err(Failure::of_error(error)),
    }
}

/// The ranges of [`ranges`], read; each refusal names the range.
pub fn ranges_from_json(ranges: &[String]) -> Result<Vec<ChannelRangeInfo>, Failure> {
    if ranges.is_empty() {
        return Err(Failure::bad_argument("no range given"));
    }
    ranges
        .iter()
        .enumerate()
        .map(|(rank, text)| {
            ChannelRangeInfo::from_json_str(text).map_err(|why| {
                Failure::bad_argument(format!("range {rank} is not a ChannelRangeInfo: {why}"))
            })
        })
        .collect()
}

/// A dataspace (a path or a URI) as an EPC, in memory:
/// `energyml_etp_store::pack::pack`. The package's bytes, the arrays its
/// objects name (for the caller to write into the files the package names),
/// and what could not be taken, said rather than left out quietly.
pub fn pack_dataspace<K: Blocking>(
    client: &mut K,
    registry: &Registry,
    dataspace: &str,
    wanted: &Wanted,
) -> Result<Packed, Failure> {
    Ok(blocking(pack(client, dataspace, registry, wanted))?)
}

/// Objects, as EnergyML JSON texts, in an in-memory package. Pure Rust, no
/// server: every refusal here is tested natively.
pub fn stage(registry: &Arc<Registry>, jsons: &[String]) -> Result<EpcStore, Failure> {
    if jsons.is_empty() {
        return Err(Failure::bad_argument("no object given"));
    }
    let bytes = energyml_storage::extract::empty_package(None).map_err(Failure::local)?;
    let mut store = EpcStore::from_reader(Box::new(Cursor::new(bytes)), APPLICATION_NAME, registry.clone())
        .map_err(Failure::local)?;
    for (rank, text) in jsons.iter().enumerate() {
        let document = energyml_core::json::read_string(text, registry).map_err(|why| {
            Failure::bad_argument(format!("object {rank} does not read as EnergyML JSON: {why}"))
        })?;
        if !document.referenced.is_empty() {
            return Err(Failure::unsupported(format!(
                "object {rank} carries `_data` (referenced objects expanded in place): put each object on its own"
            )));
        }
        store
            .put(document.root)
            .map_err(|why| Failure::bad_argument(format!("object {rank} cannot be put: {why}")))?;
    }
    Ok(store)
}
