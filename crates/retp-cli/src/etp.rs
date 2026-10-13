//! `retp` - try an ETP v1.2 server from the command line (also run as
//! `energyml etp …`).
//!
//! Argument parsing and printing only: the probe, the session and the
//! requests are `retp-client`'s.

use std::io::{BufRead, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};

use retp_client::arrays::{ArrayProgress, ArrayValues, array_metadata, budget as array_budget, read_arrays};
use retp_client::download::{Download, Progress, download_resumable};
use retp_client::login::{Choice, Login};
use retp_client::native::{self, CertificateCheck, Connect, Endpoint, Reopen};
use retp_client::resume::{Retry, RunEvent, Setback};
use retp_client::{
    ClientError, ResourceQuery, ServerProfile, dataspace_uri, delete_dataspaces, described_client,
    get_data_objects, list_dataspaces, list_resources, list_supported_types, object_file_name,
    plain_text_warning, put_dataspaces, reading_client, writing_client,
};
use retp_proto::describe::OfferDescription;
use retp_proto::negotiate::protocol_name;
use retp_proto::probe::Profile;
use retp_proto::uris::{UriUse, check_uri};
use retp_types::datatypes::data_array_types::DataArrayIdentifier;
use retp_types::datatypes::object::ContextScopeKind;
use retp_types::datatypes::object::Dataspace;

type Session = native::Remote;

#[derive(Parser)]
#[command(
    name = "retp",
    about = "Try an ETP v1.2 server: probe it, open a session, list, read",
    version
)]
pub struct EtpArgs {
    /// Say more: what the ETP crates log below a warning.
    #[arg(long, short, global = true)]
    verbose: bool,
    #[command(subcommand)]
    command: EtpCommand,
}

/// Whether the ETP crates' lines below a warning are shown - `--verbose`, or
/// `verbose on` in a shell.
static VERBOSE: AtomicBool = AtomicBool::new(false);

/// The ETP crates' log lines, on stderr: a warning always - what a server
/// got wrong and the client tolerated -, the rest when verbose. Other
/// crates' (rustls') only when they warn.
struct Stderr;

impl log::Log for Stderr {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Warn
            || (VERBOSE.load(Ordering::Relaxed) && metadata.target().starts_with("energyml"))
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            let level = match record.level() {
                log::Level::Error => "error",
                log::Level::Warn => "warning",
                log::Level::Info => "info",
                log::Level::Debug | log::Level::Trace => "debug",
            };
            eprintln!("{level}: {}", record.args());
        }
    }

    fn flush(&self) {}
}

/// Where the server is, and how to be let in.
#[derive(Args)]
struct Server {
    /// The server's WebSocket URL: `wss://host[:port]/path` - or, with
    /// `--rest`, its REST API's: `https://host/api/reservoir-ddms/v2`.
    url: String,
    /// Talk to OSDU's Reservoir DDMS through its REST API instead of the ETP
    /// WebSocket: the same commands, the same shell. What REST has no
    /// counterpart for - acknowledgements, keepalive, notifications - is
    /// refused by name (see the REST client notes of `retp-client`).
    #[arg(long)]
    rest: bool,
    #[command(flatten)]
    login: LoginArgs,
    /// An OSDU data partition: talks to the server as OSDU's Reservoir DDMS,
    /// which wants a `data-partition-id` header.
    #[arg(long)]
    partition: Option<String>,
    /// Do not check the server's certificate - for a server whose certificate
    /// is known to be expired, self-signed or misnamed. The session stays
    /// encrypted, to a server nobody vouched for.
    #[arg(long, alias = "unsecure")]
    insecure: bool,
    /// Ask the server to acknowledge every request (flag 0x10, §3.5.4.2), and
    /// print each acknowledgement as it comes.
    #[arg(long)]
    ack: bool,
    /// Connect as a front end can: no header at all - ETP's attributes and the
    /// OSDU partition in the query string, the credentials in an `Authorize`
    /// once connected (§4.1.4.1, §4.3.2). Says whether a server will take the
    /// front end.
    #[arg(long)]
    as_browser: bool,
    /// Send a Ping when the session has been quiet this many seconds, so that
    /// no server or proxy closes it for being idle; 0 never pings. 30 by
    /// default.
    #[arg(long, value_name = "SECONDS")]
    keepalive: Option<u64>,
    /// The data object types to ask for (`resqml20.*`, `eml23.*`…). None
    /// given: every release this build knows, and every type the server
    /// announces before the session.
    #[arg(long = "type", value_name = "QUALIFIED_TYPE")]
    types: Vec<String>,
    /// What this client announces, from a JSON file: types, formats,
    /// protocols, endpoint capabilities, tolerance (`docs/client.md`).
    #[arg(long, value_name = "FILE", conflicts_with = "types")]
    offer: Option<PathBuf>,
    /// How many requests of one operation - the handfuls of a dataspace,
    /// the blocks of an array - are in flight at once, on either transport.
    /// 4 by default; `1` sends them one after the other.
    #[arg(long, alias = "parallel", value_name = "N")]
    at_once: Option<usize>,
}

/// How to log in - one way at a time. Secrets are better given through the
/// environment (`ETP_TOKEN`, `ETP_PASSWORD`, `ETP_CLIENT_SECRET`) than on a
/// command line, which lands in the shell's history.
#[derive(Args)]
struct LoginArgs {
    /// A bearer token. Read from `ETP_TOKEN` when no other login is given.
    #[arg(long, conflicts_with_all = ["user", "token_url", "azure_tenant", "cognito_region"])]
    token: Option<String>,
    /// HTTP Basic's user - or, with `--token-url`, the password grant's, or
    /// with `--cognito-region`, the Cognito user. Read from `ETP_USER` when
    /// not given.
    #[arg(long)]
    user: Option<String>,
    /// The password of `--user`. Read from `ETP_PASSWORD` when not given.
    #[arg(long)]
    password: Option<String>,
    /// An OAuth 2.0 token endpoint: a token for `--user` and its password
    /// (the password grant), or by client credentials (`--client-id`,
    /// `--client-secret`) when no user is given; `--oauth-scope` for either.
    #[arg(long, conflicts_with_all = ["azure_tenant", "cognito_region"])]
    token_url: Option<String>,
    /// An Azure AD tenant: a token from its endpoint, by client credentials.
    #[arg(long, conflicts_with = "cognito_region")]
    azure_tenant: Option<String>,
    /// The AWS region of a Cognito user pool: a token for `--user` and
    /// `--password`, through the app client `--client-id`.
    #[arg(long)]
    cognito_region: Option<String>,
    /// The OAuth or Cognito app client's id.
    #[arg(long)]
    client_id: Option<String>,
    /// Its secret. Read from `ETP_CLIENT_SECRET` when not given.
    #[arg(long)]
    client_secret: Option<String>,
    /// The scope to ask the token for. Not `--scope`, which is ETP's
    /// (`resources`, `supported-types`).
    #[arg(long)]
    oauth_scope: Option<String>,
}

fn from_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

impl LoginArgs {
    /// Which login the options ask for - the choice only; getting the
    /// credentials is the library's.
    fn login(&self) -> Result<Option<Login>> {
        let choice = Choice {
            token: self.token.clone(),
            user: self.user.clone(),
            password: self.password.clone(),
            token_url: self.token_url.clone(),
            azure_tenant: self.azure_tenant.clone(),
            cognito_region: self.cognito_region.clone(),
            client_id: self.client_id.clone(),
            client_secret: self.client_secret.clone(),
            oauth_scope: self.oauth_scope.clone(),
        };
        Ok(choice.login(from_env)?)
    }
}

impl Server {
    fn profile(&self) -> ServerProfile {
        match &self.partition {
            Some(data_partition) => ServerProfile::Rddms {
                data_partition: data_partition.clone(),
            },
            None => ServerProfile::Standard,
        }
    }

    /// The server, with the credentials obtained and the warnings said.
    fn endpoint(&self) -> Result<Endpoint> {
        let certificates = if self.insecure {
            CertificateCheck::Skip
        } else {
            CertificateCheck::Verify
        };
        for warning in [plain_text_warning(&self.url), certificates.warning(&self.url)]
            .into_iter()
            .flatten()
        {
            eprintln!("warning: {warning}");
        }
        let credentials = match self.login.login()? {
            Some(login) => {
                let obtained = login.obtain().context("logging in")?;
                match obtained.expires_in {
                    Some(lasts) => eprintln!(
                        "credentials: {}, for {} s",
                        obtained.credentials.scheme(),
                        lasts.as_secs()
                    ),
                    None => eprintln!("credentials: {}", obtained.credentials.scheme()),
                }
                Some(obtained.credentials)
            }
            None => None,
        };
        Ok(Endpoint {
            url: self.url.clone(),
            profile: self.profile(),
            credentials,
            certificates,
            handshake: if self.as_browser {
                Profile::Browser
            } else {
                Profile::Native
            },
        })
    }

    fn open(&self) -> Result<Session> {
        self.open_for(false)
    }

    /// A session whose offer says it will write: Store, DataArray,
    /// Transaction with their writing messages (`writing_client`). Asked for
    /// by the commands that write, so that a read-only errand announces
    /// nothing it will not do.
    fn open_for(&self, writing: bool) -> Result<Session> {
        if self.rest {
            return self.open_rest();
        }
        // The offer first: a file that is wrong costs no login.
        let (name, version, id) = (
            "retp command line",
            env!("CARGO_PKG_VERSION"),
            native::random_uuid(),
        );
        let (offer, handlers) = match &self.offer {
            Some(file) => {
                let text =
                    std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
                let description =
                    OfferDescription::parse(&text).with_context(|| file.display().to_string())?;
                described_client(name, version, id, &description)
                    .map_err(|why| anyhow::anyhow!("{}: {why}", file.display()))?
            }
            None if writing => writing_client(name, version, id, &self.types),
            None => reading_client(name, version, id, &self.types),
        };
        let endpoint = self.endpoint()?;
        // No type named: whatever the server announces is asked for too.
        let offer = match (&self.offer, self.types.is_empty()) {
            (None, true) => native::fit_to_server(&endpoint, offer).0,
            _ => offer,
        };
        let mut client = native::connect(Connect {
            endpoint,
            offer,
            handlers,
            max_message: None,
        })
        .with_context(|| format!("opening a session with {}", self.url))?;
        client.ask_acknowledge(self.ack);
        if let Some(seconds) = self.keepalive {
            client.set_keepalive((seconds > 0).then(|| std::time::Duration::from_secs(seconds)));
        }
        if let Some(at_once) = self.at_once {
            client.set_at_once(at_once);
        }
        Ok(native::Remote::Etp(Box::new(client)))
    }

    /// The REST client: nothing to open, but what only a session has means
    /// nothing here - said, not ignored.
    fn open_rest(&self) -> Result<Session> {
        if !(self.url.starts_with("https://") || self.url.starts_with("http://")) {
            let hint = if self.url.contains("reservoir-ddms-etp") {
                " - OSDU usually serves it at the same path without `-etp`, but that is a habit, not a rule"
            } else {
                ""
            };
            anyhow::bail!(
                "--rest takes the REST API's URL (https://…/api/reservoir-ddms/v2), not a WebSocket's{hint}"
            );
        }
        for (flag, given) in [
            ("--ack", self.ack),
            ("--as-browser", self.as_browser),
            ("--keepalive", self.keepalive.is_some()),
            ("--type", !self.types.is_empty()),
            ("--offer", self.offer.is_some()),
        ] {
            if given {
                anyhow::bail!(
                    "{flag} means nothing over REST: there is no session to negotiate, acknowledge or keep alive"
                );
            }
        }
        let mut client = native::rest_client(&self.endpoint()?);
        if let Some(at_once) = self.at_once {
            client = client.with_at_once(at_once);
        }
        Ok(native::Remote::Rest(Box::new(client)))
    }
}

/// Which way from a URI, in Discovery's graph (§8.1.1).
#[derive(Clone, Copy, ValueEnum)]
enum Scope {
    /// The URI itself.
    #[value(name = "self")]
    Itself,
    /// What it refers to - for a dataspace, the objects in it.
    Targets,
    /// What refers to it.
    Sources,
    /// Its targets, and itself.
    TargetsOrSelf,
    /// Its sources, and itself.
    SourcesOrSelf,
}

impl Scope {
    fn kind(self) -> ContextScopeKind {
        match self {
            Scope::Itself => ContextScopeKind::Self_,
            Scope::Targets => ContextScopeKind::Targets,
            Scope::Sources => ContextScopeKind::Sources,
            Scope::TargetsOrSelf => ContextScopeKind::TargetsOrSelf,
            Scope::SourcesOrSelf => ContextScopeKind::SourcesOrSelf,
        }
    }
}

/// A data object format.
#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Xml,
    Json,
}

impl Format {
    fn name(self) -> &'static str {
        match self {
            Format::Xml => "xml",
            Format::Json => "json",
        }
    }
}

/// What `resources` takes, in one command or in a shell.
#[derive(Args)]
struct ResourcesArgs {
    /// The canonical URI to start from - or a dataspace's path.
    #[arg(long, default_value = "eml:///")]
    uri: String,
    /// How many levels down.
    #[arg(long, default_value_t = 1)]
    depth: i32,
    /// Which way from the URI.
    #[arg(long, value_enum, default_value = "targets")]
    scope: Scope,
    /// Only this type, repeatable (`resqml20.obj_TriangulatedSetRepresentation`,
    /// `resqml20.*`); the session's types when none is given.
    #[arg(long, value_name = "QUALIFIED_TYPE")]
    only: Vec<String>,
    /// Also the edges between the resources.
    #[arg(long)]
    edges: bool,
    /// Ask the store to count each resource's sources and targets.
    #[arg(long)]
    count: bool,
    /// Only the resources of this activity status (`activeStatusFilter`);
    /// every status otherwise.
    #[arg(long, value_enum)]
    active_status: Option<ActiveStatus>,
}

/// What `get` takes, in one command or in a shell.
#[derive(Args)]
struct GetArgs {
    /// The objects' URIs.
    #[arg(required = true)]
    uris: Vec<String>,
    /// Write each object to a file here instead of printing it.
    #[arg(long)]
    out: Option<PathBuf>,
    /// The format to ask the objects in - one the session agreed.
    #[arg(long, value_enum, default_value = "xml")]
    format: Format,
}

/// What `supported-types` takes, in one command or in a shell.
#[derive(Args)]
struct SupportedTypesArgs {
    /// The canonical URI to look from - or a dataspace's path.
    #[arg(long, default_value = "eml:///")]
    uri: String,
    /// `targets` or `sources`: the only two §22.3.1 allows.
    #[arg(long, value_enum, default_value = "targets")]
    scope: Scope,
    /// Count the objects of each type.
    #[arg(long)]
    count: bool,
    /// Also the types the store supports and holds none of.
    #[arg(long)]
    empty: bool,
}

/// What `download-epc` takes, in one command or in a shell.
#[derive(Args)]
struct PackArgs {
    /// The dataspace: its path (`demo`, `carl/teapot`) or its URI; the
    /// default dataspace when empty.
    dataspace: String,
    /// The EPC to write. Its arrays go into HDF5 files beside it, named as
    /// the package names them.
    #[arg(long, short)]
    out: PathBuf,
    /// Only these types, repeatable (`resqml20.*`).
    #[arg(long = "only", value_name = "QUALIFIED_TYPE")]
    only: Vec<String>,
    /// The objects alone: no array is read, and the package names files that
    /// are not there.
    #[arg(long)]
    no_arrays: bool,
    /// Also what the kept objects reference, and what those reference - with
    /// `--only`, the difference between a package and a package that dangles.
    #[arg(long)]
    with_references: bool,
}

/// What `prefetch` takes, in one command or in a shell.
#[derive(Args)]
struct PrefetchArgs {
    /// The objects' URIs.
    #[arg(required = true)]
    uris: Vec<String>,
    /// Without their arrays' metadata.
    #[arg(long)]
    no_arrays: bool,
}

/// What `array` takes, in one command or in a shell.
#[derive(Args)]
struct ArrayArgs {
    /// The URI of what holds the arrays - the `EpcExternalPartReference` in
    /// RESQML 2.0.1, the data object itself from 2.2 on.
    uri: String,
    /// Paths of arrays in it, as the object's `PathInHdfFile` /
    /// `PathInExternalFile` spells them.
    #[arg(required = true)]
    paths: Vec<String>,
    /// Only what each array is - dimensions, types -, nothing read.
    #[arg(long)]
    metadata: bool,
    /// How many values of each array to print.
    #[arg(long, default_value_t = 10)]
    show: usize,
}

/// What `download` takes, in one command or in a shell.
#[derive(Args)]
struct DownloadArgs {
    /// The dataspace: its path (`demo`, `carl/teapot`) or its URI; the default
    /// dataspace when empty.
    dataspace: String,
    /// Where the files go, one per object; created if missing.
    #[arg(long)]
    out: PathBuf,
    /// Only this type, repeatable.
    #[arg(long, value_name = "QUALIFIED_TYPE")]
    only: Vec<String>,
    /// The format to ask the objects in.
    #[arg(long, value_enum, default_value = "xml")]
    format: Format,
    /// How many objects one request asks for.
    #[arg(long, default_value_t = 50)]
    batch: usize,
    /// Write again the files already there - by default they are taken as
    /// done, so that the same download run again goes on where it stopped.
    #[arg(long)]
    overwrite: bool,
    /// How many times in a row one step may fail - a lost connection, a
    /// timeout - before the download stops; each failure opens a new session.
    #[arg(long, default_value_t = 3)]
    retries: u32,
}

#[derive(Subcommand)]
enum EtpCommand {
    /// What the server says of itself before any connection
    /// (`/.well-known/etp-server-capabilities`).
    Probe(Server),
    /// Open a session, say what was agreed, ping, close.
    Connect(Server),
    /// The store's dataspaces (Dataspace, protocol 24).
    Dataspaces(Server),
    /// What a dataspace or an object holds (Discovery, protocol 3).
    Resources {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        args: ResourcesArgs,
    },
    /// Objects by canonical URI (Store, protocol 4): printed, or written to a
    /// directory.
    Get {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        args: GetArgs,
    },
    /// The data object types found from a URI (SupportedTypes, protocol 25).
    SupportedTypes {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        args: SupportedTypesArgs,
    },
    /// Data arrays (DataArray, protocol 9): their metadata first, then each
    /// read whole or by sub-arrays, as it fits.
    Array {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        args: ArrayArgs,
    },
    /// Objects, what they reference and their arrays' metadata, in one
    /// request - REST only (`--rest`).
    Prefetch {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        args: PrefetchArgs,
    },
    /// A dataspace as an EPC, its arrays in HDF5 files beside it.
    DownloadEpc {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        args: PackArgs,
    },
    /// A dataspace into a directory, one file per object; run again, it goes
    /// on where it stopped.
    Download {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        args: DownloadArgs,
    },
    /// Create a dataspace (Dataspace, protocol 24) - `PutDataspaces`, or
    /// `POST /dataspaces` over REST.
    CreateDataspace {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        args: DataspaceArgs,
    },
    /// Delete a dataspace and everything in it (`DeleteDataspaces`, or
    /// `DELETE /dataspaces/{id}` over REST).
    DeleteDataspace {
        #[command(flatten)]
        server: Server,
        /// The dataspace: its path or its URI.
        dataspace: String,
    },
    /// Send an EPC - its objects and their arrays - into a dataspace, in
    /// islands of short transactions (Store, DataArray, Transaction).
    Push {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        args: PushArgs,
    },
    /// Open one session, then read commands one per line - `dataspaces`,
    /// `resources`, `get`, `supported-types`, `download`, `info`, `ping`,
    /// `verbose`, `reconnect`, `exit`. From a terminal, or from a file piped in.
    Shell(Server),
}

/// What `create-dataspace` takes.
#[derive(Args)]
struct DataspaceArgs {
    /// The dataspace: its path (`demo`, `carl/teapot`) or its URI.
    dataspace: String,
    /// A `customData` entry, `key=value`, repeatable - an OSDU store wants
    /// `owners`, `viewers` and `legaltags`; a value with commas is a list.
    #[arg(long = "custom", value_name = "KEY=VALUE")]
    custom: Vec<String>,
}

/// What `push` takes.
#[derive(Args)]
struct PushArgs {
    /// The EPC to send; its arrays are read from the files it names, beside
    /// it (`--features hdf5`).
    epc: PathBuf,
    /// The dataspace to send into: its path or its URI.
    dataspace: String,
    /// Send the objects as JSON Energistics rather than XML - what the
    /// Reservoir DDMS REST API takes; implied by `--rest`.
    #[arg(long)]
    json: bool,
    /// Only these objects, by URI or by uuid, repeatable. Everything
    /// otherwise.
    #[arg(long = "only", value_name = "URI_OR_UUID")]
    only: Vec<String>,
    /// Also the official dictionary's PropertyKinds the package references
    /// without holding - for a store that refuses a commit for them.
    #[arg(long)]
    well_known: bool,
    /// At most this many objects per island (transaction).
    #[arg(long, value_name = "N")]
    objects: Option<usize>,
    /// At most this many bytes of XML per island.
    #[arg(long, value_name = "BYTES")]
    bytes: Option<u64>,
    /// Send each array in its own message, one after the other, instead of
    /// several per message and in flight - for a store that mishandles a
    /// PutDataArrays of several arrays, or to see what one alone provokes.
    #[arg(long)]
    one_by_one: bool,
}

/// What `watch` takes.
#[derive(Args)]
struct WatchArgs {
    /// The context: a dataspace, or an object, by URI.
    uri: String,
    /// How far from it in the graph.
    #[arg(long, default_value_t = 1)]
    depth: i32,
    /// Which side of the graph.
    #[arg(long, value_enum, default_value = "targets-or-self")]
    scope: Scope,
    /// Only these types (`resqml20.*`); every type otherwise.
    #[arg(long, value_name = "QUALIFIED_TYPE")]
    types: Vec<String>,
    /// Have each change carry the object's data, not only its resource.
    #[arg(long)]
    with_data: bool,
}

/// What `changes-since` takes.
#[derive(Args)]
struct ChangesArgs {
    /// The dataspace: its path or its URI.
    dataspace: String,
    /// Since when: an ISO 8601 UTC time (`2026-09-17T10:00:00Z`), or
    /// `watermark` for the last time heard from this server in this session.
    since: String,
    /// Only these types; every type otherwise.
    #[arg(long, value_name = "QUALIFIED_TYPE")]
    types: Vec<String>,
}

/// A command inside an open session: the same arguments as the one-shot
/// commands, without the server's - it is already there.
#[derive(Subcommand)]
enum InSession {
    /// What the session agreed.
    Info,
    /// A `Ping`, and the time its `Pong` took.
    Ping,
    /// The store's dataspaces (Dataspace, protocol 24).
    Dataspaces,
    /// What a dataspace or an object holds (Discovery, protocol 3).
    Resources(ResourcesArgs),
    /// Objects by canonical URI (Store, protocol 4).
    Get(GetArgs),
    /// The data object types found from a URI (SupportedTypes, protocol 25).
    SupportedTypes(SupportedTypesArgs),
    /// Data arrays (DataArray, protocol 9).
    Array(ArrayArgs),
    /// Objects, what they reference and their arrays' metadata - REST only.
    Prefetch(PrefetchArgs),
    /// A dataspace as an EPC, its arrays in HDF5 files beside it.
    DownloadEpc(PackArgs),
    /// A dataspace into a directory, one file per object.
    Download(DownloadArgs),
    /// Create a dataspace (Dataspace, protocol 24).
    CreateDataspace(DataspaceArgs),
    /// Delete a dataspace and everything in it.
    DeleteDataspace {
        /// The dataspace: its path or its URI.
        dataspace: String,
    },
    /// Send an EPC into a dataspace, in islands of short transactions.
    Push(PushArgs),
    /// Watch a context for changes (StoreNotification, protocol 5): prints
    /// the uuid the watch goes by. Read what comes with `notifications`.
    Watch(WatchArgs),
    /// End a watch by its uuid.
    Unwatch {
        /// The uuid `watch` printed.
        uuid: String,
    },
    /// What the store said on its own about the watches, typed - after
    /// waiting this many seconds for more (0: only what is already here).
    Notifications {
        #[arg(long, default_value_t = 0)]
        wait: u64,
    },
    /// What changed in a dataspace since a time: written and deleted
    /// resources (Discovery, protocol 3) - the catch-up after a cut.
    ChangesSince(ChangesArgs),
    /// The channels at these URIs (ChannelSubscribe, protocol 21): ids,
    /// names, units - the ids are what data items name.
    Channels {
        /// Logs, channel sets or channels, by URI.
        uris: Vec<String>,
    },
    /// Subscribe to channels by id (protocol 21). Read what comes with
    /// `channel-data`.
    SubscribeChannels {
        /// Channel ids, as `channels` printed them.
        ids: Vec<i64>,
        /// Also the changes to values already sent.
        #[arg(long)]
        changes: bool,
    },
    /// Unsubscribe channels by id.
    UnsubscribeChannels { ids: Vec<i64> },
    /// What came on the channel streams - subscribed (21) and streamed
    /// (1) -, after waiting this many seconds for more.
    ChannelData {
        #[arg(long, default_value_t = 0)]
        wait: u64,
    },
    /// Ask a producer to stream its channels (ChannelStreaming, protocol
    /// 1), or to stop: `streaming on`, `streaming off`.
    Streaming {
        #[arg(value_parser = ["on", "off"])]
        state: String,
    },
    /// Ask the server to acknowledge each request, or not: `ack on`, `ack off`;
    /// alone, it toggles. Each acknowledgement is printed after the command.
    Ack {
        #[arg(value_parser = ["on", "off"])]
        state: Option<String>,
    },
    /// Every message sent and received, or not: `verbose on`, `verbose off`.
    Verbose {
        #[arg(value_parser = ["on", "off"])]
        state: Option<String>,
    },
    /// Close this session and open a new one - login included.
    Reconnect,
    /// Close the session and leave.
    #[command(alias = "quit")]
    Exit,
}

/// One line of a shell.
#[derive(Parser)]
#[command(name = "etp>", no_binary_name = true, disable_version_flag = true)]
struct Line {
    #[command(subcommand)]
    command: InSession,
}

pub fn run(args: EtpArgs) -> Result<ExitCode> {
    VERBOSE.store(args.verbose, Ordering::Relaxed);
    if log::set_logger(&Stderr).is_ok() {
        log::set_max_level(log::LevelFilter::Debug);
    }
    match args.command {
        EtpCommand::Probe(server) => probe(&server),
        EtpCommand::Connect(server) => one_shot(&server, |session| {
            info(session);
            ping(session)?;
            Ok(true)
        }),
        EtpCommand::Dataspaces(server) => one_shot(&server, dataspaces),
        EtpCommand::Resources { server, args } => {
            // Before connecting: a URI Discovery would refuse costs no session.
            check_uri(&dataspace_uri(&args.uri), UriUse::CanonicalOnly, false)
                .map_err(|incident| anyhow::anyhow!("{incident}"))?;
            one_shot(&server, |session| resources(session, &args, &server.types))
        }
        EtpCommand::Get { server, args } => one_shot(&server, |session| get(session, &args)),
        EtpCommand::SupportedTypes { server, args } => {
            one_shot(&server, |session| supported_types(session, &args))
        }
        EtpCommand::Array { server, args } => one_shot(&server, |session| arrays(session, &args)),
        EtpCommand::Prefetch { server, args } => one_shot(&server, |session| prefetch(session, &args)),
        EtpCommand::DownloadEpc { server, args } => one_shot(&server, |session| download_epc(session, &args)),
        EtpCommand::Download { server, args } => {
            let mut session = None;
            let complete = download_to(&server, &mut session, &args);
            if let Some(open) = session {
                let _ = open.close("done");
            }
            Ok(if complete? {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        EtpCommand::CreateDataspace { server, args } => {
            one_shot_writing(&server, |session| create_dataspace(session, &args))
        }
        EtpCommand::DeleteDataspace { server, dataspace } => {
            one_shot_writing(&server, |session| delete_dataspace(session, &dataspace))
        }
        EtpCommand::Push { server, args } => {
            let args = PushArgs {
                json: args.json || server.rest,
                ..args
            };
            one_shot_writing(&server, |session| push_epc(session, &args))
        }
        EtpCommand::Shell(server) => shell(&server),
    }
}

/// [`one_shot`], on a session whose offer says it writes.
fn one_shot_writing(server: &Server, work: impl FnOnce(&mut Session) -> Result<bool>) -> Result<ExitCode> {
    let mut session = server.open_for(true)?;
    let complete = work(&mut session);
    said_acknowledgements(&mut session);
    let complete = complete?;
    session.close("done")?;
    Ok(if complete {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

/// One `customData` value from the command line: `a,b` is a list of strings,
/// anything else one string. An OSDU store reads its owners as a list.
fn create_dataspace(session: &mut Session, args: &DataspaceArgs) -> Result<bool> {
    let uri = dataspace_uri(&args.dataspace);
    let path = uri
        .strip_prefix("eml:///dataspace('")
        .and_then(|rest| rest.strip_suffix("')"))
        .unwrap_or(&args.dataspace)
        .to_string();
    let mut custom_data = std::collections::BTreeMap::new();
    for entry in &args.custom {
        let (key, value) = entry
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--custom takes KEY=VALUE, not `{entry}`"))?;
        custom_data.insert(key.to_string(), retp_client::custom_data_value(value));
    }
    let space = Dataspace {
        uri: uri.clone(),
        path,
        custom_data,
        ..Default::default()
    };
    let answers = put_dataspaces(session, std::slice::from_ref(&space))?;
    let mut complete = true;
    for (uri, answer) in &answers {
        match answer {
            Ok(said) => println!("created	{said}"),
            Err(why) => {
                eprintln!("refused	{uri}	{} ({})", why.message, why.code);
                complete = false;
            }
        }
    }
    Ok(complete)
}

fn delete_dataspace(session: &mut Session, dataspace: &str) -> Result<bool> {
    let uri = dataspace_uri(dataspace);
    let answers = delete_dataspaces(session, std::slice::from_ref(&uri))?;
    let mut complete = true;
    for (uri, answer) in &answers {
        match answer {
            Ok(said) => println!("deleted	{said}"),
            Err(why) => {
                eprintln!("refused	{uri}	{} ({})", why.message, why.code);
                complete = false;
            }
        }
    }
    Ok(complete)
}

/// An EPC into a dataspace: `energyml_etp_store::push`, with the package
/// as the source. The exit code says whether everything went.
fn push_epc(session: &mut Session, args: &PushArgs) -> Result<bool> {
    use energyml_etp_store::push::{
        Budget, Options, Progress as PushProgress, Stop, WellKnown, WriteFormat, push,
    };
    use energyml_storage::storage::EnergymlStorage;
    use retp_client::arrays::Grouping;
    let registry = std::sync::Arc::new(energyml_schemas::all());
    let store = energyml_storage::EpcStore::open(&args.epc, registry.clone())
        .with_context(|| format!("opening {}", args.epc.display()))?;
    // `--only` by uuid: matched against the package's own URIs.
    let only: Option<Vec<String>> = if args.only.is_empty() {
        None
    } else {
        let listed = retp_client::blocking(EnergymlStorage::resources(&store))?;
        let mut chosen = Vec::new();
        for wanted in &args.only {
            let lower = wanted.to_ascii_lowercase();
            match listed
                .iter()
                .find(|one| one.uri == *wanted || one.uuid.eq_ignore_ascii_case(&lower))
            {
                Some(found) => chosen.push(found.uri.clone()),
                None => anyhow::bail!("{wanted}: not in {}", args.epc.display()),
            }
        }
        Some(chosen)
    };
    let mut budget = Budget::default();
    if let Some(objects) = args.objects {
        budget.objects = objects;
    }
    if let Some(bytes) = args.bytes {
        budget.bytes = bytes;
    }
    let options = Options {
        budget,
        format: if args.json {
            WriteFormat::Json
        } else {
            WriteFormat::Xml
        },
        well_known: if args.well_known {
            WellKnown::Push(&registry)
        } else {
            WellKnown::Leave
        },
        only: only.as_deref(),
        grouping: if args.one_by_one {
            Grouping::OneByOne
        } else {
            Grouping::Together
        },
        // An EPC knows no `storeLastWrite`: nothing to compare the store
        // against, so nothing is checked - the front end, which took its objects
        // from the server, is the one that checks.
        expected: None,
        overwrite: false,
    };
    let report = retp_client::blocking(push(
        session,
        &store,
        &args.dataspace,
        &options,
        |event| match event {
            PushProgress::Planned { islands, objects } => {
                eprintln!("plan	{islands} island(s), {objects} object(s)")
            }
            PushProgress::Sent { island, of } => eprintln!("sent	island {}/{of}", island + 1),
        },
    ))?;
    for (rank, island) in report.islands.iter().enumerate() {
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
        println!(
            "island {}	{}	{} object(s), {} array(s), {} bytes",
            rank + 1,
            state,
            island.uris.len(),
            island.arrays,
            island.bytes
        );
        for uri in &island.uris {
            println!("	{uri}");
        }
    }
    if report.without_transactions {
        eprintln!(
            "warning: the session agreed no Transaction (18): each island went as plain puts, and a refused island may be partly there"
        );
    }
    for said in &report.left_out {
        eprintln!("left out: {said}");
    }
    for (from, to) in &report.dangling {
        eprintln!("leaves the package: {from} -> {to}");
    }
    if let Some(stopped) = &report.stopped {
        match &stopped.why {
            Stop::Refused(why) => eprintln!("stopped at island {}: refused: {why}", stopped.island + 1),
            Stop::Failed(why) => eprintln!(
                "stopped at island {}: {why} (the island's outcome is unknown)",
                stopped.island + 1
            ),
        }
    }
    println!("{} of {} island(s) committed", report.sent, report.islands.len());
    Ok(report.stopped.is_none() && report.left_out.is_empty())
}

/// Open a session, do one thing, close it: the exit code says whether the
/// thing was done in full.
fn one_shot(server: &Server, work: impl FnOnce(&mut Session) -> Result<bool>) -> Result<ExitCode> {
    let mut session = server.open()?;
    let complete = work(&mut session);
    said_acknowledgements(&mut session);
    let complete = complete?;
    session.close("done")?;
    Ok(if complete {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

/// Run one command of a shell in `session`. Reads only, so far: running one
/// again after a lost connection is harmless.
fn perform(session: &mut Session, command: &InSession, types: &[String]) -> Result<()> {
    match command {
        InSession::Info => info(session),
        InSession::Ping => ping(session)?,
        InSession::Dataspaces => {
            dataspaces(session)?;
        }
        InSession::Resources(args) => {
            resources(session, args, types)?;
        }
        InSession::Get(args) => {
            get(session, args)?;
        }
        InSession::SupportedTypes(args) => {
            supported_types(session, args)?;
        }
        InSession::Array(args) => {
            arrays(session, args)?;
        }
        InSession::Prefetch(args) => {
            prefetch(session, args)?;
        }
        InSession::DownloadEpc(args) => {
            download_epc(session, args)?;
        }
        InSession::CreateDataspace(args) => {
            create_dataspace(session, args)?;
        }
        InSession::DeleteDataspace { dataspace } => {
            delete_dataspace(session, dataspace)?;
        }
        InSession::Push(args) => {
            push_epc(session, args)?;
        }
        InSession::Watch(args) => watch(session, args)?,
        InSession::Unwatch { uuid } => unwatch(session, uuid)?,
        InSession::Notifications { wait } => said_notifications(session, *wait)?,
        InSession::ChangesSince(args) => changes_since_cmd(session, args)?,
        InSession::Channels { uris } => channels(session, uris)?,
        InSession::SubscribeChannels { ids, changes } => subscribe_channels_cmd(session, ids, *changes)?,
        InSession::UnsubscribeChannels { ids } => unsubscribe_channels_cmd(session, ids)?,
        InSession::ChannelData { wait } => said_channel_data(session, *wait)?,
        InSession::Streaming { state } => streaming(session, state == "on")?,
        InSession::Download(_) => unreachable!("handled by the shell: it may open new sessions"),
        InSession::Verbose { .. } | InSession::Ack { .. } | InSession::Reconnect | InSession::Exit => {
            unreachable!("handled by the shell")
        }
    }
    Ok(())
}

/// Whether a command may be run again on a new session after the connection
/// fell.
///
/// **Only what is idempotent**: a read asked twice costs a round
/// trip, and a write replayed writes twice. An interrupted write is of
/// *unknown* outcome - the store may have committed it and lost the answer -
/// so the shell reports it and the person decides, which is the one thing a
/// shell must not do on its own.
fn replays(command: &InSession) -> bool {
    !matches!(
        command,
        InSession::Push(_) | InSession::CreateDataspace(_) | InSession::DeleteDataspace { .. }
    )
}

/// Say what a session opened after a cut can still catch up on.
///
/// The watermark of the old session against what the new one says the server
/// keeps: either the time to ask changes from - printed as the argument
/// `changes-since` takes - or the news that it is too late and the dataspace
/// must be read again (§26.6.3).
fn said_catch_up(fresh: &Session, kept: Option<&retp_proto::continuity::Continuity>) {
    use retp_client::Caller;
    use retp_client::rest::iso;
    let Some(kept) = kept else {
        eprintln!("a new session is open");
        return;
    };
    match kept.window(Caller::watermark(fresh).unwrap_or(kept.watermark)) {
        Ok(since) => eprintln!(
            "a new session is open; what changed since {} can still be asked for - `changes-since <dataspace> {}`",
            iso(since),
            iso(since)
        ),
        Err(gone) => eprintln!(
            "a new session is open, and the catch-up window is gone: the server was last heard at {}, and it keeps changes from {} on - read the dataspace again rather than ask what changed",
            iso(gone.watermark),
            iso(gone.oldest_kept)
        ),
    }
}

/// Split a shell line into words: whitespace separates, a double-quoted run
/// is one word. **Single quotes are kept as they are**, because ETP's URIs
/// carry them - `eml:///dataspace('demo')` is one word, unquoted.
fn split_line(line: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut started = false;
    let mut quoted = false;
    for c in line.chars() {
        match c {
            '"' => {
                quoted = !quoted;
                started = true;
            }
            c if c.is_whitespace() && !quoted => {
                if started {
                    words.push(std::mem::take(&mut word));
                    started = false;
                }
            }
            c => {
                word.push(c);
                started = true;
            }
        }
    }
    if quoted {
        return Err("a double quote is not closed".into());
    }
    if started {
        words.push(word);
    }
    Ok(words)
}

/// Print what arrived unasked and matters to whoever is at the keyboard.
fn said_notices(session: &mut Session, interactive: bool) {
    use retp_proto::connection::Event;
    let mut said = false;
    for event in session.unsolicited() {
        let notice = match event {
            Event::AuthorizationExpiring => "the server says the authorization runs out soon".to_string(),
            Event::Fault(incident) => format!("the server sent what could not be taken: {incident}"),
            Event::TimedOut { request, incident } => format!("request #{request} timed out: {incident}"),
            Event::Closed { reason } => format!("the server closed the session: {reason}"),
            _ => continue,
        };
        if !said && interactive {
            eprintln!();
        }
        said = true;
        eprintln!("notice: {notice}");
    }
    if said && interactive {
        print!("etp> ");
        std::io::stdout().flush().ok();
    }
}

/// Print the acknowledgements the session heard.
fn said_acknowledgements(session: &mut Session) {
    for ack in session.acknowledgements() {
        eprintln!(
            "acknowledged: {} #{} after {} ms",
            ack.name,
            ack.message_id,
            ack.after.as_millis()
        );
    }
}

/// One session, many commands. A lost connection is said, a new session is
/// opened - login included - and the command runs again, once: every command
/// here is a read.
fn shell(server: &Server) -> Result<ExitCode> {
    // A shell may write: its offer says so from the start, so that a `push`
    // typed later does not find a session that announced reads only.
    let mut session = Some(server.open_for(true)?);
    // Survives a reconnection: the new session asks as the old one did.
    let acknowledging = AtomicBool::new(server.ack);
    let interactive = std::io::stdin().is_terminal();
    if interactive {
        info(session.as_mut().expect("just opened"));
        eprintln!(
            "commands: dataspaces, resources, get, supported-types, array, download, info, ping, ack, verbose, reconnect, exit \
             - `help <command>` for its options"
        );
    }
    // The keyboard is read on a thread of its own, so that the session is
    // listened to while nothing is typed: a WebSocket server whose pings go
    // unanswered closes the connection (a server may do so after twenty seconds),
    // and the keepalive Ping has to be sent by someone.
    let (typed, lines) = std::sync::mpsc::channel::<std::io::Result<String>>();
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            if typed.send(line).is_err() {
                break;
            }
        }
    });
    loop {
        if interactive {
            print!("etp> ");
            std::io::stdout().flush().ok();
        }
        let line = loop {
            match lines.recv_timeout(std::time::Duration::from_millis(10)) {
                Ok(line) => break Some(line),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break None,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    let Some(open) = session.as_mut() else { continue };
                    match open.idle(std::time::Duration::from_millis(250)) {
                        Ok(()) => said_notices(open, interactive),
                        Err(error) => {
                            eprintln!(
                                "\nthe connection fell while idle ({error}); the next command opens a new session"
                            );
                            session = None;
                            if interactive {
                                print!("etp> ");
                                std::io::stdout().flush().ok();
                            }
                        }
                    }
                }
            }
        };
        let Some(line) = line else { break };
        let line = line?;
        if line.trim_start().starts_with('#') {
            continue;
        }
        let words = match split_line(&line) {
            Ok(words) if words.is_empty() => continue,
            Ok(words) => words,
            Err(why) => {
                eprintln!("error: {why}");
                continue;
            }
        };
        let command = match Line::try_parse_from(&words) {
            Ok(line) => line.command,
            Err(error) => {
                // `help` lands here too, as clap's way of saying it.
                let _ = error.print();
                continue;
            }
        };
        match &command {
            InSession::Exit => break,
            InSession::Verbose { state } => {
                let on = state
                    .as_deref()
                    .map_or(!VERBOSE.load(Ordering::Relaxed), |s| s == "on");
                VERBOSE.store(on, Ordering::Relaxed);
                eprintln!("verbose {}", if on { "on" } else { "off" });
                continue;
            }
            InSession::Ack { state } => {
                let open = session.as_mut();
                let now = open.as_ref().is_some_and(|s| s.asks_acknowledge());
                let on = state.as_deref().map_or(!now, |s| s == "on");
                acknowledging.store(on, Ordering::Relaxed);
                if let Some(open) = open
                    && let Err(error) = open.ask_acknowledge(on)
                {
                    acknowledging.store(false, Ordering::Relaxed);
                    eprintln!("error: {error}");
                    continue;
                }
                eprintln!("acknowledgements {}", if on { "asked" } else { "not asked" });
                continue;
            }
            InSession::Reconnect => {
                if let Some(old) = session.take() {
                    let _ = old.close("retp shell: reconnecting");
                }
            }
            InSession::Download(args) => {
                // The download reconnects by itself, and leaves its last
                // session to the shell.
                if let Err(error) = download_to(server, &mut session, args) {
                    eprintln!("error: {error:#}");
                }
                if let Some(open) = session.as_mut() {
                    said_acknowledgements(open);
                }
                continue;
            }
            _ => {}
        }
        // What this session knows, kept **before** the command that may cost
        // it: once the socket is gone its watermark cannot be read any more.
        let kept = session.as_ref().and_then(retp_client::resume::kept_of);
        for attempt in 0..2 {
            if session.is_none() {
                match server.open_for(true) {
                    Ok(mut fresh) => {
                        said_catch_up(&fresh, kept.as_ref());
                        let _ = fresh.ask_acknowledge(acknowledging.load(Ordering::Relaxed));
                        session = Some(fresh);
                    }
                    Err(error) => {
                        eprintln!("error: {error:#}");
                        break;
                    }
                }
            }
            if matches!(command, InSession::Reconnect) {
                break;
            }
            let open = session.as_mut().expect("opened above");
            let done = perform(open, &command, &server.types);
            said_acknowledgements(open);
            match done {
                Ok(()) => break,
                Err(error) => {
                    eprintln!("error: {error:#}");
                    let lost = error
                        .downcast_ref::<ClientError>()
                        .is_some_and(ClientError::is_connection_lost);
                    if !lost {
                        break;
                    }
                    session = None;
                    if attempt == 0 {
                        if !replays(&command) {
                            eprintln!(
                                "the connection is lost during a write: its outcome is unknown, and it is not run again - read the dataspace and decide what to send"
                            );
                            break;
                        }
                        eprintln!("the connection is lost: opening a new session to run the command again");
                    }
                }
            }
        }
    }
    if let Some(open) = session {
        open.close("retp shell: done")?;
    }
    Ok(ExitCode::SUCCESS)
}

fn dataspaces(session: &mut Session) -> Result<bool> {
    let list = list_dataspaces(session)?;
    for dataspace in &list.items {
        println!("{}\t{}", dataspace.uri, dataspace.path);
    }
    Ok(counted(list.items.len(), list.ended_by))
}

/// `--active-status`.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum ActiveStatus {
    Active,
    Inactive,
}

impl ActiveStatus {
    fn kind(self) -> retp_types::datatypes::object::ActiveStatusKind {
        match self {
            ActiveStatus::Active => retp_types::datatypes::object::ActiveStatusKind::Active,
            ActiveStatus::Inactive => retp_types::datatypes::object::ActiveStatusKind::Inactive,
        }
    }
}

fn resources(session: &mut Session, args: &ResourcesArgs, types: &[String]) -> Result<bool> {
    let query = ResourceQuery {
        uri: dataspace_uri(&args.uri),
        depth: args.depth,
        scope: args.scope.kind(),
        data_object_types: if args.only.is_empty() {
            types.to_vec()
        } else {
            args.only.clone()
        },
        include_edges: args.edges,
        count_objects: args.count,
        active_status: args.active_status.map(ActiveStatus::kind),
    };
    let list = list_resources(session, &query)?;
    for resource in &list.resources {
        let count = |n: Option<i32>| n.map_or_else(|| "-".to_string(), |n| n.to_string());
        if args.count {
            println!(
                "{}\t{}\tsources {}\ttargets {}",
                resource.uri,
                resource.name,
                count(resource.source_count),
                count(resource.target_count)
            );
        } else {
            println!("{}\t{}", resource.uri, resource.name);
        }
    }
    for edge in &list.edges {
        println!(
            "edge\t{}\t→ {}\t{:?}",
            edge.source_uri, edge.target_uri, edge.relationship_kind
        );
    }
    if args.edges {
        eprintln!("{} edge(s)", list.edges.len());
    }
    Ok(counted(list.resources.len(), list.ended_by))
}

fn supported_types(session: &mut Session, args: &SupportedTypesArgs) -> Result<bool> {
    let list = list_supported_types(
        session,
        &dataspace_uri(&args.uri),
        args.scope.kind(),
        args.count,
        args.empty,
    )?;
    for supported in &list.items {
        match supported.object_count {
            Some(count) => println!("{}\t{count}", supported.data_object_type),
            None => println!("{}", supported.data_object_type),
        }
    }
    Ok(counted(list.items.len(), list.ended_by))
}

/// A dataspace into a directory, reconnecting when the connection is lost:
/// `session` is used if open, and the last one is left there.
fn download_to(server: &Server, session: &mut Option<Session>, args: &DownloadArgs) -> Result<bool> {
    let job = Download {
        dataspace: dataspace_uri(&args.dataspace),
        directory: args.out.clone(),
        data_object_types: args.only.clone(),
        format: args.format.name().to_string(),
        batch: args.batch,
        overwrite: args.overwrite,
    };
    let retry = Retry {
        max_retries: args.retries.max(1),
        ..Retry::default()
    };
    let terminal = std::io::stderr().is_terminal();
    let mut connector = Reopen(|| server.open());
    let outcome = download_resumable(&mut connector, session, &job, &retry, |event| match event {
        RunEvent::Progress(Progress::Listed {
            objects,
            already_there,
        }) => {
            eprintln!("{objects} object(s) listed, {already_there} already there");
        }
        RunEvent::Progress(Progress::Batch { done, total }) if terminal => {
            eprint!("\rdownloaded {done}/{total}");
            if done == total {
                eprintln!();
            }
        }
        RunEvent::Progress(Progress::Batch { done, total }) => eprintln!("downloaded {done}/{total}"),
        RunEvent::Setback(Setback::Reconnecting {
            step,
            attempt,
            max,
            pause,
            why,
        }) => eprintln!(
            "\n{step}: {why} - a new session in {} s (failure {attempt} of {max})",
            pause.as_secs()
        ),
        RunEvent::Setback(Setback::Halved { step }) => {
            eprintln!("asking for fewer objects at a time: {step}")
        }
        RunEvent::Setback(Setback::Reopened {
            resumed,
            refused,
            since,
            too_late,
        }) => {
            let how = if resumed {
                "the server took the lost session back"
            } else {
                "a new session, from the probe"
            };
            if let Some(why) = refused {
                eprintln!("the server would not take the session back ({why})");
            }
            match (since, too_late) {
                (_, true) => eprintln!(
                    "{how}; the catch-up window is gone - what was downloaded is still there, and what changed since cannot be listed"
                ),
                (Some(since), _) => eprintln!(
                    "{how}; changes since {} can still be asked for",
                    retp_client::rest::iso(since)
                ),
                (None, _) => eprintln!("{how}"),
            }
        }
    });
    let (report, stopped) = match outcome {
        Ok(report) => (report, None),
        Err(stopped) => (stopped.report.clone(), Some(stopped)),
    };
    for (uri, error) in &report.failed {
        eprintln!("{uri}\terror {}: {}", error.code, error.message);
    }
    for uri in &report.unnamed {
        eprintln!("{uri}\tnames no object: no file");
    }
    if let Some(error) = &report.listing_cut_short {
        eprintln!(
            "the listing was cut short: error {}: {}",
            error.code, error.message
        );
    }
    eprintln!(
        "{} written, {} already there, {} failed, into {}",
        report.written,
        report.already_there,
        report.failed.len(),
        args.out.display()
    );
    match stopped {
        Some(stopped) => Err(anyhow::anyhow!(
            "{} - run the same command again to go on from the files written",
            stopped.why
        )),
        None => Ok(report.complete()),
    }
}

/// A message size for a session where no end said one: DataArray's budget
/// falls back on it.
const DEFAULT_MESSAGE: u64 = 16 * 1024 * 1024;

fn arrays(session: &mut Session, args: &ArrayArgs) -> Result<bool> {
    let ids: Vec<DataArrayIdentifier> = args
        .paths
        .iter()
        .map(|path| DataArrayIdentifier {
            uri: args.uri.clone(),
            path_in_resource: path.clone(),
        })
        .collect();
    if args.metadata {
        let mut complete = true;
        for (path, answer) in args.paths.iter().zip(array_metadata(session, &ids)?) {
            match answer {
                Ok(meta) => println!(
                    "{path}\t{:?}\t{:?} as {:?}{}",
                    meta.dimensions,
                    meta.logical_array_type,
                    meta.transport_array_type,
                    if meta.preferred_subarray_dimensions.is_empty() {
                        String::new()
                    } else {
                        format!("\tpreferred blocks {:?}", meta.preferred_subarray_dimensions)
                    }
                ),
                Err(error) => {
                    complete = false;
                    eprintln!("{path}\terror {}: {}", error.code, error.message);
                }
            }
        }
        return Ok(complete);
    }
    eprintln!(
        "at most {} bytes of array a message",
        array_budget(&session.negotiated(), DEFAULT_MESSAGE)
    );
    let answers = read_arrays(session, &ids, DEFAULT_MESSAGE, |event| {
        if let ArrayProgress::Planned { path, blocks, bytes } = event {
            let size = bytes.map(|b| format!("{b} bytes, ")).unwrap_or_default();
            eprintln!(
                "{path}: {size}{}",
                if blocks == 1 {
                    "read whole".to_string()
                } else {
                    format!("{blocks} blocks")
                }
            );
        }
    })?;
    let mut complete = true;
    for (path, answer) in args.paths.iter().zip(answers) {
        match answer {
            Ok(array) => {
                println!(
                    "{path}\t{:?}\t{:?} as {:?}",
                    array.dimensions, array.logical, array.transport
                );
                match array.values.to_f64s(array.logical) {
                    Some(numbers) => {
                        let finite = numbers.iter().copied().filter(|v| v.is_finite());
                        let (min, max) = finite.fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), v| {
                            (lo.min(v), hi.max(v))
                        });
                        let first: Vec<String> =
                            numbers.iter().take(args.show).map(|v| v.to_string()).collect();
                        println!(
                            "  {} values, from {min} to {max}: {}{}",
                            numbers.len(),
                            first.join(" "),
                            if numbers.len() > args.show { " …" } else { "" }
                        );
                    }
                    None => {
                        if let ArrayValues::Strings(strings) = &array.values {
                            let first: Vec<&str> =
                                strings.iter().take(args.show).map(String::as_str).collect();
                            println!("  {} strings: {}", strings.len(), first.join(" | "));
                        }
                    }
                }
            }
            Err(failure) => {
                complete = false;
                eprintln!("{path}\t{failure}");
            }
        }
    }
    Ok(complete)
}

fn get(session: &mut Session, args: &GetArgs) -> Result<bool> {
    let answers = get_data_objects(session, &args.uris, args.format.name())?;
    let mut failed = 0;
    for (uri, answer) in &answers {
        match answer {
            Ok(object) => match &args.out {
                Some(dir) => {
                    std::fs::create_dir_all(dir)?;
                    let name = object_file_name(uri, &object.format)
                        .unwrap_or_else(|| format!("object.{}", object_extension(&object.format)));
                    let path = dir.join(name);
                    std::fs::write(&path, &object.data)?;
                    println!("{uri}\t{}", path.display());
                }
                None => println!("--- {uri}\n{}", String::from_utf8_lossy(&object.data)),
            },
            Err(error) => {
                failed += 1;
                eprintln!("{uri}\terror {}: {}", error.code, error.message);
            }
        }
    }
    Ok(failed == 0)
}

fn object_extension(format: &str) -> &str {
    match format {
        "json" => "json",
        _ => "xml",
    }
}

/// Say how many came, and whether the answer was cut short.
fn counted(count: usize, ended_by: Option<retp_types::datatypes::ErrorInfo>) -> bool {
    eprintln!("{count} item(s)");
    match ended_by {
        Some(error) => {
            eprintln!(
                "the answer was cut short: error {}: {}",
                error.code, error.message
            );
            false
        }
        None => true,
    }
}

fn probe(server: &Server) -> Result<ExitCode> {
    if server.rest {
        // No capabilities document over REST: what the server says of its
        // build, and that it answers.
        let mut session = server.open()?;
        let health = session.health().expect("a REST session")?;
        println!("{}", server.url);
        println!("server       {health}");
        println!("transport    REST - no session: each request is authorized on its own");
        return Ok(ExitCode::SUCCESS);
    }
    let (url, capabilities) = native::fetch_server_capabilities(&server.endpoint()?)?;
    println!("{url}");
    println!(
        "application  {} {}",
        capabilities.application_name, capabilities.application_version
    );
    let contact = &capabilities.contact_information;
    if !contact.organization_name.is_empty() || !contact.contact_email.is_empty() {
        println!(
            "contact      {} {}",
            contact.organization_name, contact.contact_email
        );
    }
    println!("encodings    {}", capabilities.supported_encodings.join(", "));
    println!("formats      {}", capabilities.supported_formats.join(", "));
    println!("compression  {}", capabilities.supported_compression.join(", "));
    println!("protocols");
    for p in &capabilities.supported_protocols {
        let v = &p.protocol_version;
        println!(
            "  {:>4}  {:<28} {}.{}.{}.{}  as {}",
            p.protocol,
            protocol_name(p.protocol).unwrap_or("(unknown to this build)"),
            v.major,
            v.minor,
            v.revision,
            v.patch,
            p.role
        );
    }
    println!("data objects {}", capabilities.supported_data_objects.len());
    for d in &capabilities.supported_data_objects {
        println!("  {}", d.qualified_type);
    }
    println!("endpoint capabilities");
    for (name, value) in &capabilities.endpoint_capabilities {
        println!("  {name} = {:?}", value.item);
    }
    Ok(ExitCode::SUCCESS)
}

/// What the session agreed.
fn info(session: &mut Session) {
    let negotiated = session.negotiated();
    if session.is_rest() {
        println!("session      none - REST: each request is authorized on its own");
    } else {
        println!("session      {}", uuid(&negotiated.session_id.0));
    }
    println!(
        "server       {} {}",
        negotiated.peer_application.0, negotiated.peer_application.1
    );
    println!("protocols");
    for (protocol, agreed) in &negotiated.protocols {
        println!(
            "  {:>4}  {:<28} this client is the {}",
            protocol,
            protocol_name(*protocol).unwrap_or("?"),
            agreed.role.as_str()
        );
    }
    if session.is_rest() {
        println!("data objects none agreed - REST negotiates nothing; objects of any type are read");
        println!("formats      {}", negotiated.formats.join(", "));
        return;
    }
    // What the server agreed to - not a filter: whatever type it sends is read.
    println!(
        "data objects {} agreed; objects of any type the server sends are read",
        negotiated.data_objects.len()
    );
    for d in &negotiated.data_objects {
        println!("  {}", d.qualified_type);
    }
    println!("formats      {}", negotiated.formats.join(", "));
    match negotiated.max_payload() {
        Some(bytes) => println!("max message  {bytes} bytes"),
        None => println!("max message  (none said)"),
    }
    if !negotiated.theirs.unknown().is_empty() {
        println!("ignored      {}", negotiated.theirs.unknown().join(", "));
    }
}

fn watch(session: &mut Session, args: &WatchArgs) -> Result<()> {
    use retp_client::notifications::{Watch, subscribe_notifications, uuid_text};
    let mut watch = Watch::of(args.uri.clone(), native::random_uuid());
    watch.depth = args.depth;
    watch.scope = args.scope.kind();
    watch.data_object_types = args.types.clone();
    watch.include_object_data = args.with_data;
    match subscribe_notifications(session, std::slice::from_ref(&watch))?.remove(0) {
        Ok(()) => println!("watch\t{}\t{}", uuid_text(&watch.request_uuid), args.uri),
        Err(error) => eprintln!("refused: {} ({})", error.message, error.code),
    }
    Ok(())
}

fn unwatch(session: &mut Session, uuid: &str) -> Result<()> {
    use retp_client::notifications::unsubscribe_notifications;
    let uuid = retp_client::rest::uuid_of(uuid)
        .ok_or_else(|| anyhow::anyhow!("`{uuid}` is not a uuid (8-4-4-4-12 hex)"))?;
    match unsubscribe_notifications(session, uuid)? {
        Ok(reason) => println!("ended\t{reason}"),
        Err(error) => eprintln!("refused: {} ({})", error.message, error.code),
    }
    Ok(())
}

/// Poll a stream for `wait` seconds - each poll gives the transport a
/// moment - and print what came, typed.
fn polled<T>(
    session: &mut Session,
    wait: u64,
    mut take: impl FnMut(&mut Session) -> Result<Vec<T>>,
    mut say: impl FnMut(&T),
) -> Result<usize> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait);
    let mut heard = 0;
    loop {
        for one in take(session)? {
            say(&one);
            heard += 1;
        }
        if std::time::Instant::now() >= deadline {
            return Ok(heard);
        }
    }
}

fn said_notifications(session: &mut Session, wait: u64) -> Result<()> {
    use retp_client::notifications::{Notification, notifications, uuid_text};
    let heard = polled(
        session,
        wait,
        |s| Ok(notifications(s)?),
        |one| match one {
            Notification::Changed { change, subscription } => println!(
                "changed\t{}\t{:?}\t{}\t{}",
                uuid_text(subscription),
                change.change_kind,
                retp_client::rest::iso(change.change_time),
                change.data_object.resource.uri
            ),
            Notification::Deleted {
                uri,
                change_time,
                subscription,
            } => {
                println!(
                    "deleted\t{}\t{}\t{uri}",
                    uuid_text(subscription),
                    retp_client::rest::iso(*change_time)
                )
            }
            Notification::AccessRevoked {
                uri,
                change_time,
                subscription,
            } => {
                println!(
                    "revoked\t{}\t{}\t{uri}",
                    uuid_text(subscription),
                    retp_client::rest::iso(*change_time)
                )
            }
            Notification::ActiveStatusChanged {
                resource,
                status,
                change_time,
                subscription,
            } => println!(
                "status\t{}\t{status:?}\t{}\t{}",
                uuid_text(subscription),
                retp_client::rest::iso(*change_time),
                resource.uri
            ),
            Notification::Ended { reason, subscription } => {
                println!("ended\t{}\t{reason}", uuid_text(subscription))
            }
            Notification::Unsolicited(kept) => {
                println!("kept\t{} subscription(s) the store still holds", kept.len())
            }
            Notification::Other(delivery) => println!("other\t{:?}", delivery.body),
            Notification::Session(event) => eprintln!("session: {event:?}"),
        },
    )?;
    eprintln!("{heard} notification(s)");
    Ok(())
}

fn changes_since_cmd(session: &mut Session, args: &ChangesArgs) -> Result<()> {
    use retp_client::notifications::changes_since;
    let since = if args.since == "watermark" {
        session.watermark().ok_or_else(|| {
            anyhow::anyhow!("no watermark yet: nothing has been heard from this server in this session")
        })?
    } else {
        retp_client::rest::micros_of(&args.since).ok_or_else(|| {
            anyhow::anyhow!(
                "`{}` is not an ISO 8601 UTC time (2026-09-17T10:00:00Z), nor `watermark`",
                args.since
            )
        })?
    };
    let changes = changes_since(session, &dataspace_uri(&args.dataspace), since, &args.types)?;
    for resource in &changes.written {
        println!(
            "written\t{}\t{}\t{}",
            retp_client::rest::iso(resource.store_last_write),
            resource.uri,
            resource.name
        );
    }
    for deleted in &changes.deleted {
        println!(
            "deleted\t{}\t{}",
            retp_client::rest::iso(deleted.deleted_time),
            deleted.uri
        );
    }
    eprintln!(
        "{} written, {} deleted since {}",
        changes.written.len(),
        changes.deleted.len(),
        retp_client::rest::iso(since)
    );
    Ok(())
}

fn channels(session: &mut Session, uris: &[String]) -> Result<()> {
    use retp_client::channels::channel_metadata;
    for (uri, answer) in uris.iter().zip(channel_metadata(session, uris)?) {
        match answer {
            Ok(record) => println!(
                "channel\t{}\t{}\t{}\t{:?}\t{}\t{}",
                record.id,
                record.channel_name,
                record.uom,
                record.data_kind,
                record
                    .indexes
                    .iter()
                    .map(|i| format!("{}:{:?}", i.name, i.index_kind))
                    .collect::<Vec<_>>()
                    .join(","),
                record.uri
            ),
            Err(error) => eprintln!("{uri}: {} ({})", error.message, error.code),
        }
    }
    Ok(())
}

fn subscribe_channels_cmd(session: &mut Session, ids: &[i64], changes: bool) -> Result<()> {
    use retp_client::channels::subscribe_channels;
    use retp_types::datatypes::channel_data::ChannelSubscribeInfo;
    let asked: Vec<ChannelSubscribeInfo> = ids
        .iter()
        .map(|id| ChannelSubscribeInfo {
            channel_id: *id,
            data_changes: changes,
            ..Default::default()
        })
        .collect();
    for (id, answer) in ids.iter().zip(subscribe_channels(session, &asked)?) {
        match answer {
            Ok(()) => println!("subscribed\t{id}"),
            Err(error) => eprintln!("{id}: {} ({})", error.message, error.code),
        }
    }
    Ok(())
}

fn unsubscribe_channels_cmd(session: &mut Session, ids: &[i64]) -> Result<()> {
    use retp_client::channels::unsubscribe_channels;
    for (id, answer) in ids.iter().zip(unsubscribe_channels(session, ids)?) {
        match answer {
            Ok(()) => println!("unsubscribed\t{id}"),
            Err(error) => eprintln!("{id}: {} ({})", error.message, error.code),
        }
    }
    Ok(())
}

fn said_channel_data(session: &mut Session, wait: u64) -> Result<()> {
    use retp_client::channels::{CHANNEL_STREAMING, CHANNEL_SUBSCRIBE, ChannelEvent, channel_events};
    let heard = polled(
        session,
        wait,
        |s| {
            let mut all = channel_events(s, CHANNEL_SUBSCRIBE)?;
            all.extend(channel_events(s, CHANNEL_STREAMING)?);
            Ok(all)
        },
        |one| match one {
            ChannelEvent::Data(items) => {
                for item in items {
                    println!(
                        "data\t{}\t{}\t{:?}",
                        item.channel_id,
                        item.indexes
                            .iter()
                            .map(|i| format!("{:?}", i.item))
                            .collect::<Vec<_>>()
                            .join(","),
                        item.value.item
                    );
                }
            }
            ChannelEvent::Metadata(channels) => {
                for record in channels {
                    println!(
                        "channel\t{}\t{}\t{}\t{:?}\t{}",
                        record.id, record.channel_name, record.uom, record.data_kind, record.uri
                    );
                }
            }
            ChannelEvent::RangeReplaced {
                channel_ids, data, ..
            } => println!("replaced\t{channel_ids:?}\t{} item(s)", data.len()),
            ChannelEvent::Truncated { channels, .. } => println!("truncated\t{} channel(s)", channels.len()),
            ChannelEvent::Stopped { reason, channel_ids } => println!("stopped\t{channel_ids:?}\t{reason}"),
            ChannelEvent::Other(delivery) => println!("other\t{:?}", delivery.body),
            ChannelEvent::Session(event) => eprintln!("session: {event:?}"),
        },
    )?;
    eprintln!("{heard} event(s)");
    Ok(())
}

fn streaming(session: &mut Session, on: bool) -> Result<()> {
    use retp_client::channels::{start_streaming, stop_streaming};
    if on {
        start_streaming(session)?;
        eprintln!("streaming asked: read what comes with `channel-data --wait N`");
    } else {
        stop_streaming(session)?;
        eprintln!("streaming stopped");
    }
    Ok(())
}

fn prefetch(session: &mut Session, args: &PrefetchArgs) -> Result<bool> {
    let got = session.prefetch(&args.uris, !args.no_arrays)?;
    for (uri, object) in &got.objects {
        let asked = if args.uris.contains(uri) {
            "asked"
        } else {
            "referenced"
        };
        println!("object\t{uri}\t{asked}\t{} bytes", object.len());
    }
    for (id, meta) in &got.arrays {
        println!(
            "array\t{}\t{}\t{:?}\t{:?} as {:?}",
            id.uri, id.path_in_resource, meta.dimensions, meta.logical_array_type, meta.transport_array_type
        );
    }
    for (uri, xml) in &got.without_json {
        println!("object\t{uri}\tasked, as XML only\t{} bytes", xml.len());
        eprintln!(
            "no JSON: {uri}: the server holds it but gives no JSON for it, so neither what it references nor its arrays came"
        );
    }
    for uri in &got.missing {
        eprintln!("not found: {uri}");
    }
    eprintln!(
        "{} object(s), {} array(s), {} without JSON, {} missing",
        got.objects.len(),
        got.arrays.len(),
        got.without_json.len(),
        got.missing.len()
    );
    Ok(got.missing.is_empty() && got.without_json.is_empty())
}

/// A dataspace as an EPC. The objects go into the archive; the arrays go
/// into the HDF5 files the package names, which only a build with `hdf5` can
/// write - said, never silently left out.
fn download_epc(session: &mut Session, args: &PackArgs) -> Result<bool> {
    let wanted = energyml_etp_store::pack::Wanted {
        types: args.only.clone(),
        uris: Vec::new(),
        arrays: !args.no_arrays,
        references: args.with_references,
    };
    let registry = energyml_schemas::all();
    let packed = retp_client::blocking(energyml_etp_store::pack::pack(
        session,
        &args.dataspace,
        &registry,
        &wanted,
    ))?;
    std::fs::write(&args.out, &packed.epc).with_context(|| format!("writing {}", args.out.display()))?;
    println!(
        "{}\t{} object(s)\t{} bytes",
        args.out.display(),
        packed.objects,
        packed.epc.len()
    );
    let mut written = 0;
    for array in &packed.arrays {
        let file = args
            .out
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .join(&array.file);
        write_array(&file, array).with_context(|| format!("writing {} in {}", array.path, file.display()))?;
        written += 1;
    }
    if written > 0 {
        println!("{written} array(s) written");
    }
    for said in &packed.left_out {
        eprintln!("left out: {said}");
    }
    Ok(packed.left_out.is_empty())
}

/// One array into its HDF5 file - with the `hdf5` feature; without it, the
/// arrays cannot be written here and the command says so rather than leaving
/// a package that quietly names files nobody wrote.
#[cfg(feature = "hdf5")]
fn write_array(file: &std::path::Path, array: &energyml_etp_store::pack::PackedArray) -> Result<()> {
    use energyml_storage::array::{ArrayData, ArrayMetadata};
    use energyml_storage::backend::ArrayBackend;
    let data = ArrayData {
        metadata: ArrayMetadata {
            dimensions: array.dimensions.iter().map(|d| *d as i64).collect(),
            element_type: array.values.element_type().to_string(),
        },
        values: array.values.clone(),
    };
    energyml_storage::hdf5::Hdf5Backend
        .write(file, &array.path, &data)
        .map_err(|why| anyhow::anyhow!("{why}"))
}

#[cfg(not(feature = "hdf5"))]
fn write_array(file: &std::path::Path, array: &energyml_etp_store::pack::PackedArray) -> Result<()> {
    anyhow::bail!(
        "this build cannot write HDF5, so {} of {} stays unwritten: build with `--features hdf5`, or ask for `--no-arrays`",
        array.path,
        file.display()
    )
}

fn ping(session: &mut Session) -> Result<()> {
    let rtt = session.ping()?;
    println!("ping         {} ms", rtt.as_millis());
    Ok(())
}

fn uuid(bytes: &[u8; 16]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::{EtpCommand, Line, split_line};
    use clap::{CommandFactory, Parser};

    /// Every command builds: no two arguments of one command share a name
    /// (`--scope` was both the token's and ETP's, and `resources` panicked).
    /// clap checks a command only when it is built - this builds them all.
    #[test]
    fn every_command_and_shell_line_is_well_formed() {
        #[derive(Parser)]
        struct Etp {
            #[command(subcommand)]
            command: EtpCommand,
        }
        Etp::command().debug_assert();
        Line::command().debug_assert();
    }

    /// ETP's URIs carry single quotes: they stay, unquoted; double quotes
    /// group, and go.
    #[test]
    fn a_shell_line_keeps_the_quotes_of_an_etp_uri() {
        assert_eq!(
            split_line("resources --uri eml:///dataspace('demo') --depth 2").unwrap(),
            ["resources", "--uri", "eml:///dataspace('demo')", "--depth", "2"]
        );
        assert_eq!(
            split_line(r#"get "eml:///dataspace('a b')/resqml20.obj_X(1)"  --out "my dir""#).unwrap(),
            [
                "get",
                "eml:///dataspace('a b')/resqml20.obj_X(1)",
                "--out",
                "my dir"
            ]
        );
        assert_eq!(split_line("  ").unwrap(), Vec::<String>::new());
        assert_eq!(split_line(r#"get """#).unwrap(), ["get", ""]);
        assert!(split_line(r#"get "eml:///"#).is_err());
    }
}
