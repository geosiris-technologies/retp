//! `RequestSession` → `OpenSession`: what two endpoints agree to use
//! (§5.2.1.1 steps 1 and 9, §5.2.2 rows 5-6).
//!
//! Pure functions - no socket, no clock of their own: the time comes in as an
//! argument, in microseconds since the Unix epoch, UTC, which is what ETP's
//! `currentDateTime` fields are (§3.12.5).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use retp_avro::Role;
use retp_types::datatypes::{
    Contact, ServerCapabilities, SupportedDataObject, SupportedProtocol, Uuid, Version,
};
use retp_types::protocol::core::{OpenSession, RequestSession};

use crate::capabilities::{Capabilities, Scope, names, smaller_of_two};
pub use crate::wire::Tolerance;
use crate::wire::{Config, EtpError, Incident};

/// One protocol an endpoint implements - **entirely**, as §3.1.1.2 §3 requires
/// of any protocol it announces.
#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolOffer {
    /// The protocol number.
    pub protocol: i32,
    /// Its version - `1.2.0.0` for every protocol of ETP v1.2.
    pub version: Version,
    /// The role **this** endpoint plays in it.
    pub role: Role,
    /// This endpoint's protocol capabilities.
    pub capabilities: Capabilities,
}

impl ProtocolOffer {
    /// Protocol `protocol` of ETP v1.2, played as `role`, with no capability
    /// set beyond the defaults.
    pub fn v12(protocol: i32, role: Role) -> Self {
        Self {
            protocol,
            version: v12(),
            role,
            capabilities: Capabilities::empty(Scope::Protocol),
        }
    }
}

/// The version every protocol of ETP v1.2 carries.
pub fn v12() -> Version {
    Version {
        major: 1,
        minor: 2,
        revision: 0,
        patch: 0,
    }
}

/// Why an offer could not be built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OfferRefusal {
    /// Core is never listed (§5.2.1.1 §1.b.iv).
    CoreListed,
    /// The same protocol twice.
    Twice(i32),
    /// `client` or `server` is not a role in a sub-protocol (ADR-0001).
    ConnectionRole(Role),
    /// `store` in one protocol and `customer` in another - or `producer` and
    /// `consumer` (§3.1.2.1, §5.2.1.1 §1.b.iv).
    Inconsistent(Role, Role),
    /// A data object type that is not a qualified type (§23.20) - the
    /// sentence that says so.
    NotAQualifiedType(String),
    /// A format ETP does not have, or none at all.
    Format(String),
    /// A capability this offer cannot announce.
    Capability {
        /// Which.
        name: String,
        /// Why.
        why: String,
    },
}

impl fmt::Display for OfferRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OfferRefusal::CoreListed => write!(
                f,
                "Core (protocol 0) is never listed among the protocols of a session"
            ),
            OfferRefusal::Twice(p) => write!(f, "protocol {p} is offered twice"),
            OfferRefusal::ConnectionRole(r) => {
                write!(f, "`{}` is a connection role, not a protocol role", r.as_str())
            }
            OfferRefusal::NotAQualifiedType(why) => write!(f, "{why}"),
            OfferRefusal::Format(why) => write!(f, "format {why}"),
            OfferRefusal::Capability { name, why } => write!(f, "{name}: {why}"),
            OfferRefusal::Inconsistent(a, b) => write!(
                f,
                "an endpoint is not `{}` in one protocol and `{}` in another in one session (§3.1.2.1)",
                a.as_str(),
                b.as_str()
            ),
        }
    }
}

impl std::error::Error for OfferRefusal {}

/// What the peer said that the specification forbids, and what this end made
/// of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deviation {
    /// What the peer did.
    pub what: String,
    /// The rule it breaks.
    pub rule: &'static str,
    /// What this end did about it.
    pub taken_as: String,
}

impl fmt::Display for Deviation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}); {}", self.what, self.rule, self.taken_as)
    }
}

pub(crate) fn deviation(what: String, rule: &'static str, taken_as: impl Into<String>) -> Deviation {
    Deviation {
        what,
        rule,
        taken_as: taken_as.into(),
    }
}

/// `Store (protocol 4)` - a protocol as a person reads it in a message.
fn named(protocol: i32) -> String {
    match protocol_name(protocol) {
        Some(name) => format!("{name} (protocol {protocol})"),
        None => format!("protocol {protocol}"),
    }
}

fn dotted(v: &Version) -> String {
    format!("{}.{}.{}.{}", v.major, v.minor, v.revision, v.patch)
}

/// What a `qualifiedType` in a negotiation says: `resqml20.*` or
/// `resqml20.obj_X` as they should be, `resqml20` read as the family it can
/// only mean, or nothing readable.
pub(crate) enum QualifiedType {
    Valid,
    Family(String),
    Unreadable,
}

pub(crate) fn read_qualified_type(text: &str) -> QualifiedType {
    let family = |t: &str| {
        let digits = t.trim_start_matches(|c: char| c.is_ascii_lowercase());
        digits.len() >= 2 && digits.len() < t.len() && digits.chars().all(|c| c.is_ascii_digit())
    };
    match text.split_once('.') {
        Some((head, tail)) if family(head) => {
            let name = tail == "*"
                || (!tail.is_empty() && tail.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
            if name {
                QualifiedType::Valid
            } else {
                QualifiedType::Unreadable
            }
        }
        None if family(text) => QualifiedType::Family(format!("{text}.*")),
        _ => QualifiedType::Unreadable,
    }
}

/// Two roles one endpoint may not hold in one session.
fn clash(a: Role, b: Role) -> bool {
    matches!(
        (a, b),
        (Role::Store, Role::Customer)
            | (Role::Customer, Role::Store)
            | (Role::Producer, Role::Consumer)
            | (Role::Consumer, Role::Producer)
    )
}

/// The counterpart in a sub-protocol's pair.
pub(crate) fn counterpart(role: Role) -> Role {
    match role {
        Role::Store => Role::Customer,
        Role::Customer => Role::Store,
        Role::Producer => Role::Consumer,
        Role::Consumer => Role::Producer,
        Role::Client => Role::Server,
        Role::Server => Role::Client,
    }
}

/// A protocol's name, as its schemas' namespace spells it - `Store`,
/// `Discovery`: read off the messages the generator knows, never a list kept
/// by hand.
pub fn protocol_name(protocol: i32) -> Option<&'static str> {
    (1..=32)
        .chain([1000, 1001])
        .find_map(|message_type| retp_types::dispatch::info(protocol, message_type))
        .and_then(|info| info.name.rsplit('.').nth(1))
}

/// A role as `SupportedProtocol.role` spells it.
pub fn parse_role(text: &str) -> Option<Role> {
    Some(match text {
        "store" => Role::Store,
        "customer" => Role::Customer,
        "producer" => Role::Producer,
        "consumer" => Role::Consumer,
        _ => return None,
    })
}

/// What one endpoint brings to the negotiation.
#[derive(Debug, Clone, PartialEq)]
pub struct Offer {
    /// Its name, which vendors are encouraged to prefix with theirs.
    pub application_name: String,
    /// Its version.
    pub application_version: String,
    /// A UUID for this instance (`clientInstanceId` / `serverInstanceId`).
    pub instance_id: Uuid,
    protocols: Vec<ProtocolOffer>,
    /// The data object types it supports, `resqml20.*` style wildcards allowed.
    pub data_objects: Vec<SupportedDataObject>,
    /// The formats of data objects it reads and writes (`xml`, `json`).
    pub formats: Vec<String>,
    /// Its endpoint capabilities.
    pub endpoint: Capabilities,
    /// How it takes its peer's faults in the negotiation.
    pub tolerance: Tolerance,
}

impl Offer {
    /// An offer with no protocol yet, `xml` for format, and the endpoint
    /// capabilities the wire enforces set to what it enforces.
    pub fn new(
        application_name: impl Into<String>,
        application_version: impl Into<String>,
        instance_id: Uuid,
    ) -> Self {
        let endpoint = Capabilities::empty(Scope::Endpoint)
            // Announced so that the peer knows the gap this end will wait
            // between two parts of a multipart message: the specification
            // gives a maximum and no default.
            .set(
                names::MULTIPART_MESSAGE_TIMEOUT_PERIOD,
                crate::capabilities::Value::Long(60),
            )
            .expect("60 s is within MultipartMessageTimeoutPeriod's bounds");
        Self {
            application_name: application_name.into(),
            application_version: application_version.into(),
            instance_id,
            protocols: Vec::new(),
            data_objects: Vec::new(),
            formats: vec!["xml".to_string()],
            endpoint,
            tolerance: Tolerance::default(),
        }
    }

    /// Add a protocol.
    pub fn protocol(mut self, offer: ProtocolOffer) -> Result<Self, OfferRefusal> {
        if offer.protocol == 0 {
            return Err(OfferRefusal::CoreListed);
        }
        if matches!(offer.role, Role::Client | Role::Server) {
            return Err(OfferRefusal::ConnectionRole(offer.role));
        }
        if self.protocols.iter().any(|p| p.protocol == offer.protocol) {
            return Err(OfferRefusal::Twice(offer.protocol));
        }
        if let Some(other) = self.protocols.iter().find(|p| clash(p.role, offer.role)) {
            return Err(OfferRefusal::Inconsistent(other.role, offer.role));
        }
        self.protocols.push(offer);
        Ok(self)
    }

    /// The same offer, with no protocol: what a connection rebuilds from its
    /// handlers, so that nothing is announced that nothing implements.
    pub fn without_protocols(&self) -> Offer {
        Offer {
            protocols: Vec::new(),
            ..self.clone()
        }
    }

    /// The protocols offered.
    pub fn protocols(&self) -> &[ProtocolOffer] {
        &self.protocols
    }

    /// The `RequestSession` a client sends: each protocol with the role it
    /// wants the **server** to take - the counterpart of its own.
    pub fn request_session(&self, now: i64, earliest_retained_change_time: i64) -> RequestSession {
        RequestSession {
            application_name: self.application_name.clone(),
            application_version: self.application_version.clone(),
            client_instance_id: self.instance_id,
            requested_protocols: self
                .protocols
                .iter()
                .map(|p| SupportedProtocol {
                    protocol: p.protocol,
                    protocol_version: p.version.clone(),
                    role: counterpart(p.role).as_str().to_string(),
                    protocol_capabilities: p.capabilities.to_map(),
                })
                .collect(),
            supported_data_objects: self.data_objects.clone(),
            supported_compression: Vec::new(),
            supported_formats: self.formats.clone(),
            current_date_time: now,
            earliest_retained_change_time,
            server_authorization_required: false,
            endpoint_capabilities: self.endpoint.to_map(),
        }
    }

    /// The `ServerCapabilities` a server publishes at its
    /// `/.well-known/etp-server-capabilities` (§4.3.1): what this offer says,
    /// each protocol with the role **this** end plays. The counterpart of
    /// [`crate::probe::parse_server_capabilities`], which reads it back.
    ///
    /// `supportedEncodings` is `binary` alone (the JSON encoding "is for internal
    /// testing and debugging only", §4.3, and is not implemented here), and no
    /// compression is offered (none is implemented, as in [`Offer::request_session`]).
    /// The contact is empty: an offer does not hold one.
    pub fn server_capabilities(&self) -> ServerCapabilities {
        ServerCapabilities {
            application_name: self.application_name.clone(),
            application_version: self.application_version.clone(),
            contact_information: Contact::default(),
            supported_compression: Vec::new(),
            supported_encodings: vec!["binary".to_string()],
            supported_formats: self.formats.clone(),
            supported_data_objects: self.data_objects.clone(),
            supported_protocols: self
                .protocols
                .iter()
                .map(|p| SupportedProtocol {
                    protocol: p.protocol,
                    protocol_version: p.version.clone(),
                    role: p.role.as_str().to_string(),
                    protocol_capabilities: p.capabilities.to_map(),
                })
                .collect(),
            endpoint_capabilities: self.endpoint.to_map(),
        }
    }
}

/// One protocol both ends agreed on.
#[derive(Debug, Clone, PartialEq)]
pub struct NegotiatedProtocol {
    /// Its version.
    pub version: Version,
    /// The role this end plays.
    pub role: Role,
    /// This end's protocol capabilities.
    pub ours: Capabilities,
    /// The other end's.
    pub theirs: Capabilities,
}

/// What a session was opened with.
#[derive(Debug, Clone, PartialEq)]
pub struct Negotiated {
    /// The session's id - for debugging only: ETP has no session
    /// survivability, and nothing reuses it (§5.2.2 row 2.4-2.5).
    pub session_id: Uuid,
    /// The peer's `applicationName` and `applicationVersion`.
    pub peer_application: (String, String),
    /// The protocols agreed, by number.
    pub protocols: BTreeMap<i32, NegotiatedProtocol>,
    /// The data object types agreed.
    pub data_objects: Vec<SupportedDataObject>,
    /// The formats agreed.
    pub formats: Vec<String>,
    /// The compression agreed, if any.
    pub compression: Option<String>,
    /// This end's endpoint capabilities.
    pub ours: Capabilities,
    /// The peer's, as read - unknown ones ignored, problems named.
    pub theirs: Capabilities,
    /// The peer's `currentDateTime` at the opening.
    pub peer_time: i64,
    /// The peer's `earliestRetainedChangeTime`.
    pub peer_earliest_retained: i64,
    /// What the peer said that the specification forbids, taken as meant
    /// ([`Tolerance::Tolerant`]) - named here, and in the log, so it can be
    /// reported to whoever maintains the peer. A fault that would make the
    /// two ends disagree is never here: it refuses the session.
    pub deviations: Vec<Deviation>,
}

impl Negotiated {
    /// `MaxWebSocketMessagePayloadSize` for the session: the smaller of the
    /// two ends' (§3.3.2.9, §4.3 step 4).
    pub fn max_payload(&self) -> Option<i64> {
        smaller_of_two(
            self.ours.long(names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE),
            self.theirs.long(names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE),
        )
    }

    /// Whether header extensions may be used: both ends must say so (§3.6.2 §1).
    pub fn header_extensions(&self) -> bool {
        self.ours.boolean(names::SUPPORTS_MESSAGE_HEADER_EXTENSIONS) == Some(true)
            && self.theirs.boolean(names::SUPPORTS_MESSAGE_HEADER_EXTENSIONS) == Some(true)
    }

    /// Whether this end may send alternate URIs: the **peer** must accept them
    /// (§3.7.4.1 §1.b).
    pub fn peer_accepts_alternate_uris(&self) -> bool {
        self.theirs.boolean(names::SUPPORTS_ALTERNATE_REQUEST_URIS) == Some(true)
    }

    /// The wire's configuration for this session.
    pub fn wire_config(&self) -> Config {
        let mut config = Config::new().header_extensions(self.header_extensions());
        for (protocol, agreed) in &self.protocols {
            if *protocol >= crate::wire::FIRST_PRIVATE_PROTOCOL {
                config = config.private_protocol(*protocol).expect("checked above");
            } else {
                config = config
                    .protocol(*protocol, agreed.role)
                    .expect("a session role, checked by the offer");
            }
        }
        if let Some(limit) = self.max_payload().and_then(|l| usize::try_from(l).ok()) {
            config = config.max_payload(limit);
        }
        config
    }
}

/// `resqml20.*` matches `resqml20.obj_X`, and every type matches itself.
pub(crate) fn covers(pattern: &str, qualified_type: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => qualified_type.starts_with(prefix),
        None => pattern == qualified_type,
    }
}

/// The data object types both ends support, the server's capabilities kept
/// (§5.2.2 row 6.1): the more specific of two matching entries.
fn common_data_objects(
    requested: &[SupportedDataObject],
    supported: &[SupportedDataObject],
) -> Vec<SupportedDataObject> {
    let mut out: Vec<SupportedDataObject> = Vec::new();
    let mut seen = BTreeSet::new();
    for r in requested {
        for s in supported {
            let specific = if covers(&s.qualified_type, &r.qualified_type) {
                &r.qualified_type
            } else if covers(&r.qualified_type, &s.qualified_type) {
                &s.qualified_type
            } else {
                continue;
            };
            if seen.insert(specific.clone()) {
                out.push(SupportedDataObject {
                    qualified_type: specific.clone(),
                    data_object_capabilities: s.data_object_capabilities.clone(),
                });
            }
        }
    }
    out
}

/// The server's side: answer a `RequestSession` with an `OpenSession`, or with
/// the error §5.2.1.1 names.
pub fn open_session(
    server: &Offer,
    request: &RequestSession,
    session_id: Uuid,
    now: i64,
    earliest_retained_change_time: i64,
) -> Result<(OpenSession, Negotiated), Incident> {
    let strict = server.tolerance == Tolerance::Strict;
    let mut deviations = Vec::new();
    let mut requested_roles = Vec::new();
    for asked in &request.requested_protocols {
        if asked.protocol == 0 {
            if strict {
                return Err(EtpError::EINVALID_ARGUMENT
                    .with_detail("Core (protocol 0) is listed among the requested protocols"));
            }
            deviations.push(deviation(
                "the RequestSession lists Core (protocol 0)".into(),
                "§5.2.1.1 step 1.b.iv",
                "the entry is ignored: Core is always spoken",
            ));
            continue;
        }
        let Some(role) = parse_role(&asked.role) else {
            if strict {
                return Err(EtpError::EINVALID_ARGUMENT.with_detail(format!(
                    "`{}` is not a role of {}",
                    asked.role,
                    named(asked.protocol)
                )));
            }
            // Not a role of any sub-protocol - `server`, a typo: it cannot
            // ask for the role this server does not play, so it asks for the
            // one it plays.
            continue;
        };
        if let Some(other) = requested_roles.iter().find(|r| clash(**r, role)) {
            return Err(EtpError::EINVALID_OPERATION.with_detail(format!(
                "the server is asked to be `{}` in one protocol and `{}` in another (§5.2.1.1 §1.b.iv)",
                other.as_str(),
                role.as_str()
            )));
        }
        requested_roles.push(role);
    }

    let mut agreed = Vec::new();
    let mut role_mismatch = false;
    for asked in request.requested_protocols.iter().filter(|p| p.protocol != 0) {
        let Some(ours) = server
            .protocols
            .iter()
            .find(|p| p.protocol == asked.protocol && p.version == asked.protocol_version)
        else {
            continue;
        };
        if ours.role.as_str() != asked.role {
            if parse_role(&asked.role).is_some() {
                role_mismatch = true;
                continue;
            }
            deviations.push(deviation(
                format!(
                    "the RequestSession asks for role `{}` in {}",
                    asked.role,
                    named(asked.protocol)
                ),
                "§5.2.1.1 step 1.b.iv",
                format!(
                    "taken as `{}`, the role this server plays there",
                    ours.role.as_str()
                ),
            ));
        }
        agreed.push((ours, asked));
    }
    if agreed.is_empty() {
        return Err(if role_mismatch {
            EtpError::ENOROLE.with_detail("no requested protocol is served in the role requested")
        } else {
            EtpError::ENOSUPPORTEDPROTOCOLS
                .with_detail("no requested protocol, at the version requested, is served here")
        });
    }
    let data_objects = common_data_objects(&request.supported_data_objects, &server.data_objects);
    if data_objects.is_empty() {
        return Err(EtpError::ENOSUPPORTEDDATAOBJECTTYPES
            .with_detail("no requested data object type is supported here"));
    }
    let formats: Vec<String> = request
        .supported_formats
        .iter()
        .filter(|f| server.formats.contains(f))
        .cloned()
        .collect();
    if formats.is_empty() {
        return Err(EtpError::ENOSUPPORTEDFORMATS.with_detail("no requested format is supported here"));
    }

    let open = OpenSession {
        application_name: server.application_name.clone(),
        application_version: server.application_version.clone(),
        server_instance_id: server.instance_id,
        supported_protocols: agreed
            .iter()
            .map(|(ours, _)| SupportedProtocol {
                protocol: ours.protocol,
                protocol_version: ours.version.clone(),
                role: ours.role.as_str().to_string(),
                protocol_capabilities: ours.capabilities.to_map(),
            })
            .collect(),
        supported_data_objects: data_objects.clone(),
        // No compression is implemented yet: none is agreed.
        supported_compression: String::new(),
        supported_formats: formats.clone(),
        current_date_time: now,
        earliest_retained_change_time,
        session_id,
        endpoint_capabilities: server.endpoint.to_map(),
    };
    let negotiated = Negotiated {
        session_id,
        peer_application: (
            request.application_name.clone(),
            request.application_version.clone(),
        ),
        protocols: agreed
            .iter()
            .map(|(ours, asked)| {
                (
                    ours.protocol,
                    NegotiatedProtocol {
                        version: ours.version.clone(),
                        role: ours.role,
                        ours: ours.capabilities.clone(),
                        theirs: Capabilities::read(&asked.protocol_capabilities, Scope::Protocol),
                    },
                )
            })
            .collect(),
        data_objects,
        formats,
        compression: None,
        ours: server.endpoint.clone(),
        theirs: Capabilities::read(&request.endpoint_capabilities, Scope::Endpoint),
        peer_time: request.current_date_time,
        peer_earliest_retained: request.earliest_retained_change_time,
        deviations,
    };
    Ok((open, negotiated))
}

/// The client's side: check an `OpenSession` against what was asked.
///
/// The server "MUST NOT change the version of a requested protocol", "MUST
/// NOT offer to support any additional protocols", "MUST NOT change the
/// requested role" (§5.2.2 row 5.2). What it gets wrong is sorted by
/// [`Tolerance`]:
///
/// - **taken as meant, and named**: Core listed, a protocol not requested
///   (not agreed), a revision or patch that differs, a role that is not the
///   one this client plays (read as the one requested), a compression not
///   offered (none used), a format not offered (dropped), a `qualifiedType`
///   without its `.*` (read with it) or unreadable (dropped), a data object
///   type not requested (agreed: this client reads whatever it is sent);
/// - **refused, every fault named at once**, with `EREQUEST_DENIED` and
///   `CloseSession` (§5.2.1.1 §10.b): the server claims the role this client
///   plays, a protocol in another major or minor version, no protocol or no
///   format left in common.
///
/// Under [`Tolerance::Strict`] every deviation is a fault.
pub fn accept_open_session(
    client: &Offer,
    request: &RequestSession,
    open: &OpenSession,
) -> Result<Negotiated, Incident> {
    let denied = |why: String| EtpError::EREQUEST_DENIED.with_detail(why);
    let mut protocols = BTreeMap::new();
    let mut deviations = Vec::new();
    let mut faults = Vec::new();
    for given in &open.supported_protocols {
        if given.protocol == 0 {
            deviations.push(deviation(
                "the OpenSession lists Core (protocol 0)".into(),
                "§5.2.1.1 step 9.b.iv",
                "the entry is ignored: Core is always spoken",
            ));
            continue;
        }
        let asked = request
            .requested_protocols
            .iter()
            .find(|p| p.protocol == given.protocol);
        let ours = client.protocols.iter().find(|p| p.protocol == given.protocol);
        let (Some(asked), Some(ours)) = (asked, ours) else {
            deviations.push(deviation(
                format!(
                    "the OpenSession offers {}, which was not requested",
                    named(given.protocol)
                ),
                "§5.2.2 row 5.2",
                "not agreed: this client does not speak it in this session",
            ));
            continue;
        };
        let (a, g) = (&asked.protocol_version, &given.protocol_version);
        if (a.major, a.minor) != (g.major, g.minor) {
            faults.push(format!(
                "the server answers {} in version {}, and this client speaks {} - two versions of a protocol are two protocols (§5.2.2 row 5.2)",
                named(given.protocol),
                dotted(g),
                dotted(a)
            ));
            continue;
        }
        if a != g {
            deviations.push(deviation(
                format!(
                    "the OpenSession gives {} in version {}, {} was requested",
                    named(given.protocol),
                    dotted(g),
                    dotted(a)
                ),
                "§5.2.2 row 5.2",
                format!("taken as {}: only the revision differs", dotted(a)),
            ));
        }
        if given.role != asked.role {
            if parse_role(&given.role) == Some(ours.role) {
                faults.push(format!(
                    "the server says it plays `{}` in {} - the role this client plays itself: neither end would be the `{}`, and no request of this client there could be answered (§5.2.2 row 5.2)",
                    given.role,
                    named(given.protocol),
                    asked.role
                ));
                continue;
            }
            deviations.push(deviation(
                format!(
                    "the OpenSession gives role `{}` for {}, `{}` was requested",
                    given.role,
                    named(given.protocol),
                    asked.role
                ),
                "§5.2.2 row 5.2",
                format!(
                    "taken as `{}`: the server does not claim this client's role",
                    asked.role
                ),
            ));
        }
        protocols.insert(
            given.protocol,
            NegotiatedProtocol {
                version: asked.protocol_version.clone(),
                role: ours.role,
                ours: ours.capabilities.clone(),
                theirs: Capabilities::read(&given.protocol_capabilities, Scope::Protocol),
            },
        );
    }
    if protocols.is_empty() && faults.is_empty() {
        faults.push("the OpenSession agrees to none of the requested protocols".into());
    }

    let mut compression = Some(open.supported_compression.clone()).filter(|c| !c.is_empty());
    if let Some(chosen) = compression.take_if(|c| !request.supported_compression.contains(c)) {
        deviations.push(deviation(
            format!("the OpenSession chooses compression `{chosen}`, which was not offered"),
            "§5.3.2 OpenSession.supportedCompression",
            "no compression is used",
        ));
    }

    let mut formats = Vec::new();
    for format in &open.supported_formats {
        if request.supported_formats.contains(format) {
            formats.push(format.clone());
        } else {
            deviations.push(deviation(
                format!("the OpenSession chooses format `{format}`, which was not offered"),
                "§5.3.2 OpenSession.supportedFormats",
                "dropped",
            ));
        }
    }
    if formats.is_empty() {
        faults.push(format!(
            "the server agrees to no format this client offered ({} given, {} offered): no data object could be read",
            if open.supported_formats.is_empty() { "none".to_string() } else { open.supported_formats.join(", ") },
            request.supported_formats.join(", ")
        ));
    }

    let mut data_objects = Vec::new();
    for object in &open.supported_data_objects {
        match read_qualified_type(&object.qualified_type) {
            QualifiedType::Valid => data_objects.push(object.clone()),
            QualifiedType::Family(pattern) => {
                deviations.push(deviation(
                    format!(
                        "the OpenSession lists data object type `{}`",
                        object.qualified_type
                    ),
                    "§23.20 SupportedDataObject.qualifiedType",
                    format!("read as `{pattern}`"),
                ));
                data_objects.push(SupportedDataObject {
                    qualified_type: pattern,
                    ..object.clone()
                });
            }
            QualifiedType::Unreadable => deviations.push(deviation(
                format!(
                    "the OpenSession lists data object type `{}`, which is not a qualified type",
                    object.qualified_type
                ),
                "§23.20 SupportedDataObject.qualifiedType",
                "dropped",
            )),
        }
    }

    // The agreed types are the intersection of the two lists (§5.2.2 row
    // 6.1). One never requested is taken all the same: nothing here reads
    // only what was agreed - a server met in the field agrees to `resqml20` whatever is asked,
    // and serves every type it holds.
    for object in &data_objects {
        let requested = request.supported_data_objects.iter().any(|r| {
            covers(&r.qualified_type, &object.qualified_type)
                || covers(&object.qualified_type, &r.qualified_type)
        });
        if !requested {
            deviations.push(deviation(
                format!(
                    "the OpenSession agrees to data object type `{}`, which was not requested",
                    object.qualified_type
                ),
                "§5.2.2 row 6.1",
                "agreed: this client reads whatever type it is sent",
            ));
        }
    }

    if client.tolerance == Tolerance::Strict {
        faults.extend(deviations.drain(..).map(|d| format!("{} ({})", d.what, d.rule)));
    }
    // Every fault at once: whoever fixes the server fixes them in one go.
    if !faults.is_empty() {
        return Err(denied(faults.join("; ")));
    }
    Ok(Negotiated {
        session_id: open.session_id,
        peer_application: (open.application_name.clone(), open.application_version.clone()),
        protocols,
        data_objects,
        formats,
        compression,
        ours: client.endpoint.clone(),
        theirs: Capabilities::read(&open.endpoint_capabilities, Scope::Endpoint),
        peer_time: open.current_date_time,
        peer_earliest_retained: open.earliest_retained_change_time,
        deviations,
    })
}
