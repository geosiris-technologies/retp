//! The wire: what every ETP message obeys, whatever it says.
//!
//! The header and its flags, the message ids, the correlation of a response
//! to its request, multipart actions, `Acknowledge`, `ProtocolException`,
//! BLOBs sent as `Chunk`s. **No transport, no session, no `async`**: a
//! [`Wire`] is handed the bytes of one WebSocket message and says what to send
//! back and what arrived; asked to send, it hands back bytes.
//!
//! ADR-0001 is why this is one module: the numbering, the correlation and the
//! FIN bit live here and nowhere else, so that no protocol can get them
//! wrong on its own - the failure of issue #191 on the server side.
//!
//! What the specification leaves unnamed and this module had to decide is
//! written where it is decided (see ADR-0002):
//!
//! - a message on a correlation that is not open - never opened, or closed by
//!   FIN - and a message id out of order or of the wrong parity are refused
//!   with `EINVALID_STATE` (8): *"not allowed in the current state"*;
//! - a body that does not decode is `EINVALID_MESSAGE` (19), the code the
//!   specification uses for every other deserialization failure;
//! - a compressed body is refused with `ECOMPRESSION_NOTSUPPORTED` (13) until
//!   compression is negotiated (tier 2) - for Core, always.

mod chunks;
mod errors;
mod exception;
mod flags;
mod ids;
mod receive;
mod send;
mod split;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use retp_avro::{Avro, Role};
use retp_types::datatypes::{MessageHeader, MessageHeaderExtension};
use retp_types::dispatch::Message;

pub use chunks::{ChunkMessage, chunk_objects};
pub use errors::{CodeEntry, ErrorRegistry, EtpError, Incident, Known, Origin, RegistryRefusal};
pub use exception::{Exception, MalformedException};
pub use flags::Flags;
pub use ids::{MessageIds, Side};
pub use send::{Multipart, Options, SendRefusal};
pub use split::{HEADER_MAX, Oversized, split_to_fit};

/// The first protocol number the specification leaves to private protocols
/// (§3.6.1, p.60).
pub const FIRST_PRIVATE_PROTOCOL: i32 = 2000;

/// OSDU's extension of Core (`CoreOSDU`): `ResumeSession`, which asks the
/// server to take back a session the client lost, before any session opens.
/// Numbered among the private protocols, but its schemas are published
/// (OSDU's `open-etp-server`), so its messages are decoded.
pub const CORE_OSDU: i32 = 2400;

/// What the wire must know of the session: the protocols it speaks, and in
/// which role. Filled by the negotiation of tier 2; built by hand in tests.
#[derive(Debug, Clone, Default)]
pub struct Config {
    protocols: BTreeMap<i32, Role>,
    private: BTreeSet<i32>,
    header_extensions: bool,
    max_payload: Option<usize>,
    tolerance: Tolerance,
    resumption: bool,
}

/// Why a [`Config`] refused a setting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigRefusal {
    /// Core (0) is always spoken, by the connection role; it is not negotiated.
    CoreIsImplicit,
    /// A session role is `store`, `customer`, `producer` or `consumer`;
    /// `client` and `server` are connection roles (ADR-0001).
    NotASessionRole(Role),
    /// A private protocol is numbered 2000 or more (§3.6.1); below that, a
    /// number belongs to Energistics.
    NotPrivate(i32),
    /// 2000 and up are private: registered with [`Config::private_protocol`].
    Private(i32),
}

impl fmt::Display for ConfigRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigRefusal::CoreIsImplicit => write!(f, "Core (protocol 0) is not negotiated"),
            ConfigRefusal::NotASessionRole(role) => write!(
                f,
                "`{}` is a connection role; a protocol is spoken as store, customer, producer or consumer",
                role.as_str()
            ),
            ConfigRefusal::NotPrivate(p) => write!(
                f,
                "protocol {p} is below {FIRST_PRIVATE_PROTOCOL}: its number belongs to Energistics, and its messages are decoded"
            ),
            ConfigRefusal::Private(p) => {
                write!(
                    f,
                    "protocol {p} is private: it is registered as such, and passed undecoded"
                )
            }
        }
    }
}

impl std::error::Error for ConfigRefusal {}

/// How an endpoint takes what its peer gets wrong - in the negotiation
/// (`negotiate`) and in a message (here).
///
/// The rule: be tolerant of the faults of an outside client or server -
/// accept the connection anyway when the fault is not critical, and otherwise
/// give the user an explicit message that says exactly what the problem is.
/// The line between the two is this: a fault whose meaning
/// is not in doubt, or that changes nothing agreed, is taken as meant and
/// logged; a fault that would make the two ends disagree refuses, and says so
/// in full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Tolerance {
    /// Deviations are taken as meant, and logged - the negotiation's also
    /// named in `Negotiated::deviations`.
    #[default]
    Tolerant,
    /// Every deviation refuses the session - for a conformance bench.
    Strict,
}

impl Config {
    /// How the peer's faults in a message are taken.
    pub fn tolerance(mut self, tolerance: Tolerance) -> Self {
        self.tolerance = tolerance;
        self
    }

    /// Nothing negotiated: Core only.
    pub fn new() -> Self {
        Self::default()
    }

    /// Speak `protocol`, as `role` - the session role (ADR-0001).
    pub fn protocol(mut self, protocol: i32, role: Role) -> Result<Self, ConfigRefusal> {
        if protocol == 0 {
            return Err(ConfigRefusal::CoreIsImplicit);
        }
        if protocol >= FIRST_PRIVATE_PROTOCOL {
            return Err(ConfigRefusal::Private(protocol));
        }
        if matches!(role, Role::Client | Role::Server) {
            return Err(ConfigRefusal::NotASessionRole(role));
        }
        self.protocols.insert(protocol, role);
        Ok(self)
    }

    /// Accept a private protocol, whose messages arrive undecoded.
    pub fn private_protocol(mut self, protocol: i32) -> Result<Self, ConfigRefusal> {
        if protocol < FIRST_PRIVATE_PROTOCOL {
            return Err(ConfigRefusal::NotPrivate(protocol));
        }
        self.private.insert(protocol);
        Ok(self)
    }

    /// Whether both endpoints support `MessageHeaderExtension` (§3.6.2 §1).
    pub fn header_extensions(mut self, negotiated: bool) -> Self {
        self.header_extensions = negotiated;
        self
    }

    /// `MaxWebSocketMessagePayloadSize` - the smaller of the two endpoints'
    /// (§3.3.2.9, p.46). Unset, the wire checks no size: before the
    /// negotiation there is no limit it could know.
    pub fn max_payload(mut self, bytes: usize) -> Self {
        self.max_payload = Some(bytes);
        self
    }

    /// Speak [`CORE_OSDU`] as Core is spoken - in the connection roles, decoded,
    /// with nothing negotiated: what a session about to be resumed needs, and
    /// nothing after it opens (the negotiated configuration replaces this one).
    pub fn resumption(mut self, on: bool) -> Self {
        self.resumption = on;
        self
    }

    /// Whether [`CORE_OSDU`] is spoken ([`Config::resumption`]).
    pub fn resumes(&self) -> bool {
        self.resumption
    }

    /// Our session role in `protocol`, if negotiated.
    pub fn role_in(&self, protocol: i32) -> Option<Role> {
        self.protocols.get(&protocol).copied()
    }
}

/// The other role of a sub-protocol's pair.
fn counterpart(role: Role) -> Role {
    match role {
        Role::Store => Role::Customer,
        Role::Customer => Role::Store,
        Role::Producer => Role::Consumer,
        Role::Consumer => Role::Producer,
        Role::Client => Role::Server,
        Role::Server => Role::Client,
    }
}

/// One message ready for the WebSocket: its header, and the whole payload.
#[derive(Debug, Clone, PartialEq)]
pub struct Outgoing {
    /// The header, as it was written.
    pub header: MessageHeader,
    /// Header, optional extension, body - one WebSocket message.
    pub bytes: Vec<u8>,
}

/// A header as a debug line reads it: `Store.GetDataObjects #4 → #0 [FIN]`.
pub(crate) fn describe(header: &MessageHeader) -> String {
    // `ProtocolException` and `Acknowledge` travel in every protocol, and are
    // Core's messages.
    let core_everywhere = matches!(header.message_type, 1000 | 1001);
    let protocol = if core_everywhere { 0 } else { header.protocol };
    let name = retp_types::dispatch::info(protocol, header.message_type)
        .map(|info| {
            info.name
                .rsplit('.')
                .take(2)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join(".")
        })
        .unwrap_or_else(|| format!("{}/{}", header.protocol, header.message_type));
    format!(
        "{name} #{} → #{} [{}]",
        header.message_id,
        header.correlation_id,
        Flags(header.message_flags)
    )
}

impl Outgoing {
    /// The flags the header carries.
    pub fn flags(&self) -> Flags {
        Flags(self.header.message_flags)
    }
}

/// What an incoming message is to the conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Place {
    /// The peer opens an action: a single message, or the first part of a
    /// multipart one (correlation 0).
    Opens,
    /// A later part of the peer's multipart action opened by `first`.
    Continues {
        /// The id of the action's first message.
        first: i64,
    },
    /// A response - or part of one - to our request `request`.
    Answers {
        /// The id of our request.
        request: i64,
    },
    /// An `Acknowledge` of our message `message`.
    Acknowledges {
        /// The id acknowledged.
        message: i64,
    },
    /// A `ProtocolException` about our message `message`, which no response
    /// was awaited for - a notification, say.
    About {
        /// The id the exception is about.
        message: i64,
    },
    /// A `ProtocolException` attached to no message: `correlationId` 0, as
    /// for backpressure or an expiring authorization (§3.7.2.1 §3.c).
    Unattached,
    /// A `ProtocolException` whose correlation names no message of ours. It
    /// is not answered - an exception is never answered with one - so it is
    /// handed over with the correlation it carried.
    Stray {
        /// The correlation received.
        correlation: i64,
    },
}

/// What a delivered message holds.
///
/// The decoded variant is the large one and the common one; boxing it would
/// spend an allocation on every message to save bytes on the rare private one.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Body {
    /// A decoded message of a protocol of the specification.
    Message(Message),
    /// The body of a private protocol's message, undecoded: its schema is
    /// not the specification's to know.
    Private(Vec<u8>),
}

/// One message handed to the application: checked, decoded, placed.
#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    /// Its header. For a message whose BLOBs arrived in `Chunk`s, the header
    /// of the message that announced them.
    pub header: MessageHeader,
    /// Its header extension, if it carried one.
    pub extension: Option<MessageHeaderExtension>,
    /// What it says.
    pub body: Body,
    /// Where it belongs.
    pub place: Place,
    /// Whether this completes the action - the FIN bit of the last message
    /// that went into it.
    pub last: bool,
}

impl Delivery {
    /// The exception, if this is a `ProtocolException`. The wire has refused
    /// the malformed ones already, so this cannot fail on a delivery.
    pub fn exception(&self) -> Option<Exception> {
        match &self.body {
            Body::Message(Message::ProtocolException(message)) => Exception::from_message(message).ok(),
            _ => None,
        }
    }

    /// The id a response to this message correlates to: for a later part,
    /// the first part's id (§3.7.3.1 §7.b).
    pub fn request_id(&self) -> i64 {
        match self.place {
            Place::Continues { first } => first,
            _ => self.header.message_id,
        }
    }
}

/// What one received WebSocket message produced.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Received {
    /// What to send back, in order. An `Acknowledge`, when one was asked for,
    /// is first - before any exception about the same message (§3.7.2.2 §3.b).
    pub replies: Vec<Outgoing>,
    /// What arrived for the application. `None` when the message was refused,
    /// and while a message waits for the `Chunk`s of its BLOBs.
    pub delivery: Option<Delivery>,
    /// What was wrong with a `ProtocolException` the peer sent. It is not
    /// answered - an exception answered with an exception can loop - so it is
    /// said here instead of on the wire.
    pub fault: Option<Incident>,
    /// A message refused while it answered a request of ours: that request
    /// is over - abandoned here - and this is why. Without it, the caller
    /// would wait for an answer that has already come and gone.
    pub refused_answer: Option<(i64, Incident)>,
}

/// A message announced BLOBs; held until its `Chunk`s have all arrived.
#[derive(Debug, Clone)]
struct Held {
    delivery: Delivery,
    /// The key of the action it belongs to, as the correlation table has it.
    group: i64,
    /// BLOB id → the bytes so far, and whether the final chunk came.
    blobs: BTreeMap<[u8; 16], (Vec<u8>, bool)>,
}

/// The peer's multipart action still open.
#[derive(Debug, Clone, Copy)]
struct Open {
    protocol: i32,
}

/// One end of an ETP connection, as far as the wire goes.
#[derive(Debug, Clone)]
pub struct Wire {
    side: Side,
    config: Config,
    ids: MessageIds,
    last_peer_id: i64,
    /// Our requests whose responses are still expected: id → protocol.
    awaiting: BTreeMap<i64, i32>,
    /// The peer's multipart actions still open, by first id.
    open: BTreeMap<i64, Open>,
    /// Messages waiting for their BLOBs.
    held: Vec<Held>,
}

impl Wire {
    /// One end of a connection: which side, and what the session agreed.
    pub fn new(side: Side, config: Config) -> Self {
        Self {
            side,
            config,
            ids: MessageIds::new(side),
            last_peer_id: 0,
            awaiting: BTreeMap::new(),
            open: BTreeMap::new(),
            held: Vec::new(),
        }
    }

    /// Which side this is.
    pub fn side(&self) -> Side {
        self.side
    }

    /// What the session agreed.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Replace what the session agreed - once negotiated.
    pub fn reconfigure(&mut self, config: Config) {
        self.config = config;
    }

    /// Whether a response to our request `id` is still expected.
    pub fn is_awaiting(&self, id: i64) -> bool {
        self.awaiting.contains_key(&id)
    }

    /// Stop tracking the action or request `id`: whatever arrives for it later
    /// is refused as not open. For a timeout, a cancellation, a refusal.
    pub fn abandon(&mut self, id: i64) {
        self.open.remove(&id);
        self.awaiting.remove(&id);
        self.held.retain(|h| h.group != id);
    }

    /// How many multipart actions the peer has open in `protocol` - what
    /// `MaxConcurrentMultipart` limits (§3.7.3.1.1.1).
    pub fn open_in(&self, protocol: i32) -> usize {
        self.open.values().filter(|o| o.protocol == protocol).count()
    }

    /// The role this end plays in `protocol`: its connection role in Core,
    /// its session role elsewhere.
    fn our_role(&self, protocol: i32) -> Option<Role> {
        if protocol == 0 || (protocol == CORE_OSDU && self.config.resumption) {
            Some(self.side.role())
        } else {
            self.config.role_in(protocol)
        }
    }

    /// The role the peer plays in `protocol`.
    fn peer_role(&self, protocol: i32) -> Option<Role> {
        self.our_role(protocol).map(counterpart)
    }
}

/// Header, extension and body, as one payload.
fn frame(header: &MessageHeader, extension: Option<&MessageHeaderExtension>, body: &[u8]) -> Vec<u8> {
    let mut bytes = header.to_avro();
    if let Some(extension) = extension {
        bytes.extend_from_slice(&extension.to_avro());
    }
    bytes.extend_from_slice(body);
    bytes
}
