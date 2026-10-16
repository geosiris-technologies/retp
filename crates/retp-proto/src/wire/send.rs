//! Sending: numbering, correlation, FIN, and the rules of multipart and map
//! responses (§3.5.4.1, §3.7.2, §3.7.3).
//!
//! Every message leaves through [`Wire::emit`], so the id, the FIN bit and the
//! size check cannot be skipped by any path.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use retp_avro::{Avro, EtpMessage, Role};
use retp_types::datatypes::{ErrorInfo, MessageHeader, MessageHeaderExtension};
use retp_types::protocol::core::{Acknowledge, ProtocolException};

use super::{Delivery, EtpError, Exception, Flags, Incident, Outgoing, Wire, frame};

/// What a message is sent with, beside its body.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Options {
    /// Ask the peer for an `Acknowledge` (flag `0x10`).
    pub ack: bool,
    /// A `MessageHeaderExtension` - only if both ends negotiated them.
    pub extension: Option<MessageHeaderExtension>,
}

/// Why a message was not sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendRefusal {
    /// The protocol was not negotiated.
    NotNegotiated {
        /// The protocol.
        protocol: i32,
    },
    /// Our role may not send this message (`senderRole`, ADR-0001).
    RoleMayNotSend {
        /// The message.
        message: &'static str,
        /// Our role in its protocol.
        role: Role,
    },
    /// The message's schema says `multipartFlag: false`: it is one message,
    /// never a part (§3.5.5, ADR-0001).
    NotMultipart {
        /// The message.
        message: &'static str,
    },
    /// An `Acknowledge` asking for an `Acknowledge` - the endless loop
    /// §3.7.2.2 §2.a forbids.
    AckOfAck,
    /// A header extension, which the session did not negotiate (§3.6.2 §1.c).
    ExtensionNotNegotiated,
    /// Over `MaxWebSocketMessagePayloadSize` (§3.3.2.9).
    Oversized {
        /// The message.
        message: &'static str,
        /// Its size, header included.
        size: usize,
        /// The limit.
        limit: usize,
    },
    /// A response carries its request's protocol.
    WrongProtocol {
        /// The request's protocol.
        request: i32,
        /// The message's.
        message: i32,
    },
    /// The multipart action or response has had its FIN: nothing more goes
    /// out on its correlation (§3.7.3.1 §7.c).
    Finished,
    /// Map errors, a terminating exception or a cut belong to a response.
    NotAResponse,
    /// A key answered twice - across the parts of one response too
    /// (§3.7.3 §2.b.ii).
    KeyTwice(String),
    /// A key the request did not carry.
    UnknownKey(String),
    /// More items than `MaxResponseCount` allows (§3.7.3.1 §4.e).
    OverResponseCount {
        /// The limit.
        max: usize,
    },
    /// A cut before `MaxResponseCount` items were sent (§3.7.3.1 §4.e.iii.2).
    CutTooEarly {
        /// Items sent.
        sent: usize,
        /// The limit.
        max: usize,
    },
    /// Nothing to close a response with: no pending error, no message.
    NothingToClose,
}

impl fmt::Display for SendRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendRefusal::NotNegotiated { protocol } => write!(f, "protocol {protocol} was not negotiated"),
            SendRefusal::RoleMayNotSend { message, role } => {
                write!(f, "{message} is not sent by the {}", role.as_str())
            }
            SendRefusal::NotMultipart { message } => write!(
                f,
                "{message} has multipartFlag false: it is sent as one message, never as a part (§3.5.5)"
            ),
            SendRefusal::AckOfAck => write!(f, "an Acknowledge never asks for one (§3.7.2.2 §2.a)"),
            SendRefusal::ExtensionNotNegotiated => {
                write!(f, "header extensions were not negotiated (§3.6.2 §1.c)")
            }
            SendRefusal::Oversized { message, size, limit } => write!(
                f,
                "{message} takes {size} bytes, over the session's {limit}: split it if it is multipart, refuse it otherwise (§3.3.2.9)"
            ),
            SendRefusal::WrongProtocol { request, message } => {
                write!(
                    f,
                    "a response to protocol {request} cannot be a message of protocol {message}"
                )
            }
            SendRefusal::Finished => write!(f, "the action has had its FIN; nothing more goes out on it"),
            SendRefusal::NotAResponse => write!(
                f,
                "only a response carries map errors, a terminating exception or a cut"
            ),
            SendRefusal::KeyTwice(key) => write!(f, "key `{key}` is answered twice (§3.7.3 §2.b.ii)"),
            SendRefusal::UnknownKey(key) => write!(f, "key `{key}` is not one of the request's"),
            SendRefusal::OverResponseCount { max } => write!(f, "MaxResponseCount is {max}"),
            SendRefusal::CutTooEarly { sent, max } => write!(
                f,
                "{sent} items sent of {max}: a response is cut only once MaxResponseCount items are out (§3.7.3.1 §4.e.iii)"
            ),
            SendRefusal::NothingToClose => write!(
                f,
                "no pending error to close the response with; send a last message"
            ),
        }
    }
}

impl std::error::Error for SendRefusal {}

/// What a header says, before it has an id.
#[derive(Debug, Clone, Copy)]
struct Envelope {
    name: &'static str,
    protocol: i32,
    message_type: i32,
    correlation: i64,
    flags: Flags,
}

#[derive(Debug, Clone, PartialEq)]
enum Kind {
    /// Our multipart request or notification: the first part goes out with
    /// correlation 0, the others with the first part's id (§3.7.3.1 §6).
    Action {
        expects_response: bool,
        first: Option<i64>,
    },
    /// A response: every part correlates to the request (§3.7.3.1 §7).
    Response { request: i64 },
}

/// A multipart action or response being sent, one message at a time.
///
/// It carries the bookkeeping of a map response (§3.7.3 §4): which keys the
/// request carried, which have been answered - positively or in the errors
/// map - and the errors not sent yet. Closing it sends what is owed, in the
/// order the specification asks: every map error before a terminating one,
/// and the FIN on whatever goes last.
#[derive(Debug, Clone, PartialEq)]
pub struct Multipart {
    protocol: i32,
    kind: Kind,
    options: Options,
    parts: usize,
    finished: bool,
    keys: Option<BTreeSet<String>>,
    answered: BTreeSet<String>,
    errors: BTreeMap<String, ErrorInfo>,
    max_items: Option<usize>,
    items: usize,
}

impl Multipart {
    fn new(protocol: i32, kind: Kind, options: Options) -> Self {
        Self {
            protocol,
            kind,
            options,
            parts: 0,
            finished: false,
            keys: None,
            answered: BTreeSet::new(),
            errors: BTreeMap::new(),
            max_items: None,
            items: 0,
        }
    }

    /// The keys of the map request this answers: each must be answered once,
    /// positively or with an error.
    pub fn for_keys(mut self, keys: impl IntoIterator<Item = String>) -> Self {
        self.keys = Some(keys.into_iter().collect());
        self
    }

    /// `MaxResponseCount` for a non-map response: the smaller of the two
    /// endpoints' (§3.7.3.1 §4.e).
    pub fn with_max_items(mut self, max: usize) -> Self {
        self.max_items = Some(max);
        self
    }

    /// Whether the FIN has gone out.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Record that `key` is answered positively, in a message about to go.
    pub fn answered(&mut self, key: impl Into<String>) -> Result<(), SendRefusal> {
        self.take_key(key.into())
    }

    /// Record an error for `key`; it goes out in the next errors message.
    pub fn failed(&mut self, key: impl Into<String>, incident: &Incident) -> Result<(), SendRefusal> {
        let key = key.into();
        self.take_key(key.clone())?;
        self.errors.insert(key, incident.error_info());
        Ok(())
    }

    /// Record `count` more items of a non-map response.
    pub fn counted(&mut self, count: usize) -> Result<(), SendRefusal> {
        if let Some(max) = self.max_items
            && self.items + count > max
        {
            return Err(SendRefusal::OverResponseCount { max });
        }
        self.items += count;
        Ok(())
    }

    fn take_key(&mut self, key: String) -> Result<(), SendRefusal> {
        if !matches!(self.kind, Kind::Response { .. }) {
            return Err(SendRefusal::NotAResponse);
        }
        if let Some(keys) = &self.keys
            && !keys.contains(&key)
        {
            return Err(SendRefusal::UnknownKey(key));
        }
        if !self.answered.insert(key.clone()) {
            return Err(SendRefusal::KeyTwice(key));
        }
        Ok(())
    }

    /// Keys the request carried that nothing answered: each gets an error
    /// rather than a silence the peer would wait on until its timeout.
    fn owe_unanswered(&mut self) {
        let Some(keys) = &self.keys else {
            return;
        };
        let missing: Vec<String> = keys.difference(&self.answered).cloned().collect();
        for key in missing {
            let incident = EtpError::EREQUEST_DENIED
                .with_detail("no answer was produced for this key; it was not processed");
            self.errors.insert(key.clone(), incident.error_info());
            self.answered.insert(key);
        }
    }

    fn correlation(&self) -> i64 {
        match self.kind {
            Kind::Action { first, .. } => first.unwrap_or(0),
            Kind::Response { request } => request,
        }
    }
}

impl Wire {
    /// The one exit: an id, the header, the size check.
    fn emit(
        &mut self,
        envelope: Envelope,
        extension: Option<&MessageHeaderExtension>,
        body: &[u8],
    ) -> Result<Outgoing, SendRefusal> {
        let Envelope {
            name,
            protocol,
            message_type,
            correlation,
            flags,
        } = envelope;
        if extension.is_some() && !self.config.header_extensions {
            return Err(SendRefusal::ExtensionNotNegotiated);
        }
        let flags = if extension.is_some() {
            flags | Flags::EXTENSION
        } else {
            flags
        };
        // The size is checked before an id is spent, so a refusal leaves no
        // gap in the numbering. The header is measured at its largest.
        let size = super::HEADER_MAX + extension.map_or(0, |e| e.to_avro().len()) + body.len();
        if let Some(limit) = self.config.max_payload
            && size > limit
        {
            return Err(SendRefusal::Oversized {
                message: name,
                size,
                limit,
            });
        }
        let header = MessageHeader {
            protocol,
            message_type,
            correlation_id: correlation,
            message_id: self.ids.issue(),
            message_flags: flags.0,
        };
        let bytes = frame(&header, extension, body);
        tracing::debug!(
            target: "retp_proto",
            etp_message = %super::describe(&header),
            protocol = header.protocol,
            message_id = header.message_id,
            correlation_id = header.correlation_id,
            bytes = bytes.len(),
            "sent {} ({} bytes)",
            super::describe(&header),
            bytes.len()
        );
        Ok(Outgoing { header, bytes })
    }

    /// Check that we may send `M` at all.
    fn may_send<M: EtpMessage>(&self) -> Result<(), SendRefusal> {
        let role = self.our_role(M::PROTOCOL).ok_or(SendRefusal::NotNegotiated {
            protocol: M::PROTOCOL,
        })?;
        if !M::SENDER_ROLE.allows(role) {
            return Err(SendRefusal::RoleMayNotSend {
                message: M::FULL_NAME,
                role,
            });
        }
        Ok(())
    }

    fn emit_message<M: EtpMessage>(
        &mut self,
        message: &M,
        correlation: i64,
        fin: bool,
        options: &Options,
    ) -> Result<Outgoing, SendRefusal> {
        self.may_send::<M>()?;
        if options.ack && M::PROTOCOL == 0 && M::MESSAGE_TYPE == ACKNOWLEDGE {
            return Err(SendRefusal::AckOfAck);
        }
        let mut flags = if fin { Flags::FIN } else { Flags::NONE };
        if options.ack {
            flags = flags | Flags::ACK_REQUESTED;
        }
        let envelope = Envelope {
            name: M::FULL_NAME,
            protocol: M::PROTOCOL,
            message_type: M::MESSAGE_TYPE,
            correlation,
            flags,
        };
        self.emit(envelope, options.extension.as_ref(), &message.to_avro())
    }

    /// A request, in one message, whose responses the wire will then accept.
    pub fn request<M: EtpMessage>(&mut self, message: &M) -> Result<Outgoing, SendRefusal> {
        self.request_with(message, &Options::default())
    }

    /// [`Wire::request`], with options.
    pub fn request_with<M: EtpMessage>(
        &mut self,
        message: &M,
        options: &Options,
    ) -> Result<Outgoing, SendRefusal> {
        let out = self.emit_message(message, 0, true, options)?;
        self.awaiting.insert(out.header.message_id, M::PROTOCOL);
        Ok(out)
    }

    /// A message that expects no response - a notification, `CloseSession`,
    /// a `ChannelData` - in one message.
    pub fn notify<M: EtpMessage>(&mut self, message: &M) -> Result<Outgoing, SendRefusal> {
        self.notify_with(message, &Options::default())
    }

    /// [`Wire::notify`], with options.
    pub fn notify_with<M: EtpMessage>(
        &mut self,
        message: &M,
        options: &Options,
    ) -> Result<Outgoing, SendRefusal> {
        self.emit_message(message, 0, true, options)
    }

    /// A response in one message, to `request`.
    pub fn respond<M: EtpMessage>(
        &mut self,
        request: &Delivery,
        message: &M,
    ) -> Result<Outgoing, SendRefusal> {
        if request.header.protocol != M::PROTOCOL {
            return Err(SendRefusal::WrongProtocol {
                request: request.header.protocol,
                message: M::PROTOCOL,
            });
        }
        self.emit_message(message, request.request_id(), true, &Options::default())
    }

    /// Begin our own multipart request (`expects_response`) or notification.
    pub fn begin(&self, protocol: i32, expects_response: bool, options: Options) -> Multipart {
        Multipart::new(
            protocol,
            Kind::Action {
                expects_response,
                first: None,
            },
            options,
        )
    }

    /// Begin a response, possibly multipart, to `request`.
    pub fn begin_response(&self, request: &Delivery) -> Multipart {
        Multipart::new(
            request.header.protocol,
            Kind::Response {
                request: request.request_id(),
            },
            Options::default(),
        )
    }

    /// One part, not the last: it must be a multipart message.
    pub fn part<M: EtpMessage>(
        &mut self,
        multipart: &mut Multipart,
        message: &M,
    ) -> Result<Outgoing, SendRefusal> {
        if !M::MULTIPART {
            return Err(SendRefusal::NotMultipart {
                message: M::FULL_NAME,
            });
        }
        self.send_part(multipart, message, false)
    }

    /// The last message, with the FIN - after the errors still owed, which
    /// go out first. A part of a longer action must be a multipart message;
    /// alone, any message may close it.
    pub fn last<M: EtpMessage>(
        &mut self,
        multipart: &mut Multipart,
        message: &M,
    ) -> Result<Vec<Outgoing>, SendRefusal> {
        if multipart.finished {
            return Err(SendRefusal::Finished);
        }
        multipart.owe_unanswered();
        let owed = !multipart.errors.is_empty();
        if (multipart.parts > 0 || owed) && !M::MULTIPART {
            return Err(SendRefusal::NotMultipart {
                message: M::FULL_NAME,
            });
        }
        let mut out = self.flush_errors(multipart, false)?;
        out.push(self.send_part(multipart, message, true)?);
        Ok(out)
    }

    /// Close a response with the errors still owed, the last of them with the
    /// FIN - for a map response whose every key failed.
    pub fn close(&mut self, multipart: &mut Multipart) -> Result<Vec<Outgoing>, SendRefusal> {
        if multipart.finished {
            return Err(SendRefusal::Finished);
        }
        if !matches!(multipart.kind, Kind::Response { .. }) {
            return Err(SendRefusal::NotAResponse);
        }
        multipart.owe_unanswered();
        if multipart.errors.is_empty() {
            return Err(SendRefusal::NothingToClose);
        }
        let out = self.flush_errors(multipart, true)?;
        multipart.finished = true;
        Ok(out)
    }

    /// End a response with a terminating error - after every map error it
    /// was able to send (§3.7.3 §4.f). Keys never answered are **not** owed an
    /// error here: a terminating exception says the request failed as a
    /// whole, which is what the customer retries (§3.7.3 §4.c).
    pub fn fail(
        &mut self,
        multipart: &mut Multipart,
        incident: &Incident,
    ) -> Result<Vec<Outgoing>, SendRefusal> {
        if multipart.finished {
            return Err(SendRefusal::Finished);
        }
        if !matches!(multipart.kind, Kind::Response { .. }) {
            return Err(SendRefusal::NotAResponse);
        }
        let mut out = self.flush_errors(multipart, false)?;
        out.push(self.send_exception(multipart, Exception::Single(incident.error_info()), true)?);
        Ok(out)
    }

    /// Abandon a multipart action or response: `EMULTIPART_CANCELLED`, one
    /// error, no map, the FIN (§3.7.3.1 §9).
    pub fn cancel(&mut self, multipart: &mut Multipart) -> Result<Outgoing, SendRefusal> {
        if multipart.finished {
            return Err(SendRefusal::Finished);
        }
        let error = EtpError::EMULTIPART_CANCELLED.incident().error_info();
        self.send_exception(multipart, Exception::Single(error), true)
    }

    /// Cut a non-map response at `MaxResponseCount`: `ERESPONSECOUNT_EXCEEDED`
    /// with the FIN - refused until that many items are out (§3.7.3.1 §4.e.iii).
    pub fn cut(&mut self, multipart: &mut Multipart) -> Result<Outgoing, SendRefusal> {
        if multipart.finished {
            return Err(SendRefusal::Finished);
        }
        let Some(max) = multipart.max_items else {
            return Err(SendRefusal::NotAResponse);
        };
        if multipart.items < max {
            return Err(SendRefusal::CutTooEarly {
                sent: multipart.items,
                max,
            });
        }
        let error = EtpError::ERESPONSECOUNT_EXCEEDED.incident().error_info();
        self.send_exception(multipart, Exception::Single(error), true)
    }

    fn send_part<M: EtpMessage>(
        &mut self,
        multipart: &mut Multipart,
        message: &M,
        fin: bool,
    ) -> Result<Outgoing, SendRefusal> {
        if multipart.finished {
            return Err(SendRefusal::Finished);
        }
        if M::PROTOCOL != multipart.protocol {
            return Err(SendRefusal::WrongProtocol {
                request: multipart.protocol,
                message: M::PROTOCOL,
            });
        }
        let options = multipart.options.clone();
        let out = self.emit_message(message, multipart.correlation(), fin, &options)?;
        self.opened(multipart, &out);
        if fin {
            multipart.finished = true;
        }
        Ok(out)
    }

    /// The first part of our own action fixes the correlation of the others,
    /// and - for a request - is what the responses will correlate to.
    fn opened(&mut self, multipart: &mut Multipart, out: &Outgoing) {
        multipart.parts += 1;
        if let Kind::Action {
            expects_response,
            first,
        } = &mut multipart.kind
            && first.is_none()
        {
            *first = Some(out.header.message_id);
            if *expects_response {
                self.awaiting.insert(out.header.message_id, multipart.protocol);
            }
        }
    }

    fn flush_errors(&mut self, multipart: &mut Multipart, fin: bool) -> Result<Vec<Outgoing>, SendRefusal> {
        if multipart.errors.is_empty() {
            return Ok(Vec::new());
        }
        let errors = std::mem::take(&mut multipart.errors);
        Ok(vec![self.send_exception(
            multipart,
            Exception::Map(errors),
            fin,
        )?])
    }

    fn send_exception(
        &mut self,
        multipart: &mut Multipart,
        exception: Exception,
        fin: bool,
    ) -> Result<Outgoing, SendRefusal> {
        let flags = if fin { Flags::FIN } else { Flags::NONE };
        let out = self.emit_exception(multipart.protocol, multipart.correlation(), flags, exception)?;
        self.opened(multipart, &out);
        if fin {
            multipart.finished = true;
        }
        Ok(out)
    }

    /// A `ProtocolException`: the protocol it concerns, never compressed
    /// (§3.7.2.1 §3.d.i) - this path has no way to set `0x08`.
    fn emit_exception(
        &mut self,
        protocol: i32,
        correlation: i64,
        flags: Flags,
        exception: Exception,
    ) -> Result<Outgoing, SendRefusal> {
        let single = match &exception {
            Exception::Single(error) => Some(error.clone()),
            Exception::Map(_) => None,
        };
        let envelope = Envelope {
            name: ProtocolException::FULL_NAME,
            protocol,
            message_type: PROTOCOL_EXCEPTION,
            correlation,
            flags,
        };
        let body = exception.into_message().to_avro();
        let sent = self.emit(envelope, None, &body);
        match (sent, single) {
            // A detail too long for a tiny session limit costs the detail, not
            // the exception: the code is what the peer acts on.
            (Err(SendRefusal::Oversized { .. }), Some(mut error)) => {
                error.message = EtpError::from_code(error.code)
                    .map_or_else(|| error.code.to_string(), |e| e.name().to_string());
                let body = Exception::Single(error).into_message().to_avro();
                self.emit(envelope, None, &body)
            }
            (sent, _) => sent,
        }
    }

    /// One exception about a message the peer sent: its protocol, its id,
    /// the FIN.
    pub fn exception(&mut self, about: &MessageHeader, incident: &Incident) -> Outgoing {
        self.emit_exception(
            about.protocol,
            about.message_id,
            Flags::FIN,
            Exception::Single(incident.error_info()),
        )
        .expect("one ProtocolException always fits")
    }

    /// An exception answering `delivery` - correlated to the request it
    /// belongs to, the first part's id for a multipart one (§3.7.3.1 §7.b).
    pub fn refuse(&mut self, delivery: &Delivery, incident: &Incident) -> Outgoing {
        self.exception_to(delivery.header.protocol, delivery.request_id(), incident)
    }

    /// An exception in `protocol`, correlated to `correlation`.
    pub fn exception_to(&mut self, protocol: i32, correlation: i64, incident: &Incident) -> Outgoing {
        self.emit_exception(
            protocol,
            correlation,
            Flags::FIN,
            Exception::Single(incident.error_info()),
        )
        .expect("one ProtocolException always fits")
    }

    /// An exception attached to no message: protocol 0, correlation 0 - for a
    /// header that could not be read, backpressure, an expiring
    /// authorization (§3.7.2.1 §3.c).
    pub fn unattached(&mut self, incident: &Incident) -> Outgoing {
        self.emit_exception(0, 0, Flags::FIN, Exception::Single(incident.error_info()))
            .expect("one ProtocolException always fits")
    }

    /// The `Acknowledge` of the message whose header this is: its protocol,
    /// its id, the FIN - and never `0x10` (§3.7.2.2 §3.d).
    pub(super) fn acknowledge(&mut self, of: &MessageHeader) -> Outgoing {
        let envelope = Envelope {
            name: Acknowledge::FULL_NAME,
            protocol: of.protocol,
            message_type: ACKNOWLEDGE,
            correlation: of.message_id,
            flags: Flags::FIN,
        };
        self.emit(envelope, None, &Acknowledge {}.to_avro())
            .expect("an Acknowledge always fits")
    }
}

/// The message type of `ProtocolException`, in whatever protocol it travels.
pub(super) const PROTOCOL_EXCEPTION: i32 = 1000;
/// The message type of `Acknowledge`, in whatever protocol it travels.
pub(super) const ACKNOWLEDGE: i32 = 1001;
