//! The connection common to a client and a server (ADR-0001).
//!
//! A [`Connection`] is instantiated with its side, and chains messages by the
//! rules of the protocol. It handles **natively** what every endpoint must -
//! `Ping` → `Pong`, `Authorize`, `RequestSession` → `OpenSession`,
//! `CloseSession`, `Acknowledge`, `ProtocolException`, the timeouts - and
//! **delegates** the rest to a table of [`Handler`]s, one per protocol,
//! filled at construction. A message of a protocol without a handler is never
//! ignored: the wire answers it.
//!
//! **The protocols a connection announces are its handlers', and nothing
//! else**: a protocol is implemented entirely or not announced (§3.1.1.2 §3).
//!
//! **It never reads a clock.** Every call that can depend on time takes `now` -
//! microseconds since the Unix epoch, UTC, the unit of ETP's
//! `currentDateTime` - so that a test moves time forward by a number, and the
//! transport keeps its own timer.

use std::collections::BTreeMap;

use retp_avro::{Avro, EtpMessage, Reader};
use retp_types::datatypes::{MessageHeader, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::core::{
    Authorize, AuthorizeResponse, CloseSession, OpenSession, Ping, Pong, RequestSession,
};

use crate::capabilities::{Problem, names};
use crate::credentials::Credentials;
use crate::negotiate::{Negotiated, Offer, OfferRefusal, ProtocolOffer, accept_open_session, open_session};
use crate::wire::{Body, Delivery, EtpError, Exception, Incident, Outgoing, Place, SendRefusal, Side, Wire};

/// Microseconds since the Unix epoch, UTC.
pub type Micros = i64;

const SECOND: Micros = 1_000_000;

/// How long a client lets a session go quiet before it says something - a
/// `Ping` (§5.3.5, the "high-water mark" message, which may be sent by either
/// end). Servers and proxies close a session nothing crosses; a server does
/// not ping by default, a client does.
pub const CLIENT_KEEPALIVE: std::time::Duration = std::time::Duration::from_secs(30);

/// What a handler did with a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handled {
    /// It answered, or needed no answer.
    Answered,
    /// The application wants it: it goes out as [`Event::Delivered`].
    Forward,
}

/// A handler that does not implement an operation of its protocol. The
/// connection answers `ENOTSUPPORTED` (7), with this as the detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declined(pub String);

/// What a handler answers with: the wire, and the messages to send.
pub struct Responder<'a> {
    wire: &'a mut Wire,
    out: &'a mut Vec<Outgoing>,
}

impl Responder<'_> {
    /// The wire - to begin a multipart response, send a part.
    pub fn wire(&mut self) -> &mut Wire {
        self.wire
    }

    /// Queue a message for sending.
    pub fn send(&mut self, message: Outgoing) {
        self.out.push(message);
    }

    /// Queue several.
    pub fn send_all(&mut self, messages: Vec<Outgoing>) {
        self.out.extend(messages);
    }
}

/// One protocol, implemented.
pub trait Handler: Send {
    /// The protocol, its version, the role this end plays, its capabilities.
    fn offer(&self) -> ProtocolOffer;

    /// A message the peer opened or continued in this protocol - a request, a
    /// notification. Responses to this end's own requests do not come here:
    /// they go to the application as [`Event::Delivered`].
    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined>;
}

/// A handler that forwards every message to the application - for a
/// customer, whose peer only ever sends notifications on its own.
pub struct Forward(pub ProtocolOffer);

impl Handler for Forward {
    fn offer(&self) -> ProtocolOffer {
        self.0.clone()
    }

    fn handle(&mut self, _: &Delivery, _: &mut Responder<'_>) -> Result<Handled, Declined> {
        Ok(Handled::Forward)
    }
}

/// A server's verdict on an `Authorize`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authorization {
    /// Granted, until `expires_at` if the token says when.
    Granted {
        /// When the authorization runs out.
        expires_at: Option<Micros>,
    },
    /// Not granted: the challenges telling the client how to get a token.
    Challenge(Vec<String>),
}

/// What decides whether a token is good - the application's, never the
/// protocol's.
pub trait Authorizer: Send {
    /// Judge the `authorization` field of an `Authorize` - an HTTP
    /// `Authorization` header value (`Bearer …`, `Basic …`), or empty to ask
    /// for the challenges.
    fn authorize(&mut self, authorization: &str, now: Micros) -> Authorization;
}

impl<F: FnMut(&str, Micros) -> Authorization + Send> Authorizer for F {
    fn authorize(&mut self, authorization: &str, now: Micros) -> Authorization {
        self(authorization, now)
    }
}

/// Where a connection is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// The WebSocket is open; no `RequestSession` yet.
    AwaitingSession,
    /// Authorization is going on.
    Authorizing,
    /// `RequestSession` sent or received, no `OpenSession` yet.
    Negotiating,
    /// The session is open.
    Open,
    /// The session is over, or never opened; the WebSocket is to be closed.
    Closed,
}

/// What the application learns.
///
/// `Delivered` is the large variant and the common one; boxing it would cost
/// an allocation per message to save bytes on the rare others.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The session is open: [`Connection::negotiated`] says with what.
    Opened,
    /// A message for the application: a response to its request, an
    /// acknowledgement, an exception, or what a [`Forward`] handler passed on.
    Delivered(Delivery),
    /// The server wants credentials; the challenges say where to get them.
    /// Answer with [`Connection::authorize`].
    AuthorizationNeeded {
        /// `WWW-Authenticate` style challenges.
        challenges: Vec<String>,
    },
    /// The server warns that the authorization runs out soon
    /// (`EAUTHORIZATION_EXPIRING`, §3.13.5). Renew it with
    /// [`Connection::authorize`].
    AuthorizationExpiring,
    /// A request of ours had no response in time, or its response stopped
    /// arriving. It is abandoned.
    TimedOut {
        /// The request.
        request: i64,
        /// Why.
        incident: Incident,
    },
    /// The session could not open - the peer's refusal or ours.
    Refused(Incident),
    /// Capabilities the peer sent that were wrong: named, and handled as
    /// `capabilities.rs` says.
    CapabilityProblems(Vec<Problem>),
    /// An answer to a request of ours arrived and was refused - unreadable,
    /// or against a rule this end holds to. The request is over, with this
    /// incident: the answer will not come again.
    AnswerRefused {
        /// The request.
        request: i64,
        /// Why its answer was refused.
        incident: Incident,
    },
    /// The session is over.
    Closed {
        /// Why, as said by whoever closed it.
        reason: String,
    },
    /// Something wrong in what the peer sent, not answered on the wire.
    Fault(Incident),
}

/// What one call produced.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Step {
    /// Messages to send, in order.
    pub send: Vec<Outgoing>,
    /// What the application learns.
    pub events: Vec<Event>,
    /// Whether the transport must now close the WebSocket.
    pub close: bool,
}

/// Thresholds of the send queue, in bytes (§3.13.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backpressure {
    /// Warn the peer from here: `EBACKPRESSURE_WARNING` (25).
    pub warn_at: usize,
    /// Give up from here: `EBACKPRESSURE_LIMIT_EXCEEDED` (24), then close.
    pub limit_at: usize,
}

/// A multipart action whose parts are arriving.
#[derive(Debug, Clone, Copy)]
struct Arriving {
    protocol: i32,
    last_part: Micros,
}

/// One end of an ETP session.
pub struct Connection {
    side: Side,
    wire: Wire,
    offer: Offer,
    handlers: BTreeMap<i32, Box<dyn Handler>>,
    state: State,
    negotiated: Option<Negotiated>,
    watermark: Option<Micros>,
    // Keeping the session alive: the period, the last time anything crossed,
    // and the keepalive `Ping` still waiting for its `Pong`.
    keepalive: Option<Micros>,
    last_activity: Micros,
    keepalive_ping: Option<i64>,
    // A client's.
    credentials: Option<Credentials>,
    credentials_tried: bool,
    request: Option<RequestSession>,
    request_id: Option<i64>,
    authorize_id: Option<i64>,
    // A server's.
    authorizer: Option<Box<dyn Authorizer>>,
    authorized: bool,
    expires_at: Option<Micros>,
    warned_expiring: bool,
    session_id: Uuid,
    earliest_retained: Micros,
    // Time.
    connected_at: Micros,
    session_requested_at: Option<Micros>,
    requests: BTreeMap<i64, Micros>,
    arriving: BTreeMap<i64, Arriving>,
    /// How long before an authorization runs out the server warns.
    warn_before: Micros,
    backpressure: Option<Backpressure>,
    pressure_warned: bool,
}

impl Connection {
    fn build(
        side: Side,
        offer: Offer,
        handlers: Vec<Box<dyn Handler>>,
        now: Micros,
    ) -> Result<Self, OfferRefusal> {
        // What is announced is what is handled: the offer's protocols are
        // rebuilt from the handlers, whatever it held.
        let mut announced = offer.without_protocols();
        let mut table = BTreeMap::new();
        for handler in handlers {
            let protocol = handler.offer();
            announced = announced.protocol(protocol.clone())?;
            table.insert(protocol.protocol, handler);
        }
        Ok(Self {
            side,
            wire: Wire::new(side, crate::wire::Config::new().tolerance(offer.tolerance)),
            offer: announced,
            handlers: table,
            state: State::AwaitingSession,
            negotiated: None,
            watermark: None,
            keepalive: (side == Side::Client).then(|| CLIENT_KEEPALIVE.as_micros() as Micros),
            last_activity: now,
            keepalive_ping: None,
            credentials: None,
            credentials_tried: false,
            request: None,
            request_id: None,
            authorize_id: None,
            authorizer: None,
            authorized: false,
            expires_at: None,
            warned_expiring: false,
            session_id: Uuid::default(),
            earliest_retained: 0,
            connected_at: now,
            session_requested_at: None,
            requests: BTreeMap::new(),
            arriving: BTreeMap::new(),
            warn_before: 60 * SECOND,
            backpressure: None,
            pressure_warned: false,
        })
    }

    /// A client's end, with the credentials it has before connecting, if any.
    pub fn client(
        offer: Offer,
        handlers: Vec<Box<dyn Handler>>,
        credentials: Option<Credentials>,
        now: Micros,
    ) -> Result<Self, OfferRefusal> {
        let mut connection = Self::build(Side::Client, offer, handlers, now)?;
        connection.credentials = credentials;
        Ok(connection)
    }

    /// A server's end for the session `session_id`. Without an authorizer,
    /// no authorization is asked.
    pub fn server(
        offer: Offer,
        handlers: Vec<Box<dyn Handler>>,
        authorizer: Option<Box<dyn Authorizer>>,
        session_id: Uuid,
        now: Micros,
    ) -> Result<Self, OfferRefusal> {
        let mut connection = Self::build(Side::Server, offer, handlers, now)?;
        connection.authorized = authorizer.is_none();
        connection.authorizer = authorizer;
        connection.session_id = session_id;
        Ok(connection)
    }

    /// Send-queue thresholds, for [`Connection::pressure`].
    pub fn with_backpressure(mut self, thresholds: Backpressure) -> Self {
        self.backpressure = Some(thresholds);
        self
    }

    /// Say something when the session has been quiet this long - a `Ping` -
    /// or never, with `None`. On by default for a client
    /// ([`CLIENT_KEEPALIVE`]), off for a server.
    pub fn set_keepalive(&mut self, period: Option<std::time::Duration>) {
        self.keepalive = period.map(|p| p.as_micros() as Micros);
    }

    /// How long the session may stay quiet before a keepalive `Ping`.
    pub fn keepalive(&self) -> Option<std::time::Duration> {
        self.keepalive
            .map(|p| std::time::Duration::from_micros(p.max(0) as u64))
    }

    /// How long before an authorization expires the server warns (60 s by default).
    pub fn warn_expiring_before(mut self, seconds: i64) -> Self {
        self.warn_before = seconds * SECOND;
        self
    }

    /// The `earliestRetainedChangeTime` this end announces.
    pub fn with_earliest_retained(mut self, time: Micros) -> Self {
        self.earliest_retained = time;
        self
    }

    /// Where the connection is.
    pub fn state(&self) -> State {
        self.state
    }

    /// How this end takes its peer's faults.
    pub fn tolerance(&self) -> crate::wire::Tolerance {
        self.offer.tolerance
    }

    /// What the session was opened with.
    pub fn negotiated(&self) -> Option<&Negotiated> {
        self.negotiated.as_ref()
    }

    /// The last `currentDateTime` heard from the peer (ADR-0003).
    pub fn watermark(&self) -> Option<Micros> {
        self.watermark
    }

    /// What this end announces.
    pub fn offer(&self) -> &Offer {
        &self.offer
    }

    /// The wire, for what the connection does not wrap - a multipart request.
    pub fn wire(&mut self) -> &mut Wire {
        &mut self.wire
    }

    /// Begin: a client sends its `RequestSession` (§5.2.1.1 step 1); a server
    /// waits for one.
    pub fn start(&mut self, now: Micros) -> Step {
        let mut step = Step::default();
        if self.side == Side::Client {
            self.send_request_session(now, &mut step);
        }
        step
    }

    fn send_request_session(&mut self, now: Micros, step: &mut Step) {
        let request = self.offer.request_session(now, self.earliest_retained);
        let out = self.wire.request(&request).expect("Core is always spoken");
        self.request_id = Some(out.header.message_id);
        self.request = Some(request);
        self.session_requested_at.get_or_insert(now);
        self.state = State::Negotiating;
        step.send.push(out);
    }

    /// Send a request, and start waiting for its response.
    pub fn request<M: EtpMessage>(&mut self, message: &M, now: Micros) -> Result<Outgoing, SendRefusal> {
        let out = self.wire.request(message)?;
        self.requests.insert(out.header.message_id, now);
        self.last_activity = now;
        Ok(out)
    }

    /// Send a request with options - an `Acknowledge` asked for (`0x10`), a
    /// header extension.
    pub fn request_with<M: EtpMessage>(
        &mut self,
        message: &M,
        options: &crate::wire::Options,
        now: Micros,
    ) -> Result<Outgoing, SendRefusal> {
        let out = self.wire.request_with(message, options)?;
        self.requests.insert(out.header.message_id, now);
        self.last_activity = now;
        Ok(out)
    }

    /// Send a request that leaves in **parts** - the message, then the
    /// `Chunk`s of its BLOBs - and start waiting for its response.
    ///
    /// One request, one id: the first part's, which every other part
    /// correlates to (§3.7.3.1 §6) and the response answers. The last part
    /// carries the FIN (§3.7.3.2 §3). With no parts it is [`Self::request`].
    pub fn request_parts<M: EtpMessage, P: EtpMessage>(
        &mut self,
        first: &M,
        parts: &[P],
        options: &crate::wire::Options,
        now: Micros,
    ) -> Result<Vec<Outgoing>, SendRefusal> {
        let Some((last, rest)) = parts.split_last() else {
            return Ok(vec![self.request_with(first, options, now)?]);
        };
        let mut multipart = self.wire.begin(M::PROTOCOL, true, options.clone());
        let mut out = Vec::with_capacity(parts.len() + 1);
        out.push(self.wire.part(&mut multipart, first)?);
        for part in rest {
            out.push(self.wire.part(&mut multipart, part)?);
        }
        out.extend(self.wire.last(&mut multipart, last)?);
        let id = out[0].header.message_id;
        self.requests.insert(id, now);
        self.last_activity = now;
        Ok(out)
    }

    /// Send a message that expects no response.
    pub fn notify<M: EtpMessage>(&mut self, message: &M) -> Result<Outgoing, SendRefusal> {
        self.wire.notify(message)
    }

    /// A `Ping`, carrying this end's clock.
    pub fn ping(&mut self, now: Micros) -> Outgoing {
        self.last_activity = now;
        let out = self
            .wire
            .request(&Ping {
                current_date_time: now,
            })
            .expect("Core is always spoken");
        // A Ping is a request like any other, and waits no longer: a peer
        // whose Pong never comes back correlated to it (a server's, say, correlated to
        // 0) left the client waiting for ever - `ETIMED_OUT` after
        // ResponseTimeoutPeriod, as for the rest (§3.3.2.3).
        self.requests.insert(out.header.message_id, now);
        out
    }

    /// Send credentials - for the first authorization, or to renew them
    /// before they run out (§5.2.2 row 8.4). `Authorize` carries the header
    /// value as it is, scheme included (§5.3.4).
    pub fn authorize(&mut self, credentials: &Credentials, now: Micros) -> Outgoing {
        let _ = now;
        self.credentials = Some(credentials.clone());
        self.credentials_tried = true;
        let out = self
            .wire
            .request(&Authorize {
                authorization: credentials.header().to_string(),
                supplemental_authorization: BTreeMap::new(),
            })
            .expect("Core is always spoken");
        self.authorize_id = Some(out.header.message_id);
        out
    }

    /// End the session: `CloseSession`, then the WebSocket closes.
    pub fn close(&mut self, reason: &str) -> Step {
        let mut step = Step::default();
        self.closing(reason, &mut step);
        step
    }

    fn closing(&mut self, reason: &str, step: &mut Step) {
        if self.state != State::Closed
            && let Ok(out) = self.wire.notify(&CloseSession {
                reason: reason.to_string(),
            })
        {
            step.send.push(out);
        }
        self.state = State::Closed;
        step.close = true;
        step.events.push(Event::Closed {
            reason: reason.to_string(),
        });
    }

    /// Close without `CloseSession` - for a session that never opened
    /// (§5.2.1.1 §2.a and step 1.a.i).
    fn drop_connection(&mut self, reason: &str, step: &mut Step) {
        self.state = State::Closed;
        step.close = true;
        step.events.push(Event::Closed {
            reason: reason.to_string(),
        });
    }

    /// Take in one complete WebSocket message.
    pub fn receive(&mut self, bytes: &[u8], now: Micros) -> Step {
        let mut step = Step::default();
        if self.state == State::Closed {
            return step;
        }
        self.last_activity = now;
        let received = self.wire.receive(bytes);
        let refused_open_session = self.side == Side::Client
            && self.state == State::Negotiating
            && received.delivery.is_none()
            && header_of(bytes).is_some_and(|h| (h.protocol, h.message_type) == (0, 2));
        if refused_open_session {
            // §5.2.1.1 §10.b: an OpenSession the client cannot process is
            // answered EREQUEST_DENIED, then CloseSession - whatever the wire
            // would have said about its bytes.
            let header = header_of(bytes).expect("checked above");
            let why = received
                .replies
                .iter()
                .find(|r| r.header.message_type == 1000)
                .and_then(|r| exception_text(&r.bytes))
                .unwrap_or_else(|| "the OpenSession could not be read".to_string());
            step.send.extend(
                received
                    .replies
                    .into_iter()
                    .filter(|r| r.header.message_type != 1000),
            );
            let incident = EtpError::EREQUEST_DENIED.with_detail(why);
            step.send.push(self.wire.exception(&header, &incident));
            step.events.push(Event::Refused(incident));
            self.closing("the OpenSession could not be processed", &mut step);
            return step;
        }
        step.send.extend(received.replies);
        if let Some(fault) = received.fault {
            step.events.push(Event::Fault(fault));
        }
        if let Some((request, incident)) = received.refused_answer {
            self.requests.remove(&request);
            self.arriving.remove(&request);
            step.events.push(Event::AnswerRefused { request, incident });
        }
        if let Some(delivery) = received.delivery {
            self.track(&delivery, now);
            self.dispatch(delivery, now, &mut step);
        }
        step
    }

    /// Keep the timers of what arrives.
    fn track(&mut self, delivery: &Delivery, now: Micros) {
        match delivery.place {
            Place::Answers { request } => {
                self.requests.remove(&request);
                if delivery.last {
                    self.arriving.remove(&request);
                } else {
                    self.arriving.insert(
                        request,
                        Arriving {
                            protocol: delivery.header.protocol,
                            last_part: now,
                        },
                    );
                }
            }
            Place::Opens if !delivery.last => {
                self.arriving.insert(
                    delivery.header.message_id,
                    Arriving {
                        protocol: delivery.header.protocol,
                        last_part: now,
                    },
                );
            }
            Place::Continues { first } => {
                if delivery.last {
                    self.arriving.remove(&first);
                } else if let Some(a) = self.arriving.get_mut(&first) {
                    a.last_part = now;
                }
            }
            _ => {}
        }
    }

    fn dispatch(&mut self, delivery: Delivery, now: Micros, step: &mut Step) {
        let protocol = delivery.header.protocol;

        // MaxConcurrentMultipart applies to what this end receives (§3.7.3.1.1.1 §1.a).
        if delivery.place == Place::Opens
            && !delivery.last
            && protocol != 0
            && let Some(limit) = self.offer.endpoint.long(names::MAX_CONCURRENT_MULTIPART)
            && self.wire.open_in(protocol) as i64 > limit
        {
            let id = delivery.header.message_id;
            let incident = EtpError::EMULTIPART_CANCELLED.with_detail(format!(
                "more than {limit} multipart messages at once in protocol {protocol} (MaxConcurrentMultipart)"
            ));
            step.send.push(self.wire.refuse(&delivery, &incident));
            self.wire.abandon(id);
            self.arriving.remove(&id);
            return;
        }

        let Body::Message(message) = &delivery.body else {
            return self.pass_to_handler(delivery, step);
        };
        match message {
            Message::Ping(ping) => {
                self.watermark = Some(ping.current_date_time);
                if let Ok(out) = self.wire.respond(
                    &delivery,
                    &Pong {
                        current_date_time: now,
                    },
                ) {
                    step.send.push(out);
                }
            }
            Message::Pong(pong) => {
                self.watermark = Some(pong.current_date_time);
                // A keepalive's answer is the connection's own business.
                if self
                    .keepalive_ping
                    .is_some_and(|id| delivery.place == Place::Answers { request: id })
                {
                    self.keepalive_ping = None;
                } else {
                    step.events.push(Event::Delivered(delivery));
                }
            }
            Message::CloseSession(close) => {
                let reason = close.reason.clone();
                self.state = State::Closed;
                step.close = true;
                step.events.push(Event::Closed { reason });
            }
            Message::RequestSession(request) if self.side == Side::Server => {
                let request = request.clone();
                self.server_request_session(&delivery, request, now, step);
            }
            Message::OpenSession(open) if self.side == Side::Client => {
                let open = open.clone();
                self.client_open_session(&delivery, open, step);
            }
            Message::Authorize(authorize) if self.side == Side::Server => {
                let verdict = match &mut self.authorizer {
                    Some(authorizer) => authorizer.authorize(&authorize.authorization, now),
                    None => Authorization::Granted { expires_at: None },
                };
                let response = match verdict {
                    Authorization::Granted { expires_at } => {
                        self.authorized = true;
                        self.expires_at = expires_at;
                        self.warned_expiring = false;
                        AuthorizeResponse {
                            success: true,
                            challenges: Vec::new(),
                        }
                    }
                    Authorization::Challenge(challenges) => AuthorizeResponse {
                        success: false,
                        challenges,
                    },
                };
                if let Ok(out) = self.wire.respond(&delivery, &response) {
                    step.send.push(out);
                }
            }
            Message::Authorize(_) => {
                let incident = EtpError::EINVALID_STATE.with_detail(
                    "this client did not ask the server to authorize (serverAuthorizationRequired is false)",
                );
                step.send.push(self.wire.refuse(&delivery, &incident));
            }
            Message::AuthorizeResponse(response) if self.side == Side::Client => {
                if response.success {
                    if self.state != State::Open {
                        self.send_request_session(now, step);
                    }
                } else {
                    self.state = State::Authorizing;
                    step.events.push(Event::AuthorizationNeeded {
                        challenges: response.challenges.clone(),
                    });
                }
            }
            Message::ProtocolException(_) => self.exception(delivery, step),
            Message::RequestSession(_) | Message::OpenSession(_) | Message::AuthorizeResponse(_) => {
                let incident = EtpError::EINVALID_MESSAGETYPE
                    .with_detail("this side of the connection never receives that message");
                step.send.push(self.wire.refuse(&delivery, &incident));
            }
            _ if protocol == 0 => step.events.push(Event::Delivered(delivery)),
            _ => self.pass_to_handler(delivery, step),
        }
    }

    fn pass_to_handler(&mut self, delivery: Delivery, step: &mut Step) {
        if !matches!(delivery.place, Place::Opens | Place::Continues { .. }) {
            // Responses to our requests, acknowledgements, exceptions: the
            // application's.
            step.events.push(Event::Delivered(delivery));
            return;
        }
        let Some(handler) = self.handlers.get_mut(&delivery.header.protocol) else {
            let incident = EtpError::EUNSUPPORTED_PROTOCOL.with_detail(format!(
                "protocol {} has no handler here",
                delivery.header.protocol
            ));
            step.send.push(self.wire.refuse(&delivery, &incident));
            return;
        };
        let mut responder = Responder {
            wire: &mut self.wire,
            out: &mut step.send,
        };
        match handler.handle(&delivery, &mut responder) {
            Ok(Handled::Answered) => {}
            Ok(Handled::Forward) => step.events.push(Event::Delivered(delivery)),
            Err(Declined(why)) => {
                let incident = EtpError::ENOTSUPPORTED.with_detail(why);
                step.send.push(self.wire.refuse(&delivery, &incident));
            }
        }
    }

    fn exception(&mut self, delivery: Delivery, step: &mut Step) {
        let Some(Exception::Single(error)) = delivery.exception() else {
            step.events.push(Event::Delivered(delivery));
            return;
        };
        let during_negotiation = self.side == Side::Client
            && delivery.place
                == Place::Answers {
                    request: self.request_id.unwrap_or(-1),
                };
        let to_authorize = self.side == Side::Client
            && self
                .authorize_id
                .is_some_and(|id| delivery.place == Place::Answers { request: id });
        let incident = || {
            crate::wire::ErrorRegistry::new()
                .incident(error.code, Some(error.message.clone()))
                .unwrap_or_else(|| EtpError::EREQUEST_DENIED.with_detail(error.message.clone()))
        };
        if (during_negotiation || to_authorize) && error.code == EtpError::EAUTHORIZATION_REQUIRED.code() {
            // CASE 1 with credentials not yet tried; CASE 2 - an empty
            // Authorize asking for the challenges - otherwise (§5.2.1.1 steps 3-4).
            self.state = State::Authorizing;
            let out = match (&self.credentials, self.credentials_tried) {
                (Some(credentials), false) => {
                    let credentials = credentials.clone();
                    self.authorize(&credentials, 0)
                }
                _ => {
                    let out = self
                        .wire
                        .request(&Authorize {
                            authorization: String::new(),
                            supplemental_authorization: BTreeMap::new(),
                        })
                        .expect("Core is always spoken");
                    self.authorize_id = Some(out.header.message_id);
                    out
                }
            };
            step.send.push(out);
            return;
        }
        if during_negotiation {
            // The server refused the session: it drops the connection.
            step.events.push(Event::Refused(incident()));
            self.drop_connection(&error.message, step);
            return;
        }
        if delivery.place == Place::Unattached && error.code == EtpError::EAUTHORIZATION_EXPIRING.code() {
            step.events.push(Event::AuthorizationExpiring);
            return;
        }
        step.events.push(Event::Delivered(delivery));
    }

    fn server_request_session(
        &mut self,
        delivery: &Delivery,
        request: RequestSession,
        now: Micros,
        step: &mut Step,
    ) {
        if self.state == State::Open {
            let incident = EtpError::EINVALID_STATE.with_detail("the session is open already");
            step.send.push(self.wire.refuse(delivery, &incident));
            return;
        }
        self.session_requested_at.get_or_insert(now);
        self.watermark = Some(request.current_date_time);
        if !self.authorized {
            // §5.2.1.1 step 2.b. The specification prints "(28)" there; the
            // code named EAUTHORIZATION_REQUIRED is 27 (§24.3).
            self.state = State::Authorizing;
            let incident = EtpError::EAUTHORIZATION_REQUIRED
                .with_detail("authorize first, then send RequestSession again");
            step.send.push(self.wire.refuse(delivery, &incident));
            return;
        }
        match open_session(
            &self.offer,
            &request,
            self.session_id,
            now,
            self.earliest_retained,
        ) {
            Ok((open, negotiated)) => {
                if let Ok(out) = self.wire.respond(delivery, &open) {
                    step.send.push(out);
                }
                self.opened(negotiated, step);
            }
            Err(incident) => {
                // No session was established: the connection drops, and no
                // CloseSession is sent (§5.2.1.1 §9.c).
                step.send.push(self.wire.refuse(delivery, &incident));
                step.events.push(Event::Refused(incident.clone()));
                self.drop_connection(&incident.message(), step);
            }
        }
    }

    fn client_open_session(&mut self, delivery: &Delivery, open: OpenSession, step: &mut Step) {
        let request = self
            .request
            .clone()
            .expect("an OpenSession answers our RequestSession");
        self.watermark = Some(open.current_date_time);
        match accept_open_session(&self.offer, &request, &open) {
            Ok(negotiated) => self.opened(negotiated, step),
            Err(incident) => {
                step.send.push(self.wire.exception(&delivery.header, &incident));
                step.events.push(Event::Refused(incident));
                self.closing("the OpenSession could not be processed", step);
            }
        }
    }

    fn opened(&mut self, negotiated: Negotiated, step: &mut Step) {
        self.wire
            .reconfigure(negotiated.wire_config().tolerance(self.offer.tolerance));
        let mut problems: Vec<Problem> = negotiated.theirs.problems().to_vec();
        for protocol in negotiated.protocols.values() {
            problems.extend(protocol.theirs.problems().iter().cloned());
        }
        // What the peer got wrong reaches the log whatever the application
        // does with the session: tolerated is not unseen.
        let peer = &negotiated.peer_application.0;
        for deviation in &negotiated.deviations {
            log::warn!(target: "retp_proto", "{peer}: {deviation}");
        }
        for problem in &problems {
            log::warn!(target: "retp_proto", "{peer}: capability {problem}");
        }
        self.negotiated = Some(negotiated);
        self.state = State::Open;
        step.events.push(Event::Opened);
        if !problems.is_empty() {
            step.events.push(Event::CapabilityProblems(problems));
        }
    }

    /// Let time pass: timeouts, authorization expiry.
    pub fn tick(&mut self, now: Micros) -> Step {
        let mut step = Step::default();
        if self.state == State::Closed {
            return step;
        }
        let ours = |name: &str, fallback: i64| self.offer.endpoint.long(name).unwrap_or(fallback) * SECOND;
        let request_session_timeout = ours(names::REQUEST_SESSION_TIMEOUT_PERIOD, 45);
        let establishment_timeout = ours(names::SESSION_ESTABLISHMENT_TIMEOUT_PERIOD, 60);
        let response_timeout = ours(names::RESPONSE_TIMEOUT_PERIOD, 300);
        let gap = ours(names::MULTIPART_MESSAGE_TIMEOUT_PERIOD, 60);

        // No RequestSession in time (server), no session in time (both).
        if self.side == Side::Server
            && self.session_requested_at.is_none()
            && now - self.connected_at > request_session_timeout
        {
            step.send.push(
                self.wire
                    .unattached(&EtpError::ETIMED_OUT.with_detail("no RequestSession arrived in time")),
            );
            self.drop_connection("no RequestSession arrived in time", &mut step);
            return step;
        }
        if self.state != State::Open
            && let Some(since) = self.session_requested_at
            && now - since > establishment_timeout
        {
            step.send
                .push(self.wire.unattached(
                    &EtpError::ETIMED_OUT.with_detail("the session was not established in time"),
                ));
            self.drop_connection("the session was not established in time", &mut step);
            return step;
        }

        // Our requests with no response at all.
        let late: Vec<i64> = self
            .requests
            .iter()
            .filter(|(_, sent)| now - **sent > response_timeout)
            .map(|(id, _)| *id)
            .collect();
        for request in late {
            self.requests.remove(&request);
            self.wire.abandon(request);
            step.events.push(Event::TimedOut {
                request,
                incident: EtpError::ETIMED_OUT.with_detail(format!(
                    "no response within ResponseTimeoutPeriod ({} s)",
                    response_timeout / SECOND
                )),
            });
        }

        // Multipart messages whose parts stopped arriving.
        let stalled: Vec<(i64, Arriving)> = self
            .arriving
            .iter()
            .filter(|(_, a)| now - a.last_part > gap)
            .map(|(k, a)| (*k, *a))
            .collect();
        for (key, arriving) in stalled {
            self.arriving.remove(&key);
            self.wire.abandon(key);
            let incident = EtpError::ETIMED_OUT.with_detail(format!(
                "no part for longer than MultipartMessageTimeoutPeriod ({} s)",
                gap / SECOND
            ));
            step.send
                .push(self.wire.exception_to(arriving.protocol, key, &incident));
            if self.side.may_use(key) {
                step.events.push(Event::TimedOut {
                    request: key,
                    incident,
                });
            }
        }

        // The client's authorization, as the server sees it (§3.13.5).
        if let Some(expires) = self.expires_at {
            if now >= expires {
                step.send
                    .push(self.wire.unattached(&EtpError::EAUTHORIZATION_EXPIRED.incident()));
                self.closing("the authorization expired", &mut step);
                return step;
            }
            if now >= expires - self.warn_before && !self.warned_expiring {
                self.warned_expiring = true;
                step.send.push(
                    self.wire
                        .unattached(&EtpError::EAUTHORIZATION_EXPIRING.with_detail(format!(
                            "the authorization runs out in {} s",
                            (expires - now) / SECOND
                        ))),
                );
            }
        }

        // A session nothing has crossed for a period: a `Ping`, one at a
        // time - its `Pong` refreshes the watermark, and one that never comes
        // times out like any request.
        if let Some(period) = self.keepalive
            && self.state == State::Open
            && self
                .keepalive_ping
                .is_none_or(|id| !self.requests.contains_key(&id))
            && now - self.last_activity >= period
        {
            let out = self.ping(now);
            self.requests.insert(out.header.message_id, now);
            self.keepalive_ping = Some(out.header.message_id);
            log::debug!(target: "retp_proto", "keepalive: quiet for {} s", period / SECOND);
            step.send.push(out);
        }
        if !step.send.is_empty() {
            self.last_activity = now;
        }
        step
    }

    /// When [`Self::tick`] has something to do at the earliest, by the clock
    /// it is given: a timeout running out, the authorization's warning or
    /// expiry, the keepalive `Ping`. `None` when nothing is due - a closed
    /// session, or an open one with no request out and no keepalive.
    ///
    /// For whoever drives the connection without a fixed interval: sleep
    /// until this moment (or until a message comes), then `tick`. It moves
    /// with every `receive`, `tick` and message sent, so it is asked again
    /// after each. The conditions are those of `tick`, one by one; a moment
    /// already past means *now*.
    pub fn next_deadline(&self) -> Option<Micros> {
        if self.state == State::Closed {
            return None;
        }
        let ours = |name: &str, fallback: i64| self.offer.endpoint.long(name).unwrap_or(fallback) * SECOND;
        let mut due: Option<Micros> = None;
        let mut at = |moment: Micros| due = Some(due.map_or(moment, |d| d.min(moment)));

        // `tick` acts once `now - since > period`: one microsecond after.
        if self.side == Side::Server && self.session_requested_at.is_none() {
            at(self.connected_at + ours(names::REQUEST_SESSION_TIMEOUT_PERIOD, 45) + 1);
        }
        if self.state != State::Open
            && let Some(since) = self.session_requested_at
        {
            at(since + ours(names::SESSION_ESTABLISHMENT_TIMEOUT_PERIOD, 60) + 1);
        }
        let response_timeout = ours(names::RESPONSE_TIMEOUT_PERIOD, 300);
        if let Some(oldest) = self.requests.values().min() {
            at(oldest + response_timeout + 1);
        }
        let gap = ours(names::MULTIPART_MESSAGE_TIMEOUT_PERIOD, 60);
        if let Some(oldest) = self.arriving.values().map(|a| a.last_part).min() {
            at(oldest + gap + 1);
        }
        // `tick` acts once `now >=` these.
        if let Some(expires) = self.expires_at {
            at(expires);
            if !self.warned_expiring {
                at(expires - self.warn_before);
            }
        }
        if let Some(period) = self.keepalive
            && self.state == State::Open
            && self
                .keepalive_ping
                .is_none_or(|id| !self.requests.contains_key(&id))
        {
            at(self.last_activity + period);
        }
        due
    }

    /// Report the send queue: warn, then give up (§3.13.4).
    pub fn pressure(&mut self, queued: usize) -> Step {
        let mut step = Step::default();
        let Some(thresholds) = self.backpressure else {
            return step;
        };
        if queued >= thresholds.limit_at {
            step.send.push(
                self.wire
                    .unattached(&EtpError::EBACKPRESSURE_LIMIT_EXCEEDED.incident()),
            );
            self.closing("the send queue is exhausted", &mut step);
        } else if queued >= thresholds.warn_at {
            if !self.pressure_warned {
                self.pressure_warned = true;
                step.send
                    .push(self.wire.unattached(&EtpError::EBACKPRESSURE_WARNING.incident()));
            }
        } else {
            self.pressure_warned = false;
        }
        step
    }
}

fn header_of(bytes: &[u8]) -> Option<MessageHeader> {
    MessageHeader::read(&mut Reader::new(bytes)).ok()
}

/// The message of the single error of an exception's bytes.
fn exception_text(bytes: &[u8]) -> Option<String> {
    let mut input = Reader::new(bytes);
    MessageHeader::read(&mut input).ok()?;
    let body = &bytes[bytes.len() - input.remaining()..];
    let exception = retp_types::protocol::core::ProtocolException::from_avro(body).ok()?;
    exception.error.map(|e| e.message)
}
