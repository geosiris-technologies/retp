//! The blocking client: an [`Engine`], a transport and a clock, driven.

use std::fmt;
use std::time::Duration;

use retp_avro::EtpMessage;
use retp_proto::connection::{Event, Handler, Resumable, State};
use retp_proto::credentials::Credentials;
use retp_proto::negotiate::{Negotiated, Offer, OfferRefusal};
use retp_proto::wire::{Delivery, Incident, SendRefusal};

use crate::engine::Engine;
use crate::{Clock, Transport, TransportError};

/// How the client waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// How long one wait on the transport lasts before the connection is
    /// given the time - the grain of every timeout.
    pub poll: Duration,
    /// How many requests [`Client::call_all`] keeps in flight. One sends
    /// them one after the other.
    pub at_once: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(250),
            at_once: 4,
        }
    }
}

/// Why the client could not do what was asked.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientError {
    /// The offer is not one a session can be asked with.
    Offer(OfferRefusal),
    /// The transport failed.
    Transport(TransportError),
    /// The session could not open.
    Refused(Incident),
    /// The server wants credentials; the challenges say where to get them.
    AuthorizationNeeded {
        /// `WWW-Authenticate` style challenges.
        challenges: Vec<String>,
    },
    /// The session closed.
    Closed {
        /// Why, as said by whoever closed it.
        reason: String,
    },
    /// The message could not be sent.
    Send(SendRefusal),
    /// The request is one the server would refuse - a URI that is not
    /// canonical where the specification requires one -, so it was not sent.
    /// The session is untouched.
    Invalid(Incident),
    /// No response in time - a named refusal, never a wait that does not end
    /// (§3.3.2.3).
    TimedOut {
        /// The request.
        request: i64,
        /// The error, `ETIMED_OUT`.
        incident: Incident,
    },
    /// The answer arrived and could not be taken - unreadable, or against a
    /// rule this client holds to even when tolerant. The request is over.
    AnswerRefused {
        /// The request.
        request: i64,
        /// Why.
        incident: Incident,
    },
    /// The connection fell while a request was out. Its outcome is **unknown**:
    /// neither a success (data may be lost) nor a failure (a retry may write
    /// twice) - issue #201.
    Interrupted {
        /// The request.
        request: i64,
        /// Whether it left.
        sent: bool,
        /// Whether its end was heard - never, here.
        confirmed: bool,
        /// What failed.
        why: TransportError,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Offer(why) => write!(f, "the offer is refused: {why}"),
            ClientError::Transport(why) => write!(f, "transport: {why}"),
            ClientError::Refused(incident) => write!(f, "the session could not open: {incident}"),
            ClientError::AuthorizationNeeded { challenges } => {
                write!(
                    f,
                    "the server wants credentials; its challenges: {}",
                    challenges.join(" | ")
                )
            }
            ClientError::Closed { reason } => write!(f, "the session is closed: {reason}"),
            ClientError::Send(why) => write!(f, "not sent: {why}"),
            ClientError::Invalid(incident) => write!(f, "not sent, the server would refuse it: {incident}"),
            ClientError::TimedOut { request, incident } => write!(f, "request {request}: {incident}"),
            ClientError::AnswerRefused { request, incident } => {
                write!(
                    f,
                    "the answer to request {request} could not be taken: {incident}"
                )
            }
            ClientError::Interrupted {
                request, sent, why, ..
            } => write!(
                f,
                "request {request} was {} when the connection fell ({why}); its outcome is unknown",
                if *sent { "sent" } else { "not sent" }
            ),
        }
    }
}

impl ClientError {
    /// Whether the connection is gone, so that nothing more can be asked in
    /// this session and a new one has to be opened. A refusal, a request that
    /// timed out or an answer refused leave the session open.
    pub fn is_connection_lost(&self) -> bool {
        matches!(
            self,
            ClientError::Transport(_) | ClientError::Closed { .. } | ClientError::Interrupted { .. }
        )
    }
}

impl std::error::Error for ClientError {}

impl From<TransportError> for ClientError {
    fn from(why: TransportError) -> Self {
        ClientError::Transport(why)
    }
}

impl From<SendRefusal> for ClientError {
    fn from(why: SendRefusal) -> Self {
        ClientError::Send(why)
    }
}

/// An `Acknowledge` the server sent for a request of ours (§3.5.4.2 step 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acknowledged {
    /// The request acknowledged.
    pub message_id: i64,
    /// Its message, `Store.GetDataObjects`.
    pub name: &'static str,
    /// How long after it left.
    pub after: Duration,
}

/// An ETP client over a transport - blocking: it waits on its transport, and
/// hands what comes to its [`Engine`], which does the rest.
pub struct Client<T: Transport, C: Clock> {
    engine: Engine,
    transport: T,
    clock: C,
    settings: Settings,
}

impl<T: Transport, C: Clock> Client<T, C> {
    /// Open a session over `transport`: `RequestSession`, the authorization
    /// if the server asks and `credentials` were given, `OpenSession`.
    pub fn open(
        transport: T,
        clock: C,
        offer: Offer,
        handlers: Vec<Box<dyn Handler>>,
        credentials: Option<Credentials>,
        settings: Settings,
    ) -> Result<Self, ClientError> {
        let engine = Engine::start(offer, handlers, credentials, clock.now())?;
        Self::opening(engine, transport, clock, settings)
    }

    /// Open a session over `transport` by asking back the session `lost`
    /// (OSDU's `ResumeSession`, ADR-0003) - on a server that gives it back, the
    /// same session, its id and what it negotiated; on one that does not, a new
    /// session, opened at once ([`Client::resumed`] says which). `offer` and
    /// `handlers` are those `lost` was opened with.
    pub fn resume(
        transport: T,
        clock: C,
        offer: Offer,
        handlers: Vec<Box<dyn Handler>>,
        credentials: Option<Credentials>,
        settings: Settings,
        lost: Resumable,
    ) -> Result<Self, ClientError> {
        let engine = Engine::resume(offer, handlers, credentials, lost, clock.now())?;
        Self::opening(engine, transport, clock, settings)
    }

    fn opening(engine: Engine, transport: T, clock: C, settings: Settings) -> Result<Self, ClientError> {
        let mut client = Self {
            engine,
            transport,
            clock,
            settings,
        };
        client.flush()?;
        loop {
            if let Some(opened) = client.engine.opened() {
                if client.engine.take_close() {
                    client.transport.close();
                }
                return opened.map(|()| client);
            }
            client.pump(client.settings.poll)?;
        }
    }

    /// What the session was opened with.
    pub fn negotiated(&self) -> &Negotiated {
        self.engine
            .negotiated()
            .expect("a client exists once its session is open")
    }

    /// What to ask this session back with, if its connection is lost
    /// ([`Client::resume`]).
    pub fn resumable(&self) -> Resumable {
        self.engine
            .resumable()
            .expect("a client exists once its session is open")
    }

    /// Whether the session was given back by `ResumeSession`, not opened anew.
    pub fn resumed(&self) -> bool {
        self.engine.resumed()
    }

    /// How this client takes the server's faults.
    pub fn tolerance(&self) -> retp_proto::wire::Tolerance {
        self.engine.tolerance()
    }

    /// Ask for an `Acknowledge` with every request from now on (flag `0x10`,
    /// §3.5.4.2): the server says it received the request before it answers
    /// it. Off by default - one more message per request.
    pub fn ask_acknowledge(&mut self, on: bool) {
        self.engine.ask_acknowledge(on);
    }

    /// Whether requests ask for an `Acknowledge`.
    pub fn asks_acknowledge(&self) -> bool {
        self.engine.asks_acknowledge()
    }

    /// The acknowledgements heard since the last call, oldest first.
    pub fn acknowledgements(&mut self) -> Vec<Acknowledged> {
        self.engine.acknowledgements()
    }

    /// The last `currentDateTime` heard from the server.
    pub fn watermark(&self) -> Option<i64> {
        self.engine.watermark()
    }

    /// Send a request and gather **every** message of its answer, up to the
    /// one that carries the FIN - responses, and exceptions, in order.
    pub fn call<M: EtpMessage>(&mut self, request: &M) -> Result<Vec<Delivery>, ClientError> {
        let id = self.engine.request(request, self.clock.now())?;
        self.wait_for(id)
    }

    /// [`Self::call`] for a request that leaves in parts - the message and
    /// the `Chunk`s of its BLOBs.
    pub fn call_parts<M: EtpMessage, P: EtpMessage>(
        &mut self,
        first: &M,
        parts: &[P],
    ) -> Result<Vec<Delivery>, ClientError> {
        let id = self.engine.request_parts(first, parts, self.clock.now())?;
        self.wait_for(id)
    }

    /// Send a message that expects no answer - a `StartStreaming` - and
    /// flush it.
    pub fn tell<M: EtpMessage>(&mut self, message: &M) -> Result<(), ClientError> {
        self.engine.tell(message)?;
        self.flush()
    }

    /// A `Ping`, and the time its `Pong` took.
    pub fn ping(&mut self) -> Result<Duration, ClientError> {
        let sent_at = self.clock.now();
        let id = self.engine.ping(sent_at)?;
        self.wait_for(id)?;
        let micros = (self.clock.now() - sent_at).max(0);
        Ok(Duration::from_micros(micros as u64))
    }

    /// The settings this client runs with.
    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// How many requests [`Self::call_all`] keeps in flight from now on.
    pub fn set_at_once(&mut self, requests: usize) {
        self.settings.at_once = requests.max(1);
    }

    /// Send these requests with at most `settings.at_once` in flight, and
    /// answer each in order.
    ///
    /// **One thread, several requests out**: the engine holds a call per
    /// id, so a second request goes on the wire before the first is
    /// answered, and the wire correlates each answer to its own. What a
    /// blocking client could not do was *wait for two at once* - this waits
    /// for a window of them, refilling it as answers come. A request the
    /// engine will not send is answered with its refusal, and the others go.
    pub fn call_all<M: EtpMessage>(&mut self, requests: &[M]) -> Vec<Result<Vec<Delivery>, ClientError>> {
        let window = self.settings.at_once.max(1);
        let mut outcomes: Vec<Option<Result<Vec<Delivery>, ClientError>>> =
            (0..requests.len()).map(|_| None).collect();
        let mut pending: Vec<(usize, i64)> = Vec::new();
        let mut next = 0;
        loop {
            // Fill the window.
            while pending.len() < window && next < requests.len() {
                match self.engine.request(&requests[next], self.clock.now()) {
                    Ok(id) => pending.push((next, id)),
                    Err(why) => outcomes[next] = Some(Err(why)),
                }
                next += 1;
            }
            if pending.is_empty() {
                break;
            }
            let _ = self.flush();
            // Take what is answered; pump once when nothing is.
            let mut taken = false;
            pending.retain(|(index, id)| match self.engine.take_answer(*id) {
                Some(answer) => {
                    outcomes[*index] = Some(answer);
                    taken = true;
                    false
                }
                None => true,
            });
            if taken {
                continue;
            }
            if let Err(error) = self.pump(self.settings.poll) {
                // The connection is gone: every pending request has its
                // interruption in the engine, or gets this error.
                for (index, id) in pending.drain(..) {
                    outcomes[index] = Some(self.engine.take_answer(id).unwrap_or(Err(error.clone())));
                }
                for outcome in &mut outcomes[next..] {
                    *outcome = Some(Err(error.clone()));
                }
                break;
            }
        }
        outcomes
            .into_iter()
            .map(|one| one.expect("every request has an outcome"))
            .collect()
    }

    /// Keep the messages of `protocol` the peer sends on its own - a
    /// notification, a channel's data - for [`Self::events`], apart from
    /// everything else that arrives unasked. What a store subscribed to
    /// (StoreNotification, ChannelSubscribe) reads its stream with.
    pub fn subscribe(&mut self, protocol: i32) {
        self.engine.subscribe(protocol);
    }

    /// Stop keeping them: what arrives afterwards goes with the unsolicited.
    pub fn unsubscribe(&mut self, protocol: i32) {
        self.engine.unsubscribe(protocol);
    }

    /// What arrived for a subscribed protocol - after giving the transport
    /// `wait` to bring some when nothing is waiting. Empty is *nothing yet*,
    /// not *over*: a stream ends when the peer says so, in its own messages.
    pub fn events(&mut self, protocol: i32, wait: Duration) -> Result<Vec<Event>, ClientError> {
        let held = self.engine.take_events(protocol);
        if !held.is_empty() {
            return Ok(held);
        }
        self.pump(wait)?;
        Ok(self.engine.take_events(protocol))
    }

    /// What arrived unasked since the last call: notifications, faults.
    pub fn unsolicited(&mut self) -> Vec<Event> {
        self.engine.unsolicited()
    }

    /// End the session: `CloseSession`, then the connection closes.
    pub fn close(mut self, reason: &str) -> Result<(), ClientError> {
        self.engine.close(reason);
        let sent = self.flush();
        self.transport.close();
        sent
    }

    /// Let `wait` go by with no request out, **listening**: the transport
    /// answers its own pings (a WebSocket server that is not answered closes
    /// the connection - some servers do after twenty seconds), a keepalive `Ping`
    /// goes out when one is due, and what arrives unasked is kept for
    /// [`Client::unsolicited`]. What a shell does between two commands.
    pub fn idle(&mut self, wait: Duration) -> Result<(), ClientError> {
        let start = self.clock.now();
        loop {
            let quiet = self.pump(wait)? == 0;
            if quiet && self.clock.now() - start >= wait.as_micros() as i64 {
                return Ok(());
            }
        }
    }

    /// Say something when the session has been quiet this long, or never -
    /// on by default ([`retp_proto::connection::CLIENT_KEEPALIVE`]).
    pub fn set_keepalive(&mut self, period: Option<Duration>) {
        self.engine.set_keepalive(period);
    }

    /// Send what is to be sent, then listen until the answer to `id` is over.
    /// A transport that fails on the way ends it as the engine says -
    /// [`ClientError::Interrupted`], sent or not.
    fn wait_for(&mut self, id: i64) -> Result<Vec<Delivery>, ClientError> {
        let _ = self.flush();
        loop {
            if let Some(answer) = self.engine.take_answer(id) {
                return answer;
            }
            if let Err(error) = self.pump(self.settings.poll) {
                return self.engine.take_answer(id).unwrap_or(Err(error));
            }
        }
    }

    /// Listen once, for `wait` at most: a message is taken in, or the time
    /// passes and the connection is told; then what is to be sent goes. How
    /// many things happened for whoever uses the client.
    fn pump(&mut self, wait: Duration) -> Result<usize, ClientError> {
        if self.engine.state() == State::Closed {
            return Err(ClientError::Closed {
                reason: "the session is closed".into(),
            });
        }
        if let Some(why) = self.engine.is_lost() {
            return Err(ClientError::Transport(why.clone()));
        }
        let received = match self.transport.receive(wait) {
            Ok(received) => received,
            Err(why) => {
                self.engine.lost(why.clone(), &[]);
                return Err(ClientError::Transport(why));
            }
        };
        let now = self.clock.now();
        let happened = match received {
            Some(bytes) => self.engine.receive(&bytes, now),
            None => self.engine.tick(now),
        };
        self.flush()?;
        Ok(happened)
    }

    /// Put the engine's outbox on the transport, and close it if asked. A
    /// send that fails loses the transport: what had not left is known to be
    /// unsent.
    fn flush(&mut self) -> Result<(), ClientError> {
        let outbound = self.engine.take_outbound();
        for (i, out) in outbound.iter().enumerate() {
            if let Err(why) = self.transport.send(&out.bytes) {
                let unsent: Vec<i64> = outbound[i..].iter().filter_map(|o| o.request).collect();
                self.engine.lost(why.clone(), &unsent);
                return Err(ClientError::Transport(why));
            }
        }
        if self.engine.take_close() {
            self.transport.close();
        }
        Ok(())
    }
}
