//! The client without a transport or a clock: bytes in, bytes out, answers
//! gathered - so that one client serves a program that can block and a front end
//! that cannot.
//!
//! [`Connection`] already takes bytes and the time and gives back what to send;
//! what a client adds on top is bookkeeping: which request an answer belongs
//! to, when a request is over, what arrived unasked, which acknowledgements
//! came. That is this. Who **waits** is someone else's business:
//!
//! - the blocking [`Client`](crate::Client) waits on its transport and feeds
//!   the engine what comes;
//! - the [`AsyncClient`](crate::AsyncClient) is fed by whoever holds the
//!   socket - a browser's `onmessage` - and wakes the futures whose answer
//!   arrived.
//!
//! Several requests can be out at once: each answer is gathered under its
//! own request, whatever order they come back in.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use retp_avro::EtpMessage;
use retp_proto::connection::{Connection, Event, Handler, State, Step};
use retp_proto::credentials::Credentials;
use retp_proto::negotiate::{Negotiated, Offer};
use retp_proto::wire::{Delivery, Options, Place, Tolerance};

use crate::TransportError;
use crate::client::{Acknowledged, ClientError};

/// A message to put on the transport, and the request it is, if one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outbound {
    /// The whole WebSocket message.
    pub bytes: Vec<u8>,
    /// The request this message is - so that a message that could not be
    /// sent is known to be unsent (#201).
    pub request: Option<i64>,
}

/// One request out: what came of its answer so far.
struct Call {
    deliveries: Vec<Delivery>,
    /// Over at its first message - a `Ping`'s `Pong`, whatever its flags.
    single: bool,
    outcome: Option<Result<Vec<Delivery>, ClientError>>,
}

/// The client's side of a session, driven from outside.
pub struct Engine {
    connection: Connection,
    outbox: Vec<Outbound>,
    close_asked: bool,
    /// `None` while the session opens; then how it went.
    opening: Option<Result<(), ClientError>>,
    calls: BTreeMap<i64, Call>,
    /// The transport is gone, and why.
    lost: Option<TransportError>,
    unsolicited: VecDeque<Event>,
    /// What arrives unasked for these protocols is kept apart, per
    /// protocol, for whoever subscribed - a stream read on its own.
    subscriptions: BTreeMap<i32, VecDeque<Event>>,
    ask_acknowledge: bool,
    awaiting_ack: BTreeMap<i64, (&'static str, i64)>,
    acknowledged: Vec<Acknowledged>,
}

impl Engine {
    /// A client's session, started: its `RequestSession` is in the outbox.
    /// The authorization, if the server asks and `credentials` were given,
    /// follows by itself.
    pub fn start(
        offer: Offer,
        handlers: Vec<Box<dyn Handler>>,
        credentials: Option<Credentials>,
        now: i64,
    ) -> Result<Self, ClientError> {
        let connection = Connection::client(offer, handlers, credentials, now).map_err(ClientError::Offer)?;
        let mut engine = Self {
            connection,
            outbox: Vec::new(),
            close_asked: false,
            opening: None,
            calls: BTreeMap::new(),
            lost: None,
            unsolicited: VecDeque::new(),
            subscriptions: BTreeMap::new(),
            ask_acknowledge: false,
            awaiting_ack: BTreeMap::new(),
            acknowledged: Vec::new(),
        };
        let start = engine.connection.start(now);
        engine.take_step(start, now);
        Ok(engine)
    }

    /// How the opening went: `None` while it goes on. Asked again, the same
    /// answer.
    pub fn opened(&self) -> Option<Result<(), ClientError>> {
        self.opening.clone()
    }

    /// A message arrived. The number of things that happened for whoever
    /// uses the client - an answer's message, a notice -, acknowledgements
    /// and the keepalive's `Pong` not counted.
    pub fn receive(&mut self, bytes: &[u8], now: i64) -> usize {
        let step = self.connection.receive(bytes, now);
        self.take_step(step, now)
    }

    /// Time passed: timeouts, the keepalive `Ping`, an authorization running
    /// out. Counted as [`Engine::receive`] counts.
    pub fn tick(&mut self, now: i64) -> usize {
        let step = self.connection.tick(now);
        self.take_step(step, now)
    }

    /// The transport is gone. Every request still out ends
    /// [`ClientError::Interrupted`] - sent, unless it is in `unsent`, its
    /// outcome unknown either way; a session still opening fails.
    pub fn lost(&mut self, why: TransportError, unsent: &[i64]) {
        for (id, call) in &mut self.calls {
            if call.outcome.is_none() {
                call.outcome = Some(Err(ClientError::Interrupted {
                    request: *id,
                    sent: !unsent.contains(id),
                    confirmed: false,
                    why: why.clone(),
                }));
            }
        }
        if self.opening.is_none() {
            self.opening = Some(Err(ClientError::Transport(why.clone())));
        }
        self.outbox.clear();
        // A stream ends when the connection does: whoever awaits the next
        // events of a subscribed protocol hears it, rather than waiting for
        // ever on a socket that is gone.
        let ended = Event::Closed {
            reason: format!("the connection is lost: {why}"),
        };
        for kept in self.subscriptions.values_mut() {
            kept.push_back(ended.clone());
        }
        self.lost = Some(why);
    }

    /// Why the transport is gone, if it is.
    pub fn is_lost(&self) -> Option<&TransportError> {
        self.lost.as_ref()
    }

    /// Send a message that expects no answer: it goes to the outbox, and
    /// nothing waits for it.
    pub fn tell<M: EtpMessage>(&mut self, message: &M) -> Result<(), ClientError> {
        if let Some(why) = &self.lost {
            return Err(ClientError::Transport(why.clone()));
        }
        if self.connection.state() == State::Closed {
            return Err(ClientError::Closed {
                reason: "the session is closed".into(),
            });
        }
        let out = self.connection.notify(message)?;
        self.outbox.push(Outbound {
            bytes: out.bytes,
            request: None,
        });
        Ok(())
    }

    /// Send a request: its message goes to the outbox, and its answer is
    /// gathered under the id returned.
    pub fn request<M: EtpMessage>(&mut self, request: &M, now: i64) -> Result<i64, ClientError> {
        if let Some(why) = &self.lost {
            return Err(ClientError::Transport(why.clone()));
        }
        if self.connection.state() == State::Closed {
            return Err(ClientError::Closed {
                reason: "the session is closed".into(),
            });
        }
        let out = if self.ask_acknowledge {
            let options = Options {
                ack: true,
                extension: None,
            };
            self.connection.request_with(request, &options, now)?
        } else {
            self.connection.request(request, now)?
        };
        let id = out.header.message_id;
        if self.ask_acknowledge {
            self.awaiting_ack.insert(id, (short_name(M::FULL_NAME), now));
        }
        self.calls.insert(
            id,
            Call {
                deliveries: Vec::new(),
                single: false,
                outcome: None,
            },
        );
        self.outbox.push(Outbound {
            bytes: out.bytes,
            request: Some(id),
        });
        Ok(id)
    }

    /// [`Self::request`] for a request that leaves in parts: the message and
    /// the `Chunk`s of its BLOBs, every part on the outbox, one call to wait
    /// on - the first part's id.
    pub fn request_parts<M: EtpMessage, P: EtpMessage>(
        &mut self,
        first: &M,
        parts: &[P],
        now: i64,
    ) -> Result<i64, ClientError> {
        if let Some(why) = &self.lost {
            return Err(ClientError::Transport(why.clone()));
        }
        if self.connection.state() == State::Closed {
            return Err(ClientError::Closed {
                reason: "the session is closed".into(),
            });
        }
        let options = Options {
            ack: self.ask_acknowledge,
            extension: None,
        };
        let out = self.connection.request_parts(first, parts, &options, now)?;
        let id = out[0].header.message_id;
        if self.ask_acknowledge {
            self.awaiting_ack.insert(id, (short_name(M::FULL_NAME), now));
        }
        self.calls.insert(
            id,
            Call {
                deliveries: Vec::new(),
                single: false,
                outcome: None,
            },
        );
        for message in out {
            self.outbox.push(Outbound {
                bytes: message.bytes,
                request: Some(id),
            });
        }
        Ok(id)
    }

    /// A `Ping`: its `Pong` is its answer.
    pub fn ping(&mut self, now: i64) -> Result<i64, ClientError> {
        if let Some(why) = &self.lost {
            return Err(ClientError::Transport(why.clone()));
        }
        let out = self.connection.ping(now);
        let id = out.header.message_id;
        self.calls.insert(
            id,
            Call {
                deliveries: Vec::new(),
                single: true,
                outcome: None,
            },
        );
        self.outbox.push(Outbound {
            bytes: out.bytes,
            request: Some(id),
        });
        Ok(id)
    }

    /// The answer to `request`, once it is over - every message up to the one
    /// with the FIN, or why it ended. Taken: asked again, `None`.
    pub fn take_answer(&mut self, request: i64) -> Option<Result<Vec<Delivery>, ClientError>> {
        self.calls.get(&request)?.outcome.as_ref()?;
        self.calls.remove(&request).and_then(|call| call.outcome)
    }

    /// End the session: `CloseSession`, then the transport is to close.
    pub fn close(&mut self, reason: &str) {
        let step = self.connection.close(reason);
        self.outbox.extend(step.send.into_iter().map(|out| Outbound {
            bytes: out.bytes,
            request: None,
        }));
        self.close_asked = true;
    }

    /// What to put on the transport, in order.
    pub fn take_outbound(&mut self) -> Vec<Outbound> {
        std::mem::take(&mut self.outbox)
    }

    /// Whether the transport is to close - asked once.
    pub fn take_close(&mut self) -> bool {
        std::mem::replace(&mut self.close_asked, false)
    }

    /// What the session was opened with, once it is.
    pub fn negotiated(&self) -> Option<&Negotiated> {
        self.connection.negotiated()
    }

    /// How the server's faults are taken.
    pub fn tolerance(&self) -> Tolerance {
        self.connection.tolerance()
    }

    /// The connection's state.
    pub fn state(&self) -> State {
        self.connection.state()
    }

    /// The last `currentDateTime` heard from the server.
    pub fn watermark(&self) -> Option<i64> {
        self.connection.watermark()
    }

    /// When [`Self::tick`] has something to do at the earliest, by the clock
    /// it is given; `None` when nothing is due. Asked again after every
    /// `receive`, `tick` and request: it moves.
    pub fn next_deadline(&self) -> Option<i64> {
        self.connection.next_deadline()
    }

    /// Say something when the session has been quiet this long, or never.
    pub fn set_keepalive(&mut self, period: Option<Duration>) {
        self.connection.set_keepalive(period);
    }

    /// Ask for an `Acknowledge` with every request from now on (§3.5.4.2).
    pub fn ask_acknowledge(&mut self, on: bool) {
        self.ask_acknowledge = on;
    }

    /// Whether requests ask for an `Acknowledge`.
    pub fn asks_acknowledge(&self) -> bool {
        self.ask_acknowledge
    }

    /// The acknowledgements heard since the last call, oldest first.
    pub fn acknowledgements(&mut self) -> Vec<Acknowledged> {
        std::mem::take(&mut self.acknowledged)
    }

    /// What arrived unasked since the last call: notifications, faults.
    pub fn unsolicited(&mut self) -> Vec<Event> {
        self.unsolicited.drain(..).collect()
    }

    /// Keep what arrives unasked for `protocol` apart, for [`Self::take_events`].
    pub fn subscribe(&mut self, protocol: i32) {
        self.subscriptions.entry(protocol).or_default();
    }

    /// Stop keeping it apart: what was kept and not taken goes with the
    /// unsolicited, in order, so that nothing is lost by unsubscribing.
    pub fn unsubscribe(&mut self, protocol: i32) {
        if let Some(kept) = self.subscriptions.remove(&protocol) {
            self.unsolicited.extend(kept);
        }
    }

    /// What arrived for a subscribed protocol since the last take.
    pub fn take_events(&mut self, protocol: i32) -> Vec<Event> {
        self.subscriptions
            .get_mut(&protocol)
            .map(|kept| kept.drain(..).collect())
            .unwrap_or_default()
    }

    /// Whether anything waits for a subscribed protocol.
    pub fn has_events(&self, protocol: i32) -> bool {
        self.subscriptions
            .get(&protocol)
            .is_some_and(|kept| !kept.is_empty())
    }

    /// An unasked delivery goes to its subscription when there is one.
    fn unasked(&mut self, event: Event) {
        if let Event::Delivered(delivery) = &event
            && let Some(kept) = self.subscriptions.get_mut(&delivery.header.protocol)
        {
            kept.push_back(event);
            return;
        }
        self.unsolicited.push_back(event);
    }

    /// Put what a step sends in the outbox, and sort its events: an
    /// acknowledgement is noted, an answer goes to its request, the rest is
    /// kept for [`Engine::unsolicited`].
    fn take_step(&mut self, step: Step, now: i64) -> usize {
        let Step { send, events, close } = step;
        self.outbox.extend(send.into_iter().map(|out| Outbound {
            bytes: out.bytes,
            request: None,
        }));
        if close {
            self.close_asked = true;
        }
        let mut happened = 0;
        for event in events {
            if let Event::Delivered(d) = &event
                && let Place::Acknowledges { message } = d.place
            {
                let (name, sent) = self.awaiting_ack.remove(&message).unwrap_or(("a message", now));
                let after = Duration::from_micros((now - sent).max(0) as u64);
                log::debug!(target: "retp_proto", "{name} #{message} acknowledged after {} ms", after.as_millis());
                self.acknowledged.push(Acknowledged {
                    message_id: message,
                    name,
                    after,
                });
                continue;
            }
            happened += 1;
            self.dispatch(event);
        }
        happened
    }

    fn dispatch(&mut self, event: Event) {
        if self.opening.is_none() {
            match event {
                Event::Opened => self.opening = Some(Ok(())),
                Event::Refused(incident) => self.opening = Some(Err(ClientError::Refused(incident))),
                Event::AuthorizationNeeded { challenges } => {
                    self.close_asked = true;
                    self.opening = Some(Err(ClientError::AuthorizationNeeded { challenges }));
                }
                Event::Closed { reason } => self.opening = Some(Err(ClientError::Closed { reason })),
                other => self.unsolicited.push_back(other),
            }
            return;
        }
        match event {
            Event::Delivered(delivery) => {
                let Place::Answers { request } = delivery.place else {
                    self.unasked(Event::Delivered(delivery));
                    return;
                };
                match self.calls.get_mut(&request) {
                    Some(call) if call.outcome.is_none() => {
                        let over = delivery.last || call.single;
                        call.deliveries.push(delivery);
                        if over {
                            call.outcome = Some(Ok(std::mem::take(&mut call.deliveries)));
                        }
                    }
                    _ => self.unasked(Event::Delivered(delivery)),
                }
            }
            Event::TimedOut { request, incident } => match self.calls.get_mut(&request) {
                Some(call) if call.outcome.is_none() => {
                    call.outcome = Some(Err(ClientError::TimedOut { request, incident }));
                }
                _ => self.unsolicited.push_back(Event::TimedOut { request, incident }),
            },
            Event::AnswerRefused { request, incident } => match self.calls.get_mut(&request) {
                Some(call) if call.outcome.is_none() => {
                    call.outcome = Some(Err(ClientError::AnswerRefused { request, incident }));
                }
                _ => self
                    .unsolicited
                    .push_back(Event::AnswerRefused { request, incident }),
            },
            Event::Closed { reason } => {
                for call in self.calls.values_mut().filter(|c| c.outcome.is_none()) {
                    call.outcome = Some(Err(ClientError::Closed {
                        reason: reason.clone(),
                    }));
                }
                // Every stream hears the end too - see `lost`.
                for kept in self.subscriptions.values_mut() {
                    kept.push_back(Event::Closed {
                        reason: reason.clone(),
                    });
                }
                self.unsolicited.push_back(Event::Closed { reason });
            }
            other => self.unsolicited.push_back(other),
        }
    }
}

/// `Store.GetDataObjects` out of `Energistics.Etp.v12.Protocol.Store.GetDataObjects`.
fn short_name(full: &'static str) -> &'static str {
    match full.rmatch_indices('.').nth(1) {
        Some((at, _)) => &full[at + 1..],
        None => full,
    }
}
