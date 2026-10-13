//! The client for whoever cannot block - a browser's worker, first.
//!
//! The socket is not ours: whoever holds it tells the client what arrives
//! ([`AsyncClient::received`]), that time passed ([`AsyncClient::tick`]) and
//! that the connection fell ([`AsyncClient::lost`]); the client puts what it
//! sends in a [`Sink`]. A request is a future that is over when its answer
//! is, and several can be out at once, each answered on its own.
//!
//! In a front end: the WebSocket's `onmessage` calls `received`, a `setInterval`
//! calls `tick` (timeouts and the keepalive `Ping`), `onclose`
//! calls `lost`, and the sink is `ws.send`. Nothing here knows JavaScript:
//! the glue lives in the WebAssembly front end, and all of this is tested natively.

use std::cell::RefCell;
use std::future::{Future, poll_fn};
use std::rc::Rc;
use std::task::{Poll, Waker};

use retp_avro::EtpMessage;
use retp_proto::connection::{Event, Handler};
use retp_proto::credentials::Credentials;
use retp_proto::negotiate::{Negotiated, Offer};
use retp_proto::wire::{Delivery, Tolerance};

use crate::caller::Caller;
use crate::engine::Engine;
use crate::{Acknowledged, ClientError, Clock, TransportError};

/// Where the client puts what it sends: one whole WebSocket message at a
/// time.
pub trait Sink {
    /// Send one message.
    fn send(&mut self, message: &[u8]) -> Result<(), TransportError>;

    /// Close the connection. Closing twice is not an error.
    fn close(&mut self);
}

struct Shared {
    engine: Engine,
    sink: Box<dyn Sink>,
    /// The futures waiting, woken whenever something arrives: each looks for
    /// its own answer.
    waiting: Vec<Waker>,
}

impl Shared {
    /// The outbox onto the sink; a send that fails loses the connection, and
    /// what had not left is known to be unsent.
    fn flush(&mut self) {
        let outbound = self.engine.take_outbound();
        for (i, out) in outbound.iter().enumerate() {
            if let Err(why) = self.sink.send(&out.bytes) {
                let unsent: Vec<i64> = outbound[i..].iter().filter_map(|o| o.request).collect();
                self.engine.lost(why, &unsent);
                break;
            }
        }
        if self.engine.take_close() {
            self.sink.close();
        }
    }

    fn wake(&mut self) {
        for waker in self.waiting.drain(..) {
            waker.wake();
        }
    }
}

/// An ETP client driven from outside. Cheap to clone: the clones are the
/// same client.
#[derive(Clone)]
pub struct AsyncClient<C: Clock> {
    shared: Rc<RefCell<Shared>>,
    clock: C,
}

impl<C: Clock> AsyncClient<C> {
    /// Start a session: the `RequestSession` goes to `sink` at once. The
    /// session is open when [`AsyncClient::opened`] says so; meanwhile,
    /// whoever holds the socket feeds [`AsyncClient::received`].
    pub fn start(
        sink: Box<dyn Sink>,
        clock: C,
        offer: Offer,
        handlers: Vec<Box<dyn Handler>>,
        credentials: Option<Credentials>,
    ) -> Result<Self, ClientError> {
        let engine = Engine::start(offer, handlers, credentials, clock.now())?;
        let client = Self {
            shared: Rc::new(RefCell::new(Shared {
                engine,
                sink,
                waiting: Vec::new(),
            })),
            clock,
        };
        client.shared.borrow_mut().flush();
        Ok(client)
    }

    /// Over when the session is open - or could not be, and why.
    pub fn opened(&self) -> impl Future<Output = Result<(), ClientError>> + '_ {
        poll_fn(|cx| {
            let mut shared = self.shared.borrow_mut();
            match shared.engine.opened() {
                Some(opened) => Poll::Ready(opened),
                None => {
                    shared.waiting.push(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }

    /// A message arrived on the socket.
    pub fn received(&self, message: &[u8]) {
        let now = self.clock.now();
        let mut shared = self.shared.borrow_mut();
        shared.engine.receive(message, now);
        shared.flush();
        shared.wake();
    }

    /// Time passed: what is due - a timeout, the keepalive `Ping` - happens.
    pub fn tick(&self) {
        let now = self.clock.now();
        let mut shared = self.shared.borrow_mut();
        shared.engine.tick(now);
        shared.flush();
        shared.wake();
    }

    /// The socket closed, or failed: every request still out ends
    /// [`ClientError::Interrupted`].
    pub fn lost(&self, why: TransportError) {
        let mut shared = self.shared.borrow_mut();
        shared.engine.lost(why, &[]);
        shared.wake();
    }

    /// Send a request, and gather every message of its answer.
    pub async fn call<M: EtpMessage>(&self, request: &M) -> Result<Vec<Delivery>, ClientError> {
        let id = {
            let mut shared = self.shared.borrow_mut();
            let id = shared.engine.request(request, self.clock.now())?;
            shared.flush();
            id
        };
        self.answer(id).await
    }

    /// [`Self::call`] for a request that leaves in parts.
    pub async fn call_parts<M: EtpMessage, P: EtpMessage>(
        &self,
        first: &M,
        parts: &[P],
    ) -> Result<Vec<Delivery>, ClientError> {
        let id = {
            let mut shared = self.shared.borrow_mut();
            let id = shared.engine.request_parts(first, parts, self.clock.now())?;
            shared.flush();
            id
        };
        self.answer(id).await
    }

    /// Send a message that expects no answer - a `StartStreaming` - onto
    /// the sink at once.
    pub fn tell<M: EtpMessage>(&self, message: &M) -> Result<(), ClientError> {
        let mut shared = self.shared.borrow_mut();
        shared.engine.tell(message)?;
        shared.flush();
        Ok(())
    }

    /// A `Ping`; over when its `Pong` arrives.
    pub async fn ping(&self) -> Result<(), ClientError> {
        let id = {
            let mut shared = self.shared.borrow_mut();
            let id = shared.engine.ping(self.clock.now())?;
            shared.flush();
            id
        };
        self.answer(id).await.map(|_| ())
    }

    fn answer(&self, id: i64) -> impl Future<Output = Result<Vec<Delivery>, ClientError>> + '_ {
        poll_fn(move |cx| {
            let mut shared = self.shared.borrow_mut();
            match shared.engine.take_answer(id) {
                Some(answer) => Poll::Ready(answer),
                None => {
                    shared.waiting.push(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }

    /// End the session: `CloseSession`, then the sink is closed.
    pub fn close(&self, reason: &str) {
        let mut shared = self.shared.borrow_mut();
        shared.engine.close(reason);
        shared.flush();
    }

    /// Send these requests with at most `at_once` in flight, and answer each
    /// in order - the window refilled as answers come, every request its
    /// own future, all of them polled by the one that awaits this.
    pub async fn call_all<M: EtpMessage>(
        &self,
        requests: &[M],
        at_once: usize,
    ) -> Vec<Result<Vec<Delivery>, ClientError>> {
        let window = at_once.max(1);
        let mut outcomes: Vec<Option<Result<Vec<Delivery>, ClientError>>> =
            (0..requests.len()).map(|_| None).collect();
        let mut next = 0;
        let mut pending: Vec<(usize, i64)> = Vec::new();
        loop {
            while pending.len() < window && next < requests.len() {
                let sent = {
                    let mut shared = self.shared.borrow_mut();
                    let sent = shared.engine.request(&requests[next], self.clock.now());
                    shared.flush();
                    sent
                };
                match sent {
                    Ok(id) => pending.push((next, id)),
                    Err(why) => outcomes[next] = Some(Err(why)),
                }
                next += 1;
            }
            if pending.is_empty() {
                break;
            }
            // Wait until any of the pending is answered.
            let answered = poll_fn(|cx| {
                let mut shared = self.shared.borrow_mut();
                let mut done = Vec::new();
                for (index, id) in &pending {
                    if let Some(answer) = shared.engine.take_answer(*id) {
                        done.push((*index, answer));
                    }
                }
                if done.is_empty() {
                    shared.waiting.push(cx.waker().clone());
                    Poll::Pending
                } else {
                    Poll::Ready(done)
                }
            })
            .await;
            for (index, answer) in answered {
                outcomes[index] = Some(answer);
                pending.retain(|(i, _)| *i != index);
            }
        }
        outcomes
            .into_iter()
            .map(|one| one.expect("every request has an outcome"))
            .collect()
    }

    /// Keep what the peer sends on its own for `protocol` apart, for
    /// [`Self::events`] and [`Self::next_events`].
    pub fn subscribe(&self, protocol: i32) {
        self.shared.borrow_mut().engine.subscribe(protocol);
    }

    pub fn unsubscribe(&self, protocol: i32) {
        self.shared.borrow_mut().engine.unsubscribe(protocol);
    }

    /// What arrived for a subscribed protocol since the last take; empty
    /// when nothing did.
    pub fn events(&self, protocol: i32) -> Vec<Event> {
        self.shared.borrow_mut().engine.take_events(protocol)
    }

    /// The next messages of a subscribed protocol - over when at least one
    /// is there. A stream is read by awaiting this again and again.
    pub fn next_events(&self, protocol: i32) -> impl Future<Output = Vec<Event>> + '_ {
        poll_fn(move |cx| {
            let mut shared = self.shared.borrow_mut();
            if shared.engine.has_events(protocol) {
                Poll::Ready(shared.engine.take_events(protocol))
            } else {
                shared.waiting.push(cx.waker().clone());
                Poll::Pending
            }
        })
    }

    /// What the session was opened with.
    ///
    /// # Panics
    ///
    /// Before [`AsyncClient::opened`] has said the session is open.
    pub fn negotiated(&self) -> Negotiated {
        self.shared
            .borrow()
            .engine
            .negotiated()
            .expect("asked once the session is open")
            .clone()
    }

    /// How this client takes the server's faults.
    pub fn tolerance(&self) -> Tolerance {
        self.shared.borrow().engine.tolerance()
    }

    /// Ask for an `Acknowledge` with every request from now on (§3.5.4.2).
    pub fn ask_acknowledge(&self, on: bool) {
        self.shared.borrow_mut().engine.ask_acknowledge(on);
    }

    /// The acknowledgements heard since the last call, oldest first.
    pub fn acknowledgements(&self) -> Vec<Acknowledged> {
        self.shared.borrow_mut().engine.acknowledgements()
    }

    /// What arrived unasked since the last call: notifications, faults.
    pub fn unsolicited(&self) -> Vec<Event> {
        self.shared.borrow_mut().engine.unsolicited()
    }

    /// The last `currentDateTime` heard from the server.
    pub fn watermark(&self) -> Option<i64> {
        self.shared.borrow().engine.watermark()
    }
}

impl<C: Clock> Caller for AsyncClient<C> {
    fn call<M: EtpMessage>(
        &mut self,
        request: &M,
    ) -> impl Future<Output = Result<Vec<Delivery>, ClientError>> {
        AsyncClient::call(self, request)
    }

    fn call_parts<M: EtpMessage, P: EtpMessage>(
        &mut self,
        first: &M,
        parts: &[P],
    ) -> impl Future<Output = Result<Vec<Delivery>, ClientError>> {
        AsyncClient::call_parts(self, first, parts)
    }

    fn call_all<M: EtpMessage>(
        &mut self,
        requests: &[M],
    ) -> impl Future<Output = Vec<Result<Vec<Delivery>, ClientError>>> {
        AsyncClient::call_all(self, requests, 4)
    }

    fn tell<M: EtpMessage>(&mut self, message: &M) -> impl Future<Output = Result<(), ClientError>> {
        std::future::ready(AsyncClient::tell(self, message))
    }

    fn subscribe(&mut self, protocol: i32) {
        AsyncClient::subscribe(self, protocol);
    }

    fn unsubscribe(&mut self, protocol: i32) {
        AsyncClient::unsubscribe(self, protocol);
    }

    /// What is held now: the front end's worker drives the socket, and awaits
    /// [`AsyncClient::next_events`] when it wants to wait.
    fn events(&mut self, protocol: i32) -> impl Future<Output = Result<Vec<Event>, ClientError>> {
        std::future::ready(Ok(AsyncClient::events(self, protocol)))
    }

    fn watermark(&self) -> Option<i64> {
        AsyncClient::watermark(self)
    }

    fn negotiated(&self) -> Negotiated {
        AsyncClient::negotiated(self)
    }

    fn tolerance(&self) -> Tolerance {
        AsyncClient::tolerance(self)
    }
}
