//! What an operation needs of a client - so that each operation is written
//! **once**, for the blocking [`Client`] and the [`AsyncClient`](crate::AsyncClient)
//! of a front end alike.
//!
//! An operation asks, reads the answer, and looks at what the session
//! agreed. Written as an `async fn` over [`Caller`], it runs on either: the
//! blocking client answers inside `call`, so on it an operation is over at
//! its first poll ([`run`]); the asynchronous one answers when the message
//! arrives. The alternative - the same operation written twice, blocking
//! and asynchronous - is what this crate refuses.

use std::future::Future;
use std::task::{Context, Poll, Waker};

use retp_avro::EtpMessage;
use retp_proto::connection::Event;
use retp_proto::negotiate::Negotiated;
use retp_proto::wire::{Delivery, Tolerance};

use crate::{Client, ClientError, Clock, Transport};

/// A client an operation can run on.
pub trait Caller {
    /// Send a request and gather every message of its answer, up to the one
    /// that carries the FIN.
    fn call<M: EtpMessage>(
        &mut self,
        request: &M,
    ) -> impl Future<Output = Result<Vec<Delivery>, ClientError>>;

    /// [`Self::call`] for a request that leaves in **parts** - the message,
    /// then the `Chunk`s of the BLOBs it announced (§3.7.3.2). One id, the
    /// first part's, which the answer correlates to.
    ///
    /// The default refuses by name: a caller without a wire - the REST
    /// client, whose requests are HTTP bodies and carry no BLOB - has no
    /// parts to send, and a caller that has one says so by implementing it.
    fn call_parts<M: EtpMessage, P: EtpMessage>(
        &mut self,
        first: &M,
        parts: &[P],
    ) -> impl Future<Output = Result<Vec<Delivery>, ClientError>> {
        let _ = (first, parts);
        std::future::ready(Err(ClientError::Invalid(
            retp_proto::wire::EtpError::EINVALID_ARGUMENT.with_detail(format!(
                "{} announces BLOBs to send in Chunks, and this caller sends no multipart request - the object is bigger than one message and only the ETP WebSocket sends it in pieces",
                M::FULL_NAME
            )),
        )))
    }

    /// Send these requests with at most [`Self::at_once`] in flight, and
    /// answer each in order - **what a plural read is made of**
    /// (parallel batches matter here).
    ///
    /// The default sends them one after the other, which every caller can
    /// do; the WebSocket clients keep a window of them out at once, and the
    /// engine answers each to its own request by correlation. A request that
    /// fails does not take the others down: each has its own result.
    fn call_all<M: EtpMessage>(
        &mut self,
        requests: &[M],
    ) -> impl Future<Output = Vec<Result<Vec<Delivery>, ClientError>>> {
        async move {
            let mut answers = Vec::with_capacity(requests.len());
            for request in requests {
                answers.push(self.call(request).await);
            }
            answers
        }
    }

    /// How many requests [`Self::call_all`] keeps in flight. Four unless
    /// told otherwise - the number the REST transport already used.
    fn at_once(&self) -> usize {
        4
    }

    /// Send a message that expects no answer - `StartStreaming`,
    /// `StopStreaming` (§6.3.1). A caller without a wire has nothing to say it
    /// on, and refuses by name rather than pretend.
    fn tell<M: EtpMessage>(&mut self, message: &M) -> impl Future<Output = Result<(), ClientError>> {
        let _ = message;
        std::future::ready(Err(ClientError::Invalid(
            retp_proto::wire::EtpError::ENOTSUPPORTED.with_detail(format!(
                "{} expects no answer, and this caller has no wire to say it on: it is the ETP WebSocket's",
                M::FULL_NAME
            )),
        )))
    }

    /// Keep what the peer sends on its own for `protocol` apart - a
    /// notification, a channel's data - for [`Self::events`], instead of
    /// with everything else that arrives unasked. A caller with no wire
    /// hears nothing unasked, and keeps nothing.
    fn subscribe(&mut self, protocol: i32) {
        let _ = protocol;
    }

    /// Stop keeping it apart; what was kept goes with the unsolicited.
    fn unsubscribe(&mut self, protocol: i32) {
        let _ = protocol;
    }

    /// What arrived for a subscribed protocol since the last take, after
    /// giving the transport a moment to bring some. Empty is *nothing
    /// yet*, never *over*: a stream ends when the peer says so.
    fn events(&mut self, protocol: i32) -> impl Future<Output = Result<Vec<Event>, ClientError>> {
        let _ = protocol;
        std::future::ready(Ok(Vec::new()))
    }

    /// What the session agreed.
    /// The last `currentDateTime` heard from the peer - the watermark a
    /// catch-up starts from (§26.6.2.1). `None` on a caller with no session
    /// clock: the REST API answers requests, never a time of its own.
    fn watermark(&self) -> Option<i64> {
        None
    }

    fn negotiated(&self) -> Negotiated;

    /// How the server's faults are taken.
    fn tolerance(&self) -> Tolerance;
}

impl<T: Transport, C: Clock> Caller for Client<T, C> {
    fn call<M: EtpMessage>(
        &mut self,
        request: &M,
    ) -> impl Future<Output = Result<Vec<Delivery>, ClientError>> {
        std::future::ready(Client::call(self, request))
    }

    fn call_parts<M: EtpMessage, P: EtpMessage>(
        &mut self,
        first: &M,
        parts: &[P],
    ) -> impl Future<Output = Result<Vec<Delivery>, ClientError>> {
        std::future::ready(Client::call_parts(self, first, parts))
    }

    fn call_all<M: EtpMessage>(
        &mut self,
        requests: &[M],
    ) -> impl Future<Output = Vec<Result<Vec<Delivery>, ClientError>>> {
        std::future::ready(Client::call_all(self, requests))
    }

    fn at_once(&self) -> usize {
        self.settings().at_once
    }

    fn tell<M: EtpMessage>(&mut self, message: &M) -> impl Future<Output = Result<(), ClientError>> {
        std::future::ready(Client::tell(self, message))
    }

    fn subscribe(&mut self, protocol: i32) {
        Client::subscribe(self, protocol);
    }

    fn unsubscribe(&mut self, protocol: i32) {
        Client::unsubscribe(self, protocol);
    }

    fn events(&mut self, protocol: i32) -> impl Future<Output = Result<Vec<Event>, ClientError>> {
        let wait = self.settings().poll;
        std::future::ready(Client::events(self, protocol, wait))
    }

    fn watermark(&self) -> Option<i64> {
        Client::watermark(self)
    }

    fn negotiated(&self) -> Negotiated {
        Client::negotiated(self).clone()
    }

    fn tolerance(&self) -> Tolerance {
        Client::tolerance(self)
    }
}

/// A client whose calls are over when they return - the blocking
/// [`Client`], the native REST client: the functions at the root of the crate
/// run an operation on it at once. The [`AsyncClient`](crate::AsyncClient) is
/// not one: its answers come when the front end says they came.
pub trait Blocking: Caller {
    /// End the session, if there is one to end - a WebSocket's; a REST
    /// client has none, and does nothing.
    fn end(self, reason: &str)
    where
        Self: Sized,
    {
        let _ = reason;
    }
}

impl<T: Transport, C: Clock> Blocking for Client<T, C> {
    fn end(self, reason: &str) {
        let _ = Client::close(self, reason);
    }
}

/// Run an operation on a blocking caller. It answers inside `call`, so the
/// operation is over at its first poll: nothing here waits, and nothing needs
/// an executor. Public because an operation written above this crate -
/// `energyml_etp_store::pack`, say - is run the same way.
pub fn run<F: Future>(operation: F) -> F::Output {
    let mut operation = std::pin::pin!(operation);
    match operation.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => {
            unreachable!("the blocking client answers inside `call`: an operation on it never waits")
        }
    }
}
