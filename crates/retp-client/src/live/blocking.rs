//! The live session as a blocking caller.
//!
//! Every operation of the crate is written once over [`Caller`]; the functions at the root
//! (`retp_client::list_dataspaces`, ...) run one on a [`Blocking`] caller with [`crate::blocking`],
//! which polls the operation once and expects it to be over. A [`LiveSession`] gets there by
//! running the [`AsyncClient`](crate::AsyncClient)'s own future to its end on the calling thread
//! (`LiveSession::block_on`: the thread parks until the answer wakes it, nothing polls) and handing
//! back a future that is already ready.
//!
//! Meanwhile the session task goes on reading the socket, so what the server says between two
//! calls reaches the destinations routed with [`LiveSession::route`].

use std::future::Future;

use retp_avro::EtpMessage;
use retp_proto::connection::Event;
use retp_proto::negotiate::Negotiated;
use retp_proto::wire::{Delivery, Tolerance};

use super::LiveSession;
use crate::{Blocking, Caller, ClientError};

impl Caller for LiveSession {
    fn at_once(&self) -> usize {
        self.at_once
    }

    fn call<M: EtpMessage>(
        &mut self,
        request: &M,
    ) -> impl Future<Output = Result<Vec<Delivery>, ClientError>> {
        std::future::ready(self.block_on(self.client().call(request)))
    }

    fn call_parts<M: EtpMessage, P: EtpMessage>(
        &mut self,
        first: &M,
        parts: &[P],
    ) -> impl Future<Output = Result<Vec<Delivery>, ClientError>> {
        std::future::ready(self.block_on(self.client().call_parts(first, parts)))
    }

    fn call_all<M: EtpMessage>(
        &mut self,
        requests: &[M],
    ) -> impl Future<Output = Vec<Result<Vec<Delivery>, ClientError>>> {
        let at_once = Caller::at_once(self);
        std::future::ready(self.block_on(self.client().call_all(requests, at_once)))
    }

    fn tell<M: EtpMessage>(&mut self, message: &M) -> impl Future<Output = Result<(), ClientError>> {
        std::future::ready(self.client().tell(message))
    }

    fn subscribe(&mut self, protocol: i32) {
        self.client().subscribe(protocol);
    }

    fn unsubscribe(&mut self, protocol: i32) {
        self.client().unsubscribe(protocol);
    }

    /// What the client keeps now, at once: whoever wants to wait routes a destination instead.
    fn events(&mut self, protocol: i32) -> impl Future<Output = Result<Vec<Event>, ClientError>> {
        std::future::ready(Ok(self.client().events(protocol)))
    }

    fn watermark(&self) -> Option<i64> {
        self.client().watermark()
    }

    fn negotiated(&self) -> Negotiated {
        self.client().negotiated()
    }

    fn tolerance(&self) -> Tolerance {
        self.client().tolerance()
    }
}

impl Blocking for LiveSession {
    /// `CloseSession`, the threads joined; a failure to close is logged, not returned.
    fn end(self, reason: &str) {
        if let Err(why) = self.close(reason) {
            log::warn!(target: "retp_client", "closing the live session: {why:?}");
        }
    }
}
