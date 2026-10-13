//! A transport whose other end is a server connection in the same process.
//!
//! What the tests of this crate and of every crate above it talk to: the
//! client under test is the one the command line uses, only the transport
//! differs, and the other end is a real [`Connection`] with real handlers.
//! And a clock that moves only when asked, so that a five-minute timeout takes
//! no time.
//!
//! It can also fail on purpose - after so many messages, or when a message
//! looks a certain way - because what a client does when the connection falls
//! is half of what there is to test. And it gives up after an hour of its own
//! time: a client waiting for what will never come would hang its test, and a
//! test that hangs never fails.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use retp_proto::connection::Connection;

use crate::{Clock, Transport, TransportError};

/// A clock that moves only when told to, shared by whoever holds a clone.
#[derive(Debug, Clone)]
pub struct ManualClock(Arc<AtomicI64>);

impl ManualClock {
    /// A clock at `now`, in microseconds since the Unix epoch.
    pub fn new(now: i64) -> Self {
        Self(Arc::new(AtomicI64::new(now)))
    }

    /// Put it at `now`.
    pub fn set(&self, now: i64) {
        self.0.store(now, Ordering::SeqCst);
    }

    /// Move it forward.
    pub fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_micros() as i64, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

type Predicate = Box<dyn Fn(&[u8]) -> bool + Send>;

/// A transport to a server [`Connection`] held in memory.
pub struct MemoryTransport {
    server: Connection,
    to_client: VecDeque<Vec<u8>>,
    clock: ManualClock,
    falls_after: Option<usize>,
    falls_when: Option<Predicate>,
    sent: usize,
    started: i64,
    patience: Duration,
}

impl MemoryTransport {
    /// A transport to `server`, whose waits move `clock`.
    pub fn new(server: Connection, clock: ManualClock) -> Self {
        let started = clock.now();
        Self {
            server,
            to_client: VecDeque::new(),
            clock,
            falls_after: None,
            falls_when: None,
            sent: 0,
            started,
            patience: Duration::from_secs(3600),
        }
    }

    /// The connection falls once `messages` have been sent: nothing sent
    /// after that is answered.
    pub fn falls_after(mut self, messages: usize) -> Self {
        self.falls_after = Some(messages);
        self
    }

    /// The connection falls on the first message `predicate` picks - the
    /// message is not delivered.
    pub fn falls_when(mut self, predicate: impl Fn(&[u8]) -> bool + Send + 'static) -> Self {
        self.falls_when = Some(Box::new(predicate));
        self
    }

    /// How long, by its clock, it waits before giving up on a client that
    /// waits for nothing (an hour by default).
    pub fn patience(mut self, patience: Duration) -> Self {
        self.patience = patience;
        self
    }

    fn fallen(&self) -> bool {
        self.falls_after.is_some_and(|n| self.sent >= n)
    }
}

impl Transport for MemoryTransport {
    fn send(&mut self, message: &[u8]) -> Result<(), TransportError> {
        if self.fallen() {
            return Err(TransportError::new("the connection fell"));
        }
        if self.falls_when.as_ref().is_some_and(|picks| picks(message)) {
            self.falls_after = Some(0);
            return Err(TransportError::new("the connection fell on that message"));
        }
        self.sent += 1;
        let step = self.server.receive(message, self.clock.now());
        self.to_client.extend(step.send.into_iter().map(|o| o.bytes));
        Ok(())
    }

    fn receive(&mut self, wait: Duration) -> Result<Option<Vec<u8>>, TransportError> {
        if self.fallen() {
            return Err(TransportError::new("the connection fell"));
        }
        if let Some(message) = self.to_client.pop_front() {
            return Ok(Some(message));
        }
        self.clock.advance(wait);
        if self.clock.now() - self.started > self.patience.as_micros() as i64 {
            return Err(TransportError::new(
                "the in-memory connection's patience ran out: the client would wait forever",
            ));
        }
        let step = self.server.tick(self.clock.now());
        self.to_client.extend(step.send.into_iter().map(|o| o.bytes));
        Ok(None)
    }

    fn close(&mut self) {}
}
