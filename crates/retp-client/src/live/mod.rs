//! A native ETP session that never waits actively.
//!
//! The blocking [`Client`](crate::Client) reads the WebSocket only while a
//! call runs: between two calls nothing is read, and a notification waits in
//! the socket until the next call. A [`LiveSession`] is the other way round.
//! One task, on a thread of its own, owns the socket and sleeps in the
//! operating system until one of three things happens:
//!
//! - a message arrives (it is given to the engine, and what arrived unasked
//!   is pushed to the destinations chosen by the user);
//! - something must be sent (a request from any thread);
//! - the next deadline of the engine is due (the keepalive `Ping`, a
//!   response timeout).
//!
//! There is no polling and no sleep loop: the task is woken by the socket,
//! by its queue, or by a timer set at the engine's next deadline, and the
//! thread of a caller waiting in [`LiveSession::block_on`] is parked until
//! its future is woken.
//!
//! # How it relates to the other clients
//!
//! The session is an [`AsyncClient`] (the engine driven
//! from outside, the one a browser front end drives with `onmessage` and a
//! timer) driven here by a tokio task instead of by JavaScript.
//! [`LiveSession::client`] gives that client, which is a
//! [`Caller`](crate::Caller): every operation of the crate runs on it
//! unchanged. The blocking `Client` stays for scripts that ask and wait; the
//! live session is for programs that must hear the server between their own
//! requests.
//!
//! # What arrives unasked
//!
//! [`LiveSession::route`] sends what a [`Filter`] selects to a
//! [`Destination`]: a JSON Lines file, the console, a callback or a queue.
//! Destinations are called on a dispatcher thread, never on the session task,
//! so a slow destination never delays an answer nor the keepalive. The queue
//! between them is bounded ([`LiveSettings::queue`]): when it is full the
//! newest arrivals are dropped and counted ([`LiveSession::dropped`]).
//!
//! What no route selects stays where [`AsyncClient`]
//! keeps it. Note that an arrival taken from a protocol that is routed, but
//! that no route of that protocol selects (a [`Filter::Subscription`] for
//! another subscription), is not kept.
//!
//! ```no_run
//! use std::sync::Arc;
//! use retp_client::live::{Console, Filter, LiveSession, LiveSettings};
//! use retp_client::native::Connect;
//!
//! fn watch(connect: Connect) -> Result<(), retp_client::ClientError> {
//!     let session = LiveSession::connect(connect, LiveSettings::default())?;
//!     // Everything the store notifies is printed as it arrives.
//!     session.route(Filter::Protocol(5), Arc::new(Console::stdout()));
//!     // Requests go on from this thread meanwhile.
//!     session.block_on(session.client().ping())?;
//!     session.close("done")
//! }
//! ```

mod blocking;
mod task;
mod watch;
mod ws;

pub mod destination;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use retp_proto::probe::{self, HandshakeSettings};
use retp_types::datatypes::Uuid;

pub use destination::{Callback, Console, Destination, JsonLines, Queue, as_json, as_line};

use crate::native::Connect;
use crate::{AsyncClient, ClientError, SystemClock, TransportError};

/// The sub-protocol of the store notifications (`StoreNotification`).
const NOTIFICATION_PROTOCOL: i32 = 5;

/// What arrives unasked, routed to a destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    /// Everything that arrives unasked for this sub-protocol (5: StoreNotification, 21: channels, ...).
    Protocol(i32),
    /// The notifications of one subscription (protocol 5), by the subscription's request uuid.
    Subscription(Uuid),
    /// The session's own events: a fault the server sent, the authorization running out,
    /// a request timed out, the session closed.
    Session,
}

impl Filter {
    /// The sub-protocol the client must keep apart for this filter.
    fn protocol(&self) -> Option<i32> {
        match self {
            Filter::Protocol(p) => Some(*p),
            Filter::Subscription(_) => Some(NOTIFICATION_PROTOCOL),
            Filter::Session => None,
        }
    }
}

/// A route, to remove it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RouteId(u64);

/// Bounds of the live session.
#[derive(Debug, Clone)]
pub struct LiveSettings {
    /// How many arrivals wait for the destinations before the newest are dropped and counted.
    pub queue: usize,
    /// The keepalive period; `None` keeps the engine's default.
    pub keepalive: Option<Duration>,
    /// How many requests a plural read keeps in flight (`Caller::at_once`).
    pub at_once: usize,
}

impl Default for LiveSettings {
    fn default() -> Self {
        Self {
            queue: 1024,
            keepalive: None,
            at_once: 4,
        }
    }
}

/// A native ETP session driven by its own task: requests from any thread, what arrives unasked
/// pushed to destinations as it arrives, no polling.
pub struct LiveSession {
    client: AsyncClient<SystemClock>,
    routing: Arc<task::Routing>,
    shared: Arc<task::Shared>,
    threads: Option<task::Threads>,
    at_once: usize,
}

impl LiveSession {
    /// Open the connection as `native::connect` does, start the session task on a thread of its
    /// own (a current-thread tokio runtime), and return once the session is open (or refused).
    pub fn connect(connect: Connect, settings: LiveSettings) -> Result<LiveSession, ClientError> {
        let handshake_settings = HandshakeSettings {
            max_message: connect.max_message,
            max_frame: None,
            authorization: connect.endpoint.credentials.clone(),
        };
        let handshake = probe::handshake(
            &connect.endpoint.url,
            connect.endpoint.handshake,
            &handshake_settings,
        )
        .map_err(|why| ClientError::Transport(TransportError::new(why.to_string())))?;
        let handshake = connect.endpoint.profile.adapt(handshake);

        // The client is started here, so the handlers stay on this thread
        // until the client (which is `Send`) carries them: the first message
        // only waits in the queue until the task has the socket.
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();
        let client = AsyncClient::start(
            Box::new(task::QueueSink(out_tx)),
            SystemClock,
            connect.offer,
            connect.handlers,
            connect.endpoint.credentials,
        )?;
        if let Some(period) = settings.keepalive {
            client.set_keepalive(Some(period));
        }

        let routing = Arc::new(task::Routing::default());
        let shared = Arc::new(task::Shared::default());
        let (failed_tx, failed_rx) = std::sync::mpsc::channel();
        let threads = task::start(
            handshake,
            connect.endpoint.certificates,
            client.clone(),
            out_rx,
            routing.clone(),
            shared.clone(),
            settings.queue,
            failed_tx,
        )
        .map_err(|why| {
            ClientError::Transport(TransportError::new(format!("starting the live session: {why}")))
        })?;

        let session = LiveSession {
            client,
            routing,
            shared,
            threads: Some(threads),
            at_once: settings.at_once.max(1),
        };
        match task::block_on(session.client.opened()) {
            Ok(()) => Ok(session),
            // The session is dropped here: it closes and joins its threads.
            Err(why) => Err(match failed_rx.try_recv() {
                Ok(transport) => ClientError::Transport(transport),
                Err(_) => why,
            }),
        }
    }

    /// The client: requests from any thread, `Caller` for every operation of the crate.
    pub fn client(&self) -> &AsyncClient<SystemClock> {
        &self.client
    }

    /// Run an operation (any future, typically an `async` operation over `Caller` on
    /// `self.client().clone()`) from synchronous code: the calling thread parks until the future
    /// is woken - no polling, no runtime needed by the caller.
    pub fn block_on<F: std::future::Future>(&self, operation: F) -> F::Output {
        task::block_on(operation)
    }

    /// Route what `filter` selects to `to`. `Filter::Protocol(p)` and `Filter::Subscription(_)`
    /// also make the client keep protocol `p` (5 for a subscription) apart (`subscribe`).
    pub fn route(&self, filter: Filter, to: Arc<dyn Destination>) -> RouteId {
        if let Some(protocol) = filter.protocol() {
            self.client.subscribe(protocol);
        }
        self.routing.add(filter, to)
    }

    /// Stop routing.
    pub fn unroute(&self, route: RouteId) {
        if let Some(protocol) = self.routing.remove(route) {
            self.client.unsubscribe(protocol);
        }
    }

    /// How many arrivals were dropped because the destinations' queue was full.
    pub fn dropped(&self) -> u64 {
        self.shared.dropped.load(Ordering::Relaxed)
    }

    /// End the session: `CloseSession`, the socket closed, the task and the dispatcher stopped,
    /// their threads joined.
    pub fn close(mut self, reason: &str) -> Result<(), ClientError> {
        self.finish(reason)
    }

    /// Close the session and join the threads; a second call does nothing.
    fn finish(&mut self, reason: &str) -> Result<(), ClientError> {
        let Some(threads) = self.threads.take() else {
            return Ok(());
        };
        // `CloseSession` first, then the sink's close: the task writes both.
        self.client.close(reason);
        let task = threads.task.join();
        let dispatcher = threads.dispatcher.join();
        if task.is_err() || dispatcher.is_err() {
            return Err(ClientError::Transport(TransportError::new(
                "a thread of the live session panicked",
            )));
        }
        Ok(())
    }
}

impl Drop for LiveSession {
    fn drop(&mut self) {
        let _ = self.finish("the live session was dropped");
    }
}
