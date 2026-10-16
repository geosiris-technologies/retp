//! The session task and the dispatcher thread of the live session.
//!
//! One thread (`retp-live`) runs a current-thread runtime with one task that
//! owns the WebSocket and sleeps until a message arrives, something is to be
//! sent, or the next deadline of the engine is due. What arrives unasked is
//! handed to a second thread (`retp-live-dispatch`) through a bounded queue,
//! so a slow destination never delays an answer nor the keepalive.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, JoinHandle, Thread};
use std::time::Duration;

use retp_proto::connection::Event;
use retp_proto::probe::Handshake;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Instant;

use super::destination::Destination;
use super::ws::Ws;
use super::{Filter, NOTIFICATION_PROTOCOL, RouteId};
use crate::native::CertificateCheck;
use crate::notifications::Notification;
use crate::{AsyncClient, Clock, Sink, SystemClock, TransportError};

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// What the client writes: a queue towards the task.
// ---------------------------------------------------------------------------

/// What the client asks the task to do on the socket.
pub(super) enum Out {
    /// Write one binary message.
    Send(Vec<u8>),
    /// Close the WebSocket and end.
    Close,
}

/// The [`Sink`] given to the client: it only queues, the task writes.
pub(super) struct QueueSink(pub(super) UnboundedSender<Out>);

impl Sink for QueueSink {
    fn send(&mut self, message: &[u8]) -> Result<(), TransportError> {
        self.0
            .send(Out::Send(message.to_vec()))
            .map_err(|_| TransportError::new("the live session is over"))
    }

    fn close(&mut self) {
        let _ = self.0.send(Out::Close);
    }
}

// ---------------------------------------------------------------------------
// The routing table.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Route {
    id: RouteId,
    filter: Filter,
    to: Arc<dyn Destination>,
}

#[derive(Default)]
struct Table {
    next: u64,
    routes: Vec<Route>,
}

/// Where what arrives unasked goes; shared by the session and the task.
#[derive(Default)]
pub(super) struct Routing {
    table: Mutex<Table>,
}

impl Routing {
    pub(super) fn add(&self, filter: Filter, to: Arc<dyn Destination>) -> RouteId {
        let mut table = locked(&self.table);
        table.next += 1;
        let id = RouteId(table.next);
        table.routes.push(Route { id, filter, to });
        id
    }

    /// Remove a route. When it was the last one to need a protocol kept
    /// apart, that protocol is returned: the client can stop keeping it.
    pub(super) fn remove(&self, id: RouteId) -> Option<i32> {
        let mut table = locked(&self.table);
        let at = table.routes.iter().position(|r| r.id == id)?;
        let removed = table.routes.remove(at);
        let protocol = removed.filter.protocol()?;
        let still_needed = table.routes.iter().any(|r| r.filter.protocol() == Some(protocol));
        (!still_needed).then_some(protocol)
    }

    fn snapshot(&self) -> Vec<Route> {
        locked(&self.table).routes.clone()
    }
}

// ---------------------------------------------------------------------------
// The queue towards the dispatcher, and the counts.
// ---------------------------------------------------------------------------

/// What is shared between the task, the dispatcher and the session handle.
#[derive(Default)]
pub(super) struct Shared {
    /// Arrivals dropped because the queue was full.
    pub(super) dropped: AtomicU64,
    /// The same, per route, until the destination has been told.
    lost: Mutex<HashMap<RouteId, u64>>,
}

impl Shared {
    fn lose(&self, targets: &[(RouteId, Arc<dyn Destination>)]) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        let mut lost = locked(&self.lost);
        for (id, _) in targets {
            *lost.entry(*id).or_insert(0) += 1;
        }
    }

    fn take_lost(&self, id: RouteId) -> u64 {
        locked(&self.lost).remove(&id).unwrap_or(0)
    }
}

/// One arrival and the routes that selected it.
pub(super) struct Item {
    event: Event,
    targets: Vec<(RouteId, Arc<dyn Destination>)>,
}

/// The dispatcher thread: the only place the destinations are called.
fn dispatch(
    queue: Receiver<Item>,
    shared: Arc<Shared>,
    routing: Arc<Routing>,
    client: AsyncClient<SystemClock>,
) {
    let mut gone: HashSet<RouteId> = HashSet::new();
    // Ends when the task has dropped its sender and the queue is drained.
    for item in queue {
        for (id, destination) in item.targets {
            if gone.contains(&id) {
                continue;
            }
            let lost = shared.take_lost(id);
            let delivered = catch_unwind(AssertUnwindSafe(|| {
                if lost > 0 {
                    destination.dropped(lost);
                }
                destination.deliver(&item.event)
            }))
            .unwrap_or_else(|_| Err("the destination panicked".to_string()));
            if let Err(why) = delivered {
                tracing::warn!(target: "retp_client", error = %why, "a live destination failed and is removed: {why}");
                gone.insert(id);
                if let Some(protocol) = routing.remove(id) {
                    client.unsubscribe(protocol);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The task.
// ---------------------------------------------------------------------------

/// What the task needs to hand arrivals to the destinations.
struct Collector {
    client: AsyncClient<SystemClock>,
    routing: Arc<Routing>,
    shared: Arc<Shared>,
    queue: SyncSender<Item>,
}

impl Collector {
    /// Take what arrived unasked for the routed filters and queue it.
    fn collect(&self) {
        let routes = self.routing.snapshot();
        if routes.is_empty() {
            return;
        }
        let protocols: BTreeSet<i32> = routes.iter().filter_map(|r| r.filter.protocol()).collect();
        let by_subscription = routes.iter().any(|r| matches!(r.filter, Filter::Subscription(_)));
        for protocol in protocols {
            for event in self.client.events(protocol) {
                let subscription = (by_subscription && protocol == NOTIFICATION_PROTOCOL)
                    .then(|| Notification::of(event.clone()).subscription())
                    .flatten();
                let targets = routes
                    .iter()
                    .filter(|r| match &r.filter {
                        Filter::Protocol(p) => *p == protocol,
                        Filter::Subscription(wanted) => Some(*wanted) == subscription,
                        Filter::Session => false,
                    })
                    .map(|r| (r.id, r.to.clone()))
                    .collect();
                self.push(event, targets);
            }
        }
        let session: Vec<_> = routes
            .iter()
            .filter(|r| r.filter == Filter::Session)
            .map(|r| (r.id, r.to.clone()))
            .collect();
        if !session.is_empty() {
            for event in self.client.unsolicited() {
                self.push(event, session.clone());
            }
        }
    }

    fn push(&self, event: Event, targets: Vec<(RouteId, Arc<dyn Destination>)>) {
        if targets.is_empty() {
            return;
        }
        match self.queue.try_send(Item { event, targets }) {
            Ok(()) => {}
            Err(TrySendError::Full(item)) => self.shared.lose(&item.targets),
            // The dispatcher is gone: nothing can be delivered any more.
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

/// A point in clock time (microseconds since the Unix epoch) as a tokio
/// instant; a deadline already past is now.
fn instant_at(clock_micros: i64) -> Instant {
    let wait = clock_micros.saturating_sub(SystemClock.now()).max(0);
    Instant::now() + Duration::from_micros(wait.unsigned_abs())
}

/// The session task: it owns the socket and sleeps until something is due.
async fn run(mut ws: Ws, collector: Collector, mut out: tokio::sync::mpsc::UnboundedReceiver<Out>) {
    let client = collector.client.clone();
    let lost = loop {
        let deadline = client.next_deadline().map(instant_at);
        tokio::select! {
            arrived = ws.receive() => match arrived {
                Ok(bytes) => {
                    client.received(&bytes);
                    collector.collect();
                }
                Err(why) => break Some(why),
            },
            outgoing = out.recv() => match outgoing {
                Some(Out::Send(bytes)) => {
                    if let Err(why) = ws.send(bytes).await {
                        break Some(why);
                    }
                }
                Some(Out::Close) | None => {
                    ws.close().await;
                    break None;
                }
            },
            () = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {
                client.tick();
                collector.collect();
            }
        }
    };
    if let Some(why) = lost {
        client.lost(why);
    }
    collector.collect();
}

/// The threads of a session.
pub(super) struct Threads {
    pub(super) task: JoinHandle<()>,
    pub(super) dispatcher: JoinHandle<()>,
}

/// Start the dispatcher and the session thread. The WebSocket is opened
/// inside the thread; if that fails the error goes to `failed` and the client
/// is told the connection is lost, so `opened()` ends.
#[allow(clippy::too_many_arguments)]
pub(super) fn start(
    handshake: Handshake,
    certificates: CertificateCheck,
    client: AsyncClient<SystemClock>,
    out: UnboundedReceiver<Out>,
    routing: Arc<Routing>,
    shared: Arc<Shared>,
    queue: usize,
    failed: std::sync::mpsc::Sender<TransportError>,
) -> std::io::Result<Threads> {
    let (to_dispatcher, from_task) = std::sync::mpsc::sync_channel::<Item>(queue.max(1));
    let dispatcher = {
        let (shared, routing, client) = (shared.clone(), routing.clone(), client.clone());
        thread::Builder::new()
            .name("retp-live-dispatch".into())
            .spawn(move || dispatch(from_task, shared, routing, client))?
    };
    let collector = Collector {
        client: client.clone(),
        routing,
        shared,
        queue: to_dispatcher,
    };
    let task = thread::Builder::new().name("retp-live".into()).spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(runtime) => runtime,
            Err(why) => {
                let why = TransportError::new(format!("starting the live session's runtime: {why}"));
                let _ = failed.send(TransportError::new(why.why.clone()));
                client.lost(why);
                return;
            }
        };
        runtime.block_on(async move {
            match Ws::open(&handshake, certificates).await {
                Ok(ws) => run(ws, collector, out).await,
                Err(why) => {
                    let _ = failed.send(TransportError::new(why.why.clone()));
                    client.lost(why);
                }
            }
        });
    });
    match task {
        Ok(task) => Ok(Threads { task, dispatcher }),
        Err(why) => {
            // The closure is gone with its sender: the dispatcher ends by itself.
            let _ = dispatcher.join();
            Err(why)
        }
    }
}

// ---------------------------------------------------------------------------
// block_on: park the calling thread until the future is woken.
// ---------------------------------------------------------------------------

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Run a future on the calling thread: while it is pending the thread is
/// parked, and the waker unparks it. No polling, no sleep loop.
pub(super) fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}
