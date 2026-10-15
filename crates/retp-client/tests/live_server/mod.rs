//! A local ETP server over a real WebSocket, for the tests of the live session.
//!
//! The server is a real WebSocket on `127.0.0.1` (the crate's own `tungstenite`, in its server
//! role) with a server [`Connection`] behind it. It opens the session, lists one dataspace,
//! confirms a subscription (and ends one on `UnsubscribeNotifications`), and then, **without
//! being asked**, sends an `ObjectChanged` for each subscription it holds, a little later.
//!
//! One uri, [`REFUSED`], is refused by the store. A store that notified for it anyway would be
//! faulty, and the server does exactly that, so a client that left a route behind would show it.
//!
//! Included by a test file with `mod live_server;`: not every test uses every item.

#![allow(dead_code)]

use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use retp_avro::Role;
use retp_client::live::{LiveSession, LiveSettings};
use retp_client::native::{Connect, Endpoint};
use retp_client::{Clock, SystemClock, reading_client};
use retp_proto::connection::{Connection, Declined, Event, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::probe;
use retp_proto::wire::{Body, Delivery, Outgoing};
use retp_types::datatypes::object::{DataObject, ObjectChange, ObjectChangeKind, Resource};
use retp_types::datatypes::{SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::dataspace::GetDataspacesResponse;
use retp_types::protocol::store_notification::{
    ObjectChanged, SubscribeNotificationsResponse, SubscriptionEnded,
};
use tungstenite::Message as Frame;
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::http::HeaderValue;

/// How long a test may take before it is declared hung.
pub const DEADLINE: Duration = Duration::from_secs(30);
/// How long the server waits after a subscription before it notifies.
pub const NOTIFY_AFTER: Duration = Duration::from_millis(200);
/// How long an arrival may take, from the subscription.
pub const ARRIVAL: Duration = Duration::from_secs(10);

/// The dataspace the server lists, and the context tests subscribe to.
pub const DATASPACE: &str = "eml:///dataspace('live')";
/// The object a notification names.
pub const OBJECT: &str = "eml:///resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)";
/// The context the store refuses to be subscribed to.
pub const REFUSED: &str = "eml:///dataspace('secret')";

/// The subscription the tests make: a uuid they choose, the server repeats it.
pub const SUBSCRIPTION: Uuid = Uuid([7; 16]);
/// A second one.
pub const OTHER_SUBSCRIPTION: Uuid = Uuid([8; 16]);

/// What the server holds, shared between its handler and its loop.
type Wanted = Arc<Mutex<Vec<Uuid>>>;

fn locked<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|p| p.into_inner())
}

// ---- the local server ----------------------------------------------------

/// The Dataspace (24) store: one dataspace, whatever is asked.
struct Dataspaces;

impl Handler for Dataspaces {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(24, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        let Body::Message(Message::GetDataspaces(_)) = &delivery.body else {
            return Err(Declined("only GetDataspaces here".into()));
        };
        let mut reply = responder.wire().begin_response(delivery);
        let response = GetDataspacesResponse {
            dataspaces: vec![retp_types::datatypes::object::Dataspace {
                uri: DATASPACE.into(),
                path: "live".into(),
                store_last_write: 1,
                store_created: 1,
                custom_data: Default::default(),
            }],
        };
        match responder.wire().last(&mut reply, &response) {
            Ok(out) => {
                responder.send_all(out);
                Ok(Handled::Answered)
            }
            Err(why) => Err(Declined(format!("{why:?}"))),
        }
    }
}

/// The StoreNotification (5) store: it confirms a subscription and keeps the request uuid, so
/// that the server loop can notify for it later, on its own; it ends one on request.
struct Subscriptions {
    wanted: Wanted,
}

impl Handler for Subscriptions {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(5, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        match &delivery.body {
            Body::Message(Message::SubscribeNotifications(request)) => {
                // Faulty on purpose: the uuid is kept (and notified for) even when refused.
                locked(&self.wanted).extend(request.request.values().map(|info| info.request_uuid));
                if request.request.values().any(|info| info.context.uri == REFUSED) {
                    return Err(Declined(format!("{REFUSED} may not be watched")));
                }
                let response = SubscribeNotificationsResponse {
                    success: request
                        .request
                        .keys()
                        .map(|k| (k.clone(), "subscribed".to_string()))
                        .collect(),
                };
                match responder.wire().respond(delivery, &response) {
                    Ok(out) => responder.send(out),
                    Err(why) => return Err(Declined(format!("{why:?}"))),
                }
                Ok(Handled::Answered)
            }
            Body::Message(Message::UnsubscribeNotifications(request)) => {
                locked(&self.wanted).retain(|uuid| *uuid != request.request_uuid);
                let ended = SubscriptionEnded {
                    reason: "unsubscribed".into(),
                    request_uuid: request.request_uuid,
                };
                match responder.wire().respond(delivery, &ended) {
                    Ok(out) => responder.send(out),
                    Err(why) => return Err(Declined(format!("{why:?}"))),
                }
                Ok(Handled::Answered)
            }
            _ => Err(Declined("only (Un)SubscribeNotifications here".into())),
        }
    }
}

/// A server connection with both stores behind it.
fn server_connection(wanted: Wanted) -> Connection {
    let mut offer = Offer::new("a live test store", "0.1", Uuid([2; 16]));
    offer.data_objects = vec![SupportedDataObject {
        qualified_type: "resqml20.*".into(),
        data_object_capabilities: Default::default(),
    }];
    let handlers: Vec<Box<dyn Handler>> = vec![Box::new(Dataspaces), Box::new(Subscriptions { wanted })];
    Connection::server(offer, handlers, None, Uuid([0xAB; 16]), SystemClock.now())
        .expect("the test server's offer is valid")
}

/// One `ObjectChanged` for `subscription`.
fn changed(subscription: Uuid) -> ObjectChanged {
    ObjectChanged {
        change: ObjectChange {
            change_kind: ObjectChangeKind::Update,
            change_time: 1,
            data_object: DataObject {
                resource: Resource {
                    uri: OBJECT.into(),
                    ..Default::default()
                },
                ..Default::default()
            },
        },
        request_uuid: subscription,
    }
}

fn send(socket: &mut tungstenite::WebSocket<TcpStream>, out: Vec<Outgoing>) -> bool {
    for message in out {
        if socket.send(Frame::Binary(message.bytes.into())).is_err() {
            return false;
        }
    }
    true
}

/// Serve one connection until it ends. Reads with a short timeout so that the server can also
/// act on its own: tick the connection, and send the notifications when they are due.
// The handshake callback's `Result` is tungstenite's signature, not ours.
#[allow(clippy::result_large_err)]
fn serve(stream: TcpStream, notify_after: Duration) {
    let callback = |_: &Request, mut response: Response| -> Result<Response, ErrorResponse> {
        response.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_static(probe::SUBPROTOCOL),
        );
        Ok(response)
    };
    let Ok(mut socket) = tungstenite::accept_hdr(stream, callback) else {
        return;
    };
    if socket
        .get_ref()
        .set_read_timeout(Some(Duration::from_millis(10)))
        .is_err()
    {
        return;
    }
    let wanted: Wanted = Arc::new(Mutex::new(Vec::new()));
    let mut connection = server_connection(wanted.clone());
    let mut due: Option<Instant> = None;
    let mut notified = false;
    loop {
        match socket.read() {
            Ok(Frame::Binary(bytes)) => {
                let step = connection.receive(&bytes, SystemClock.now());
                if !send(&mut socket, step.send) || step.close {
                    break;
                }
            }
            Ok(Frame::Close(_)) => break,
            Ok(_) => {}
            Err(tungstenite::Error::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                let step = connection.tick(SystemClock.now());
                if !send(&mut socket, step.send) || step.close {
                    break;
                }
            }
            Err(_) => break,
        }
        // A subscription arrived: the notification follows on its own.
        let asked = !locked(&wanted).is_empty();
        if asked && due.is_none() && !notified {
            due = Some(Instant::now() + notify_after);
        }
        if due.is_some_and(|at| Instant::now() >= at) {
            due = None;
            notified = true;
            let uuids: Vec<Uuid> = locked(&wanted).clone();
            for uuid in uuids {
                match connection.wire().notify(&changed(uuid)) {
                    Ok(out) => {
                        if !send(&mut socket, vec![out]) {
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
        }
    }
    let _ = socket.close(None);
}

/// A running local server: where it listens, and a signal when its one connection has ended.
pub struct Server {
    /// The `ws://` url to connect to.
    pub url: String,
    /// Receives once the connection has ended.
    pub ended: Receiver<()>,
}

/// Listen on a free port and serve the first connection, notifying [`NOTIFY_AFTER`] after the
/// first subscription.
pub fn start_server() -> Server {
    start_server_notifying_after(NOTIFY_AFTER)
}

/// [`start_server`] with its own delay before the notification.
pub fn start_server_notifying_after(notify_after: Duration) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free local port");
    let port = listener.local_addr().expect("a bound address").port();
    let (ended_tx, ended) = mpsc::channel();
    std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            serve(stream, notify_after);
        }
        let _ = ended_tx.send(());
    });
    Server {
        url: format!("ws://127.0.0.1:{port}"),
        ended,
    }
}

// ---- the client side -----------------------------------------------------

/// Open a live session on the server.
pub fn open(server: &Server) -> LiveSession {
    let (offer, handlers) = reading_client(
        "live test client",
        "0.1",
        Uuid([1; 16]),
        &["resqml20.*".to_string()],
    );
    let connect = Connect {
        endpoint: Endpoint::new(server.url.clone()),
        offer,
        handlers,
        max_message: None,
    };
    match LiveSession::connect(connect, LiveSettings::default()) {
        Ok(session) => session,
        Err(why) => panic!("the live session should open: {why:?}"),
    }
}

/// Run `body` on a thread of its own and fail, rather than hang, when it takes longer than
/// [`DEADLINE`]. A panic in `body` is the test's panic.
pub fn within<T: Send + 'static>(body: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let _ = done.send(body());
    });
    match result.recv_timeout(DEADLINE) {
        Ok(value) => {
            let _ = handle.join();
            value
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => match handle.join() {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(()) => panic!("the test body ended without a result"),
        },
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("the test did not finish within {DEADLINE:?}: it hangs")
        }
    }
}

/// Whether `event` is the `ObjectChanged` of `subscription`.
pub fn is_the_change_of(event: &Event, subscription: Uuid) -> bool {
    matches!(
        event,
        Event::Delivered(delivery)
            if matches!(&delivery.body, Body::Message(Message::ObjectChanged(c)) if c.request_uuid == subscription)
    )
}
