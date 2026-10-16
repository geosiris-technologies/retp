//! The event-driven native session (`retp_client::live`), against a local
//! WebSocket server.
//!
//! The server is a real WebSocket on `127.0.0.1` (the crate's own `tungstenite`,
//! in its server role) with a server [`Connection`] behind it. It opens the
//! session, lists one dataspace, confirms a subscription, and then, **without
//! being asked**, sends an `ObjectChanged` for that subscription a little
//! later. The point of the whole feature is that the client receives it with
//! nothing calling the session: nothing polls, yet it arrives.
//!
//! Every test runs under a deadline, so a hang fails instead of blocking CI.

#![cfg(all(feature = "live", not(target_arch = "wasm32")))]

use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use retp_avro::Role;
use retp_client::live::{Callback, Filter, JsonLines, LiveSession, LiveSettings, Queue};
use retp_client::native::{Connect, Endpoint};
use retp_client::notifications::{Watch, uuid_text};
use retp_client::{Clock, SystemClock, asynchronous, reading_client};
use retp_proto::connection::{Connection, Declined, Event, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::probe;
use retp_proto::wire::{Body, Delivery, Outgoing};
use retp_types::datatypes::object::{DataObject, ObjectChange, ObjectChangeKind, Resource};
use retp_types::datatypes::{SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::dataspace::GetDataspacesResponse;
use retp_types::protocol::store_notification::{ObjectChanged, SubscribeNotificationsResponse};
use tungstenite::Message as Frame;
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::http::HeaderValue;

/// How long a test may take before it is declared hung.
const DEADLINE: Duration = Duration::from_secs(30);
/// How long the server waits after a subscription before it notifies.
const NOTIFY_AFTER: Duration = Duration::from_millis(200);
/// How long an arrival may take, from the subscription.
const ARRIVAL: Duration = Duration::from_secs(10);

const DATASPACE: &str = "eml:///dataspace('live')";
const OBJECT: &str = "eml:///resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)";

/// The subscription the tests make: a uuid they choose, the server repeats it.
const SUBSCRIPTION: Uuid = Uuid([7; 16]);

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

/// The StoreNotification (5) store: it confirms a subscription and keeps the
/// request uuid, so that the server loop can notify for it later, on its own.
struct Subscriptions {
    wanted: Arc<Mutex<Vec<Uuid>>>,
}

impl Handler for Subscriptions {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(5, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        let Body::Message(Message::SubscribeNotifications(request)) = &delivery.body else {
            return Err(Declined("only SubscribeNotifications here".into()));
        };
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
        let mut wanted = self.wanted.lock().unwrap_or_else(|p| p.into_inner());
        wanted.extend(request.request.values().map(|info| info.request_uuid));
        Ok(Handled::Answered)
    }
}

/// A server connection with both stores behind it.
fn server_connection(wanted: Arc<Mutex<Vec<Uuid>>>) -> Connection {
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

/// Serve one connection until it ends. Reads with a short timeout so that
/// the server can also act on its own: tick the connection, and send the
/// notification when it is due.
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
    let wanted = Arc::new(Mutex::new(Vec::new()));
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
        // The subscription was confirmed: the notification follows on its own.
        let asked = !wanted.lock().unwrap_or_else(|p| p.into_inner()).is_empty();
        if asked && due.is_none() && !notified {
            due = Some(Instant::now() + notify_after);
        }
        if due.is_some_and(|at| Instant::now() >= at) {
            due = None;
            notified = true;
            let uuids: Vec<Uuid> = wanted.lock().unwrap_or_else(|p| p.into_inner()).clone();
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

/// A running local server: where it listens, and a signal when its one
/// connection has ended.
struct Server {
    url: String,
    ended: Receiver<()>,
}

/// Listen on a free port and serve the first connection.
fn start_server() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free local port");
    let port = listener.local_addr().expect("a bound address").port();
    let (ended_tx, ended) = mpsc::channel();
    std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            serve(stream, NOTIFY_AFTER);
        }
        let _ = ended_tx.send(());
    });
    Server {
        url: format!("ws://127.0.0.1:{port}"),
        ended,
    }
}

// ---- the client side -----------------------------------------------------

fn open(server: &Server) -> LiveSession {
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

/// Run `body` on a thread of its own and fail, rather than hang, when it
/// takes longer than [`DEADLINE`]. A panic in `body` is the test's panic.
fn within<T: Send + 'static>(body: impl FnOnce() -> T + Send + 'static) -> T {
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

/// The dataspaces the server lists, through the crate's own operation.
fn dataspaces(session: &LiveSession) -> Vec<String> {
    let mut client = session.client().clone();
    let list = session
        .block_on(asynchronous::list_dataspaces(&mut client))
        .expect("GetDataspaces is answered");
    list.items.into_iter().map(|d| d.uri).collect()
}

/// Subscribe to the test object's dataspace with [`SUBSCRIPTION`].
fn subscribe(session: &LiveSession) {
    let mut client = session.client().clone();
    let watches = [Watch::of(DATASPACE, SUBSCRIPTION)];
    let outcomes = session
        .block_on(asynchronous::subscribe_notifications(&mut client, &watches))
        .expect("SubscribeNotifications is answered");
    assert!(
        outcomes.iter().all(Result::is_ok),
        "the subscription is confirmed"
    );
}

/// Whether `event` is the `ObjectChanged` of [`SUBSCRIPTION`].
fn is_the_change(event: &Event) -> bool {
    matches!(
        event,
        Event::Delivered(delivery)
            if matches!(&delivery.body, Body::Message(Message::ObjectChanged(c)) if c.request_uuid == SUBSCRIPTION)
    )
}

/// A file name no other test (nor another run) uses.
fn temp_file(label: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNT: AtomicU32 = AtomicU32::new(0);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    std::env::temp_dir().join(format!(
        "retp-live-{label}-{}-{}-{stamp}.jsonl",
        std::process::id(),
        COUNT.fetch_add(1, Ordering::Relaxed)
    ))
}

// ---- the tests -----------------------------------------------------------

/// The session opens and an operation of the crate runs on it from
/// synchronous code.
#[test]
fn the_live_session_opens_and_lists_the_servers_dataspace() {
    within(|| {
        let server = start_server();
        let session = open(&server);
        assert_eq!(dataspaces(&session), vec![DATASPACE.to_string()]);
        session.close("done").expect("the session closes");
    });
}

/// The point of the feature: the server notifies on its own, nothing is
/// called on the session, and the notification reaches the queue.
#[test]
fn a_notification_the_server_sends_unasked_reaches_a_queue_with_nothing_polling() {
    within(|| {
        let server = start_server();
        let session = open(&server);
        let (queue, arrivals) = Queue::bounded(16);
        // Routed before the subscription is made, so the arrival cannot beat
        // the route; the route is by the uuid the test chooses.
        session.route(Filter::Subscription(SUBSCRIPTION), Arc::new(queue));
        subscribe(&session);
        // From here the test calls nothing on the session.
        let event = arrivals
            .recv_timeout(ARRIVAL)
            .expect("the notification arrives without anything polling");
        assert!(
            is_the_change(&event),
            "it is the ObjectChanged of the subscription: {event:?}"
        );
        session.close("done").expect("the session closes");
    });
}

/// The same arrival, written as one JSON line.
#[test]
fn a_notification_the_server_sends_unasked_is_written_as_one_json_line() {
    within(|| {
        let path = temp_file("lines");
        let server = start_server();
        let session = open(&server);
        let destination = JsonLines::create(&path).expect("the file is created");
        session.route(Filter::Subscription(SUBSCRIPTION), Arc::new(destination));
        subscribe(&session);
        // Wait for the line, from outside the session: the file is the only
        // thing observed.
        let started = Instant::now();
        let text = loop {
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            if text.ends_with('\n') {
                break text;
            }
            assert!(started.elapsed() < ARRIVAL, "no line was written in {ARRIVAL:?}");
            std::thread::sleep(Duration::from_millis(20));
        };
        session.close("done").expect("the session closes");
        let _ = std::fs::remove_file(&path);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1, "one arrival, one line: {text:?}");
        let value: serde_json::Value = serde_json::from_str(lines[0]).expect("the line is valid JSON");
        assert_eq!(value["message"], "ObjectChanged");
        assert_eq!(value["protocol"], 5);
        assert_eq!(value["subscription"], uuid_text(&SUBSCRIPTION));
    });
}

/// A request sent while notifications are routed gets its answer, before and
/// after a notification arrived.
#[test]
fn a_request_still_gets_its_answer_while_notifications_are_routed() {
    within(|| {
        let server = start_server();
        let session = open(&server);
        let (queue, arrivals) = Queue::bounded(16);
        session.route(Filter::Subscription(SUBSCRIPTION), Arc::new(queue));
        subscribe(&session);
        assert_eq!(dataspaces(&session).len(), 1, "answered before the notification");
        let event = arrivals.recv_timeout(ARRIVAL).expect("the notification arrives");
        assert!(is_the_change(&event));
        assert_eq!(
            dataspaces(&session),
            vec![DATASPACE.to_string()],
            "answered after the notification"
        );
        session.close("done").expect("the session closes");
    });
}

/// `close` returns `Ok`, and the socket is really closed: the server sees its
/// connection end. The threads stopping is checked by the test finishing.
#[test]
fn closing_the_session_ends_cleanly_and_the_server_sees_it_end() {
    within(|| {
        let server = start_server();
        let session = open(&server);
        assert_eq!(dataspaces(&session).len(), 1);
        session.close("test over").expect("close returns Ok");
        server
            .ended
            .recv_timeout(ARRIVAL)
            .expect("the server saw the connection end");
    });
}

/// A destination that fails is dropped by the session, which stays usable.
#[test]
fn a_destination_returning_an_error_does_not_break_the_session() {
    within(|| {
        let server = start_server();
        let session = open(&server);
        let (called, was_called) = mpsc::channel();
        let called = Mutex::new(called);
        let failing = Callback::new(move |_event: &Event| {
            if let Ok(called) = called.lock() {
                let _ = called.send(());
            }
            Err("this destination always fails".to_string())
        });
        session.route(Filter::Subscription(SUBSCRIPTION), Arc::new(failing));
        subscribe(&session);
        was_called
            .recv_timeout(ARRIVAL)
            .expect("the failing destination was called once");
        assert_eq!(
            dataspaces(&session),
            vec![DATASPACE.to_string()],
            "a request still answers after the destination failed"
        );
        session.close("done").expect("the session closes");
    });
}
