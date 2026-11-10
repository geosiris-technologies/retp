//! The live session with credentials from a source: opened with it, renewed
//! with it, and the source never called on the session's own thread.
//!
//! The server is a real WebSocket on `127.0.0.1` (the crate's own `tungstenite`,
//! in its server role) with a server `Connection` behind it that asks for
//! authorization, and an HTTP layer under it that answers the upgrade `401` or
//! `403` to the credentials it is told to refuse. The rules come from the
//! specification:
//!
//! - §3.13.5 (p.90): an endpoint re-authorizes BEFORE the authorization expires
//!   or the server sends `EAUTHORIZATION_EXPIRED` then `CloseSession`;
//! - §4.1.4 (p.95): the handshake carries the `Authorization` header, and the
//!   server may refuse the upgrade with an HTTP status (RFC 6750 section 3:
//!   401 for a credential to try again, 403 for a scope to give up on);
//! - §5.3.4 and §5.3.7 (p.121, p.123): `Authorize` and its answer.
//!
//! Every test runs under a deadline, so a hang fails instead of blocking CI.
//! The first authorization of the server is short (a second): the tests that
//! wait past it wait in real time, a little under two seconds each.

#![cfg(all(feature = "live", not(target_arch = "wasm32")))]

use std::collections::VecDeque;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use std::time::Duration;

use retp_avro::Role;
use retp_client::live::{Filter, LiveSession, LiveSettings, Queue};
use retp_client::native::{Connect, Endpoint};
use retp_client::{
    ClientError, Clock, CredentialSource, Fresh, GaveUp, Refused, SourceError, SystemClock, Witness,
};
use retp_proto::connection::{Authorization, Authorizer, Connection, Event, Forward};
use retp_proto::credentials::Credentials;
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::probe;
use retp_proto::wire::Outgoing;
use retp_types::datatypes::{SupportedDataObject, Uuid};
use retp_types::protocol::discovery::GetResources;
use tungstenite::Message as Frame;
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::http::HeaderValue;

const DEADLINE: Duration = Duration::from_secs(30);
const S: i64 = 1_000_000;

/// How long the short token lasts, and how long a test waits to be sure it
/// has run out.
const SHORT: i64 = S;
const PAST_SHORT: Duration = Duration::from_millis(1800);

// ---- the local server ----------------------------------------------------

fn server_offer() -> Offer {
    let mut offer = Offer::new("a live test store", "0.1", Uuid([2; 16]));
    offer.data_objects = vec![SupportedDataObject {
        qualified_type: "resqml20.*".into(),
        data_object_capabilities: Default::default(),
    }];
    offer
}

/// `ok-short` runs out a second after it is granted, `ok-long` much later;
/// anything else is refused with a challenge.
fn authorizer() -> Box<dyn Authorizer> {
    Box::new(|authorization: &str, now: i64| match authorization {
        "Bearer ok-short" => Authorization::Granted {
            expires_at: Some(now + SHORT),
        },
        "Bearer ok-long" => Authorization::Granted {
            expires_at: Some(now + 1000 * S),
        },
        _ => Authorization::Challenge(vec![r#"Bearer authz_server="https://auth.example""#.into()]),
    })
}

fn send(socket: &mut tungstenite::WebSocket<TcpStream>, out: Vec<Outgoing>) -> bool {
    for message in out {
        if socket.send(Frame::Binary(message.bytes.into())).is_err() {
            return false;
        }
    }
    true
}

fn refusal(status: u16, challenge: Option<&str>) -> ErrorResponse {
    let mut builder = tungstenite::http::Response::builder().status(status);
    if let Some(challenge) = challenge {
        builder = builder.header("www-authenticate", challenge);
    }
    builder.body(None).expect("a response")
}

// The handshake callback's `Result` is tungstenite's signature, not ours.
#[allow(clippy::result_large_err)]
fn serve(stream: TcpStream) {
    let callback = |request: &Request, mut response: Response| -> Result<Response, ErrorResponse> {
        let header = request
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        match header {
            "Bearer expired" => Err(refusal(401, Some(r#"Bearer realm="etp", error="invalid_token""#))),
            "Bearer forbidden" => Err(refusal(403, None)),
            _ => {
                response.headers_mut().insert(
                    "sec-websocket-protocol",
                    HeaderValue::from_static(probe::SUBPROTOCOL),
                );
                Ok(response)
            }
        }
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
    let mut connection = Connection::server(
        server_offer(),
        vec![Box::new(Forward(ProtocolOffer::v12(3, Role::Store)))],
        Some(authorizer()),
        Uuid([0xAB; 16]),
        SystemClock.now(),
    )
    .expect("the test server's offer is valid");
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
    }
    let _ = socket.close(None);
}

/// Listen on a free port and serve every connection, each on its own thread.
fn start_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free local port");
    let port = listener.local_addr().expect("a bound address").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            std::thread::spawn(move || serve(stream));
        }
    });
    format!("ws://127.0.0.1:{port}")
}

// ---- the source ----------------------------------------------------------

/// Who called the source: a thread, and its name.
type Caller = (ThreadId, Option<String>);

/// A source that gives the tokens it is scripted to give, in order, with the
/// token as the witness of what it gave (the tokens are fake strings), and
/// records which witnesses came back and which thread each call ran on.
struct Script {
    give: Mutex<VecDeque<&'static str>>,
    refused: Mutex<Vec<String>>,
    callers: Mutex<Vec<Caller>>,
}

impl Script {
    fn new(tokens: &[&'static str]) -> Arc<Self> {
        Arc::new(Self {
            give: Mutex::new(tokens.iter().copied().collect()),
            refused: Mutex::new(Vec::new()),
            callers: Mutex::new(Vec::new()),
        })
    }

    fn called(&self) {
        let here = std::thread::current();
        self.callers
            .lock()
            .unwrap()
            .push((here.id(), here.name().map(str::to_string)));
    }

    fn next(&self) -> Option<Fresh> {
        let token = self.give.lock().unwrap().pop_front()?;
        Some(Fresh {
            credentials: Credentials::bearer(token).unwrap(),
            witness: Witness::new(token.to_string()),
            valid_until: None,
        })
    }

    fn refusals(&self) -> Vec<String> {
        self.refused.lock().unwrap().clone()
    }
}

impl CredentialSource for Script {
    fn credentials(&self) -> Result<Fresh, SourceError> {
        self.called();
        self.next()
            .ok_or_else(|| SourceError("nothing left to give".into()))
    }

    fn refused(&self, witness: Witness, _why: &Refused) -> Result<Fresh, GaveUp> {
        self.called();
        let token = witness.downcast::<String>().expect("the witness it gave");
        self.refused.lock().unwrap().push(token);
        self.next().ok_or_else(|| GaveUp("nothing else to try".into()))
    }
}

// ---- the client side -----------------------------------------------------

fn connect_to(url: &str) -> Connect {
    let mut offer = Offer::new("live credentials test", "0.1", Uuid([1; 16]));
    offer.data_objects = vec![SupportedDataObject {
        qualified_type: "resqml20.*".into(),
        data_object_capabilities: Default::default(),
    }];
    Connect {
        endpoint: Endpoint::new(url.to_string()),
        offer,
        handlers: vec![Box::new(Forward(ProtocolOffer::v12(3, Role::Customer)))],
        max_message: None,
    }
}

fn open(url: &str, source: &Arc<Script>) -> Result<LiveSession, ClientError> {
    let url = url.to_string();
    LiveSession::connect_with_source(
        source.clone() as Arc<dyn CredentialSource>,
        move || connect_to(&url),
        LiveSettings::default(),
    )
}

/// Run `body` on a thread of its own and fail, rather than hang, when it takes longer than
/// [`DEADLINE`]. A panic in `body` is the test's panic.
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

/// Whether the session still answers: a `Ping` is native, and the session
/// answers it as long as it is open.
fn answers(session: &LiveSession) -> bool {
    session.block_on(session.client().ping()).is_ok()
}

/// Whether the session is over: a request on it is refused by name.
fn is_over(session: &LiveSession) -> bool {
    matches!(
        session.block_on(session.client().call(&GetResources::default())),
        Err(ClientError::Closed { .. } | ClientError::Transport(_))
    )
}

// ---- the tests -----------------------------------------------------------

/// The test that proves renewal matters: opened with the short token and
/// renewed from the source, the session answers past the moment the short
/// token ran out. The next test is the same without the renewal.
#[test]
fn a_renewed_live_session_outlives_its_first_authorization() {
    let url = start_server();
    within(move || {
        let source = Script::new(&["ok-short", "ok-long"]);
        let session = open(&url, &source).expect("the session opens");
        assert!(answers(&session));

        session.renew().expect("the renewal is accepted");

        std::thread::sleep(PAST_SHORT);
        assert!(
            answers(&session),
            "the same session answers past the short token's end"
        );
        assert!(source.refusals().is_empty());
        session.close("done").unwrap();
    });
}

/// Nobody renews: the server closes the session at the end of the short token
/// (§3.13.5 point 3), and a request on it is refused by name.
#[test]
fn without_a_renewal_the_server_closes_the_live_session() {
    let url = start_server();
    within(move || {
        let source = Script::new(&["ok-short", "ok-long"]);
        let session = open(&url, &source).expect("the session opens");
        assert!(answers(&session));

        std::thread::sleep(PAST_SHORT);

        assert!(is_over(&session), "the server closed it at the deadline");
    });
}

/// A refused renewal says so with the challenges, as the event of the session
/// and as the error of the call, goes back to the source once, and leaves the
/// session open.
#[test]
fn a_refused_renewal_is_an_event_with_its_challenges() {
    let url = start_server();
    within(move || {
        let source = Script::new(&["ok-long", "stale", "staler", "ok-long"]);
        let session = open(&url, &source).expect("the session opens");
        let (queue, events) = Queue::bounded(64);
        session.route(Filter::Session, Arc::new(queue));

        let result = session.renew();

        match result {
            Err(ClientError::AuthorizationNeeded { challenges }) => {
                assert!(challenges[0].contains("authz_server"), "{challenges:?}");
            }
            other => panic!("the renewal is refused, twice: {other:?}"),
        }
        assert_eq!(
            source.refusals(),
            ["stale"],
            "the source is told once, about what was refused"
        );

        // Both refusals reach whoever listens to the session, with their
        // challenges and no credential.
        let mut refused = 0;
        while let Ok(event) = events.recv_timeout(Duration::from_secs(5)) {
            let shown = format!("{event:?}");
            assert!(!shown.contains("stale"), "no credential in an event: {shown}");
            if let Event::AuthorizationRefused { challenges, .. } = event {
                assert!(challenges[0].contains("authz_server"));
                refused += 1;
                if refused == 2 {
                    break;
                }
            }
        }
        assert_eq!(refused, 2);
        assert!(answers(&session), "the session is still open");
    });
}

/// The source runs on the thread of the caller - to open, to renew - and
/// never on the session's task or its dispatcher, where a source that blocks
/// would stop the keepalive and every answer.
#[test]
fn the_source_is_never_called_on_the_thread_of_the_session() {
    let url = start_server();
    within(move || {
        let me = std::thread::current().id();
        let source = Script::new(&["ok-long", "stale", "ok-long"]);
        let session = open(&url, &source).expect("the session opens");
        session
            .renew()
            .expect("the renewal is accepted after one refusal");

        let callers = source.callers.lock().unwrap().clone();
        // credentials (open), credentials (renew), refused (renew), no more.
        assert_eq!(callers.len(), 3, "{callers:?}");
        for (thread, name) in &callers {
            assert_eq!(*thread, me, "the caller's thread: {name:?}");
            assert!(
                !name.as_deref().is_some_and(|n| n.starts_with("retp-live")),
                "never the session's: {name:?}"
            );
        }
    });
}

/// A 401 on the upgrade goes to the source once, with the witness of the
/// credentials it refused; what the source gives next opens the session.
#[test]
fn a_401_at_the_handshake_goes_through_refused_once() {
    let url = start_server();
    within(move || {
        let source = Script::new(&["expired", "ok-long"]);
        let session = open(&url, &source).expect("the second credentials open it");
        assert_eq!(source.refusals(), ["expired"]);
        assert!(answers(&session));
    });
}

/// A second 401 is the error, typed, and the source is not asked again.
#[test]
fn a_second_401_at_the_handshake_is_the_error() {
    let url = start_server();
    within(move || {
        let source = Script::new(&["expired", "expired", "ok-long"]);
        match open(&url, &source) {
            Err(ClientError::HandshakeRefused { status, challenges }) => {
                assert_eq!(status, 401);
                assert!(challenges[0].contains("invalid_token"), "{challenges:?}");
            }
            Err(other) => panic!("a typed refusal expected: {other:?}"),
            Ok(_) => panic!("the server refused both"),
        }
        assert_eq!(source.refusals(), ["expired"], "told once");
        assert_eq!(
            source.give.lock().unwrap().len(),
            1,
            "the third was never asked for"
        );
    });
}

/// A 403 is a scope the credentials do not reach: more credentials of the
/// same kind will not widen it, so the source is not asked.
#[test]
fn a_403_at_the_handshake_is_not_retried() {
    let url = start_server();
    within(move || {
        let source = Script::new(&["forbidden", "ok-long"]);
        match open(&url, &source) {
            Err(ClientError::HandshakeRefused { status, .. }) => assert_eq!(status, 403),
            Err(other) => panic!("a typed refusal expected: {other:?}"),
            Ok(_) => panic!("the server refused it"),
        }
        assert!(
            source.refusals().is_empty(),
            "a 403 does not go back to the source"
        );
    });
}

/// A session opened without a source says so by name when asked to renew
/// from one; `reauthorize` with credentials works on it all the same.
#[test]
fn a_session_without_a_source_renews_with_credentials() {
    let url = start_server();
    within(move || {
        let mut connect = connect_to(&url);
        connect.endpoint.credentials = Some(Credentials::bearer("ok-short").unwrap());
        let session = LiveSession::connect(connect, LiveSettings::default()).expect("opens");

        assert!(matches!(session.renew(), Err(ClientError::Invalid(_))));
        session
            .reauthorize(&Credentials::bearer("ok-long").unwrap())
            .expect("accepted");
        std::thread::sleep(PAST_SHORT);
        assert!(answers(&session));
    });
}
