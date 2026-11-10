//! A refused WebSocket upgrade is a typed refusal: the status and the
//! `WWW-Authenticate` challenges, so that an application tells without reading
//! a sentence a credential to try again (401), a scope to give up on (403,
//! RFC 6750 section 3.1) and any other status.
//!
//! The server is a bare TCP listener written here, answering the upgrade
//! request with a plain HTTP response - what a gateway in front of an ETP
//! server does when it holds the authentication rule.

#![cfg(not(target_arch = "wasm32"))]

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};

use retp_client::native::{Connect, Endpoint, connect};
use retp_client::{ClientError, reading_client};
use retp_types::datatypes::Uuid;

/// A header the server sends that is not a challenge: it must reach nobody.
const NOT_A_CHALLENGE: &str = "Set-Cookie: session=fake-cookie-value-not-for-the-error";

/// Listen on a free port and answer the first upgrade request with `status`
/// and `headers`; the URL to connect to.
fn refusing(status: &str, headers: &[&str]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free local port");
    let port = listener.local_addr().expect("a bound address").port();
    let response = {
        let mut text = format!("HTTP/1.1 {status}\r\n");
        for header in headers {
            text.push_str(header);
            text.push_str("\r\n");
        }
        text.push_str("Content-Length: 0\r\nConnection: close\r\n\r\n");
        text
    };
    std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            answer(stream, &response);
        }
    });
    format!("ws://127.0.0.1:{port}")
}

/// Read the request up to its blank line, then answer.
fn answer(mut stream: TcpStream, response: &str) {
    let mut reader = BufReader::new(stream.try_clone().expect("a second handle"));
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) if line == "\r\n" => break,
            Ok(_) => {}
        }
    }
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn open(url: String) -> ClientError {
    let (offer, handlers) = reading_client(
        "handshake test client",
        "0.1",
        Uuid([1; 16]),
        &["resqml20.*".to_string()],
    );
    let attempt = connect(Connect {
        endpoint: Endpoint::new(url),
        offer,
        handlers,
        max_message: None,
    });
    match attempt {
        Ok(_) => panic!("the server refused the upgrade: no session can open"),
        Err(why) => why,
    }
}

fn assert_clean(error: &ClientError) {
    let shown = format!("{error} {error:?}");
    assert!(
        !shown.contains("fake-cookie-value"),
        "only the challenges of the answer are kept, not its other headers: {shown}"
    );
}

/// 401: the credential is missing or refused. Every `WWW-Authenticate` header
/// is a challenge, in the order sent, and nothing else of the answer is kept.
#[test]
fn a_401_refusal_carries_its_status_and_its_challenges() {
    let url = refusing(
        "401 Unauthorized",
        &[
            "WWW-Authenticate: Bearer realm=\"etp\", error=\"invalid_token\"",
            "WWW-Authenticate: Basic realm=\"etp\"",
            NOT_A_CHALLENGE,
        ],
    );
    let error = open(url);
    match &error {
        ClientError::HandshakeRefused { status, challenges } => {
            assert_eq!(*status, 401);
            assert_eq!(
                challenges,
                &[
                    "Bearer realm=\"etp\", error=\"invalid_token\"",
                    "Basic realm=\"etp\""
                ]
            );
        }
        other => panic!("a typed refusal was expected: {other:?}"),
    }
    assert!(!error.is_connection_lost(), "{error:?}");
    assert_clean(&error);
}

/// 403: the credential is understood and its scope is not enough. Same
/// variant, the status tells it from a 401; no challenge here.
#[test]
fn a_403_refusal_carries_its_status() {
    let error = open(refusing("403 Forbidden", &[NOT_A_CHALLENGE]));
    match &error {
        ClientError::HandshakeRefused { status, challenges } => {
            assert_eq!(*status, 403);
            assert!(challenges.is_empty(), "{challenges:?}");
        }
        other => panic!("a typed refusal was expected: {other:?}"),
    }
    assert_clean(&error);
}

/// Any other status is typed too, not a text: the application sees a server
/// failing or an address that is wrong, and nothing about a credential.
#[test]
fn another_status_is_typed_too() {
    let error = open(refusing("500 Internal Server Error", &[]));
    match &error {
        ClientError::HandshakeRefused { status, challenges } => {
            assert_eq!(*status, 500);
            assert!(challenges.is_empty(), "{challenges:?}");
        }
        other => panic!("a typed refusal was expected: {other:?}"),
    }
    assert_clean(&error);
}

/// A server that is not there is still a transport failure, not a refusal:
/// the typed variant says "the server answered, and said no".
#[test]
fn an_unreachable_server_is_not_a_handshake_refusal() {
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a free local port");
        listener.local_addr().expect("a bound address").port()
    };
    let error = open(format!("ws://127.0.0.1:{port}"));
    assert!(matches!(error, ClientError::Transport(_)), "{error:?}");
}

/// The event-driven session refuses the same way, with the same variant.
#[cfg(feature = "live")]
#[test]
fn the_live_session_refuses_the_same_way() {
    use retp_client::live::{LiveSession, LiveSettings};

    let url = refusing("401 Unauthorized", &["WWW-Authenticate: Bearer realm=\"etp\""]);
    let (offer, handlers) = reading_client(
        "handshake test client",
        "0.1",
        Uuid([1; 16]),
        &["resqml20.*".to_string()],
    );
    let attempt = LiveSession::connect(
        Connect {
            endpoint: Endpoint::new(url),
            offer,
            handlers,
            max_message: None,
        },
        LiveSettings::default(),
    );
    match attempt {
        Err(ClientError::HandshakeRefused { status, challenges }) => {
            assert_eq!(status, 401);
            assert_eq!(challenges, ["Bearer realm=\"etp\""]);
        }
        Err(other) => panic!("a typed refusal was expected: {other:?}"),
        Ok(_) => panic!("the server refused the upgrade: no session can open"),
    }
}
