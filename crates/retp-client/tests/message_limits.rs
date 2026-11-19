//! What a server may make this client buffer: a WebSocket message or an HTTP
//! body larger than the limit is refused by name, before it is held in
//! memory, and one under the limit comes through as before.
//!
//! The limits are this crate's own choice (`DEFAULT_MAX_WEBSOCKET_MESSAGE`,
//! `DEFAULT_MAX_HTTP_BODY`), not a figure read in a specification, so the
//! tests check the behaviour at a small limit against servers written here.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use retp_client::Transport;
use retp_client::native::{CertificateCheck, UreqHttp, WebSocketTransport};
use retp_client::rest::{Http, HttpRequest, Method};
use retp_proto::probe::{Handshake, Profile, SUBPROTOCOL};

/// A WebSocket server that sends one binary message of `size` bytes to the
/// first client, then waits for it to leave.
fn websocket_server(size: usize) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().expect("an address").port();
    std::thread::spawn(move || {
        let Ok((stream, _)) = listener.accept() else {
            return;
        };
        #[allow(clippy::result_large_err)] // the callback's signature is the library's
        fn answer_with_protocol(
            _: &tungstenite::handshake::server::Request,
            mut response: tungstenite::handshake::server::Response,
        ) -> Result<tungstenite::handshake::server::Response, tungstenite::handshake::server::ErrorResponse>
        {
            response
                .headers_mut()
                .insert("sec-websocket-protocol", SUBPROTOCOL.parse().expect("a header"));
            Ok(response)
        }
        let Ok(mut socket) = tungstenite::accept_hdr(stream, answer_with_protocol) else {
            return;
        };
        let _ = socket.send(tungstenite::Message::Binary(vec![7u8; size].into()));
        let _ = socket.flush();
        std::thread::sleep(Duration::from_millis(500));
    });
    port
}

fn handshake(port: u16, headers: Vec<(String, String)>) -> Handshake {
    Handshake {
        url: format!("ws://127.0.0.1:{port}/"),
        subprotocols: vec![SUBPROTOCOL],
        headers,
        authorize_after_connecting: None,
        profile: Profile::Native,
    }
}

#[test]
fn a_websocket_message_above_the_limit_is_refused_by_name() {
    let port = websocket_server(5_000);
    let mut socket =
        WebSocketTransport::connect_limited(&handshake(port, Vec::new()), CertificateCheck::Verify, 1_000)
            .expect("connected");
    let refused = socket
        .receive(Duration::from_secs(5))
        .expect_err("a 5000 byte message is above a 1000 byte limit")
        .to_string();
    assert!(refused.contains("1000"), "says the limit: {refused}");
    assert!(
        refused.contains("MaxWebSocketMessagePayloadSize"),
        "says how to raise it: {refused}"
    );
}

#[test]
fn a_websocket_message_under_the_limit_comes_through() {
    let port = websocket_server(5_000);
    let mut socket =
        WebSocketTransport::connect_limited(&handshake(port, Vec::new()), CertificateCheck::Verify, 10_000)
            .expect("connected");
    let message = socket.receive(Duration::from_secs(5)).expect("a message");
    assert_eq!(message, Some(vec![7u8; 5_000]));
}

/// The limit a connection enforces is the one its handshake announces.
#[test]
fn the_announced_message_size_is_the_limit() {
    let port = websocket_server(5_000);
    let announced = vec![("maxwebsocketmessagepayloadsize".to_string(), "1000".to_string())];
    let mut socket = WebSocketTransport::connect(&handshake(port, announced), CertificateCheck::Verify)
        .expect("connected");
    let refused = socket
        .receive(Duration::from_secs(5))
        .expect_err("above what the handshake announced")
        .to_string();
    assert!(refused.contains("1000"), "{refused}");
}

/// A server that answers every request with a body of `size` bytes, with a
/// `content-length`, or without (the body then ends with the connection).
fn http_server(size: usize, with_length: bool) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().expect("an address").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            std::thread::spawn(move || serve(stream, size, with_length));
        }
    });
    port
}

fn serve(stream: TcpStream, size: usize, with_length: bool) {
    let mut reader = BufReader::new(stream.try_clone().expect("the same socket"));
    let mut stream = stream;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line.trim().is_empty() {
            break;
        }
    }
    let length = if with_length {
        format!("content-length: {size}\r\n")
    } else {
        "connection: close\r\n".to_string()
    };
    let head = format!("HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\n{length}\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&vec![b'x'; size]);
    let _ = stream.flush();
}

fn get(port: u16) -> HttpRequest {
    HttpRequest {
        method: Method::Get,
        url: format!("http://127.0.0.1:{port}/"),
        headers: Vec::new(),
        body: None,
    }
}

fn now<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("the native transport answers at its first poll"),
    }
}

#[test]
fn an_http_body_above_the_limit_is_refused_by_name() {
    for with_length in [true, false] {
        let port = http_server(5_000, with_length);
        let mut http = UreqHttp::new(CertificateCheck::Verify).with_max_body(1_000);
        let refused = now(http.send(get(port)))
            .expect_err("a 5000 byte body is above a 1000 byte limit")
            .to_string();
        assert!(
            refused.contains("1000"),
            "says the limit (length {with_length}): {refused}"
        );
        assert!(
            refused.contains("with_max_body"),
            "says how to raise it: {refused}"
        );
    }
}

#[test]
fn an_http_body_under_the_limit_comes_through() {
    for with_length in [true, false] {
        let port = http_server(5_000, with_length);
        let mut http = UreqHttp::new(CertificateCheck::Verify).with_max_body(10_000);
        let answer = now(http.send(get(port))).expect("an answer");
        assert_eq!(answer.body, vec![b'x'; 5_000]);
    }
}
