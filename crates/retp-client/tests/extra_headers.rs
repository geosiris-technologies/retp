//! The headers a server wants in addition (`ExtraHeaders`): what is refused
//! when one is added, where a handshake puts them, and that each one is on the
//! wire with every request - the WebSocket handshake, the probe of
//! `/.well-known/etp-server-capabilities`, a call of the REST API.
//!
//! ETP says nothing of a header that is a server's own; what it says of its
//! own attributes is followed for these too, and HTTP says the rest:
//!
//! - ETP 1.2 §4.3 step 4 (p.97): the attributes of the WebSocket handshake
//!   are header properties, and (4.c) "All protocol header names and values
//!   MUST be lower case" - the names are sent in lower case;
//! - ETP 1.2 §4.3.2 (p.102): a browser-based client cannot add request
//!   headers, so "it MUST specify them as query string parameters",
//!   URL-encoded;
//! - RFC 9110 §5.1 and §5.5: a field name is a token, a field value holds no
//!   control character (a line break in it would start another field);
//! - RFC 6455 §11.3: the `sec-websocket-*` headers are the handshake's own.
//!
//! The server is a bare TCP listener written here: it keeps the request it
//! received and answers `401`, which is enough - what is checked is what the
//! client sent.

#![cfg(not(target_arch = "wasm32"))]

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::Duration;

use retp_client::native::{self, Connect, Endpoint, ProbeError, Remote, connect, fetch_server_capabilities};
use retp_client::{ClientError, ExtraHeaders, HeaderRefusal, reading_client, sensitive_header};
use retp_proto::credentials::Credentials;
use retp_proto::probe::{self, HandshakeSettings, Profile};
use retp_types::datatypes::Uuid;

/// A value that must be nowhere but on the wire.
const SECRET: &str = "SECRET-91c4-must-appear-nowhere";

const WAIT: Duration = Duration::from_secs(20);

// ---- what is refused when a header is added --------------------------------

#[test]
fn a_name_that_is_not_a_header_name_is_refused() {
    for name in [
        "",
        "x tenant",
        "x:tenant",
        "x-tenant\r\nx-other",
        "é-tenant",
        "x=tenant",
    ] {
        let refusal = ExtraHeaders::new().with(name, "acme").expect_err(name);
        assert_eq!(refusal, HeaderRefusal::Name);
    }
    // What is given as a name may be a value (`-H "X-Api-Key: abc=="`, split at the `=`): it is not shown.
    let refusal = ExtraHeaders::new()
        .with(format!("X-Api-Key: {SECRET}"), "=")
        .expect_err("not a name");
    let shown = format!("{refusal} {refusal:?}");
    assert!(!shown.contains(SECRET), "{shown}");
    let taken = ExtraHeaders::new()
        .with("X-Tenant_1", "acme")
        .expect("letters, digits, `-` and `_`");
    assert_eq!(taken.iter().collect::<Vec<_>>(), [("X-Tenant_1", "acme")]);
}

#[test]
fn a_header_the_client_writes_itself_is_refused() {
    for name in [
        "authorization",
        "Authorization",
        "etp-encoding",
        "MaxWebSocketMessagePayloadSize",
        "maxwebsocketframepayloadsize",
        "host",
        "connection",
        "upgrade",
        "content-type",
        "content-length",
        "transfer-encoding",
        "accept",
        "sec-websocket-protocol",
        "Sec-WebSocket-Key",
    ] {
        let refusal = ExtraHeaders::new().with(name, "x").expect_err(name);
        assert_eq!(refusal, HeaderRefusal::Own(name.to_string()), "{name}");
        // The same door for a value held as a secret.
        let refusal = ExtraHeaders::new().with_secret(name, "x").expect_err(name);
        assert_eq!(refusal, HeaderRefusal::Own(name.to_string()), "{name}");
    }
    // The credentials have their own way in, and the refusal says which.
    let said = ExtraHeaders::new()
        .with("Authorization", SECRET)
        .expect_err("refused")
        .to_string();
    assert!(said.contains("credentials"), "{said}");
    assert!(!said.contains(SECRET), "{said}");
}

#[test]
fn the_same_header_twice_is_refused_whatever_the_case() {
    let headers = ExtraHeaders::new().with("x-tenant", "acme").expect("taken");
    assert_eq!(
        headers.clone().with("X-Tenant", "other").expect_err("twice"),
        HeaderRefusal::Twice("X-Tenant".to_string())
    );
    let others = ExtraHeaders::new().with("X-TENANT", "other").expect("taken");
    assert_eq!(
        headers.clone().and(&others).expect_err("twice"),
        HeaderRefusal::Twice("X-TENANT".to_string())
    );
    assert_eq!(
        ExtraHeaders::from_pairs([("a", "1"), ("b", "2"), ("A", "3")]).expect_err("twice"),
        HeaderRefusal::Twice("A".to_string())
    );
}

#[test]
fn a_value_with_a_control_character_is_refused_and_never_shown() {
    // RFC 9110 §5.5: a line break in a value would start another header.
    for value in [
        format!("{SECRET}\r\nx-injected: 1"),
        format!("{SECRET}\n"),
        format!("{SECRET}\0"),
        format!("{SECRET}\u{7f}"),
        // Not ASCII: a WebSocket handshake would refuse it after connecting, and say the value.
        format!("{SECRET}\u{a0}"),
        format!("{SECRET}\u{e9}"),
    ] {
        let refusal = ExtraHeaders::new()
            .with("x-tenant", value.as_str())
            .expect_err("refused");
        assert_eq!(refusal, HeaderRefusal::Value("x-tenant".to_string()));
        let shown = format!("{refusal} {refusal:?}");
        assert!(!shown.contains(SECRET), "{shown}");
    }
    // A tab is the one control character a value may hold.
    assert!(ExtraHeaders::new().with("x-tenant", "a\tb").is_ok());
}

// ---- a value is never shown --------------------------------------------------

#[test]
fn debug_shows_the_names_and_never_a_value() {
    let headers = ExtraHeaders::new()
        .with("x-tenant", "acme-tenant")
        .and_then(|headers| headers.with_secret("x-routing", SECRET))
        .expect("taken");
    let shown = format!("{headers:?} {headers:#?}");
    assert!(
        shown.contains("x-tenant") && shown.contains("x-routing"),
        "{shown}"
    );
    assert!(
        !shown.contains(SECRET) && !shown.contains("acme-tenant"),
        "{shown}"
    );

    // Neither does the endpoint that carries them, nor the handshake they go
    // in - the credentials no more than the header.
    let endpoint = Endpoint {
        headers,
        credentials: Some(Credentials::bearer(&format!("token-{SECRET}")).expect("a token")),
        ..Endpoint::new("wss://etp.example/etp")
    };
    let settings = HandshakeSettings {
        authorization: endpoint.credentials.clone(),
        ..HandshakeSettings::default()
    };
    let handshake = probe::handshake(&endpoint.url, Profile::Native, &settings).expect("a URL");
    let handshake = endpoint.adapt(handshake).expect("sendable");
    assert!(
        handshake
            .headers
            .contains(&("x-routing".to_string(), SECRET.to_string())),
        "the header is in the handshake"
    );
    let shown = format!("{endpoint:?} {endpoint:#?} {handshake:?} {handshake:#?}");
    assert!(!shown.contains(SECRET), "{shown}");
    assert!(
        shown.contains("etp-encoding: binary")
            && shown.contains("x-routing")
            && shown.contains("authorization"),
        "the names, and ETP's own attributes, are still said: {shown}"
    );
}

// ---- where a handshake puts them ---------------------------------------------

#[test]
fn a_program_sends_headers_and_a_front_end_writes_the_query_string() {
    let settings = HandshakeSettings::default();
    let headers = ExtraHeaders::from_pairs([("X-Tenant", "acme corp"), ("x-zone", "eu")]).expect("taken");

    // §4.3 step 4.c: header properties, their names in lower case.
    let native = probe::handshake("wss://etp.example/etp", Profile::Native, &settings).expect("a URL");
    let adapted = headers.onto(native.clone()).expect("sendable");
    let added: Vec<_> = adapted
        .headers
        .iter()
        .filter(|header| !native.headers.contains(header))
        .collect();
    assert_eq!(
        added,
        [
            &("x-tenant".to_string(), "acme corp".to_string()),
            &("x-zone".to_string(), "eu".to_string())
        ]
    );
    assert_eq!(adapted.url, native.url, "nothing in the URL");

    // §4.3.2: a browser sets no header, the properties go in the query
    // string, URL-encoded.
    let headers = ExtraHeaders::from_pairs([("x-tenant", "acme corp"), ("x-zone", "eu")]).expect("taken");
    let browser = probe::handshake("wss://etp.example/etp", Profile::Browser, &settings).expect("a URL");
    let adapted = headers.onto(browser.clone()).expect("sendable");
    assert!(adapted.headers.is_empty(), "no header at all");
    assert_eq!(
        adapted.url,
        format!("{}&x-tenant=acme%20corp&x-zone=eu", browser.url)
    );
}

#[test]
fn a_secret_is_never_written_in_a_url() {
    let settings = HandshakeSettings::default();
    let browser = probe::handshake("wss://etp.example/etp", Profile::Browser, &settings).expect("a URL");
    let native = probe::handshake("wss://etp.example/etp", Profile::Native, &settings).expect("a URL");

    // A secret by its name, and a secret because whoever added it said so.
    assert!(sensitive_header("X-Api-Key") && !sensitive_header("x-routing"));
    let by_name = ExtraHeaders::new().with("X-Api-Key", SECRET).expect("taken");
    let by_flag = ExtraHeaders::new()
        .with_secret("x-routing", SECRET)
        .expect("taken");
    for (headers, name) in [(&by_name, "X-Api-Key"), (&by_flag, "x-routing")] {
        let refusal = headers.onto(browser.clone()).expect_err("refused");
        assert_eq!(refusal, HeaderRefusal::InTheUrl(name.to_string()));
        assert!(!format!("{refusal} {refusal:?}").contains(SECRET));
        // A program has a header to put it in.
        let adapted = headers.onto(native.clone()).expect("sendable");
        assert!(adapted.headers.iter().any(|(_, value)| value == SECRET));
        assert!(!adapted.url.contains(SECRET));
    }
    // One secret refuses the whole list: nothing of it reaches the URL.
    let mixed = ExtraHeaders::new()
        .with("x-tenant", "acme")
        .and_then(|headers| headers.with_secret("x-routing", SECRET))
        .expect("taken");
    assert_eq!(
        mixed.onto(browser).expect_err("refused"),
        HeaderRefusal::InTheUrl("x-routing".to_string())
    );

    // The endpoint says it with nothing built, for whoever logs in first:
    // the same refusal as the connection's, and none for a program.
    let endpoint = Endpoint {
        headers: mixed,
        ..Endpoint::new("wss://etp.example/etp")
    };
    assert_eq!(endpoint.sendable(), Ok(()));
    let front_end = Endpoint {
        handshake: Profile::Browser,
        ..endpoint
    };
    assert_eq!(
        front_end.sendable(),
        Err(HeaderRefusal::InTheUrl("x-routing".to_string()))
    );
    // Without a secret, a front end's handshake carries them.
    let plain = Endpoint {
        headers: ExtraHeaders::new().with("x-tenant", "acme").expect("taken"),
        handshake: Profile::Browser,
        ..Endpoint::new("wss://etp.example/etp")
    };
    assert_eq!(plain.sendable(), Ok(()));
}

// ---- the partition is a header like any other --------------------------------

/// OSDU's Reservoir DDMS at `url`: its data partition is a header of the server.
fn rddms(url: impl Into<String>) -> Endpoint {
    Endpoint {
        headers: ExtraHeaders::new()
            .with("data-partition-id", "opendes")
            .expect("taken"),
        ..Endpoint::new(url)
    }
}

#[test]
fn the_partition_is_one_header_added_to_the_handshake_and_nothing_else() {
    let settings = HandshakeSettings::default();
    let base = probe::handshake("wss://rddms.example/api/etp", Profile::Native, &settings).expect("a URL");
    let adapted = rddms("wss://rddms.example/api/etp")
        .adapt(base.clone())
        .expect("sendable");
    let added: Vec<_> = adapted
        .headers
        .iter()
        .filter(|h| !base.headers.contains(h))
        .collect();
    assert_eq!(added, [&("data-partition-id".to_string(), "opendes".to_string())]);
    assert_eq!(
        (adapted.url.as_str(), &adapted.subprotocols),
        (base.url.as_str(), &base.subprotocols)
    );
    assert_eq!(
        Endpoint::new("wss://etp.example/etp")
            .adapt(base.clone())
            .expect("sendable"),
        base,
        "a server that wants nothing: nothing added"
    );
}

// ---- on the wire ---------------------------------------------------------------

/// Listen on a free port; every request received is kept, and answered `401`.
/// The port, and the requests as they arrive (the request line and the
/// headers, as text).
fn recording() -> (u16, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free local port");
    let port = listener.local_addr().expect("a bound address").port();
    let (seen, requests) = mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let mut reader = BufReader::new(stream.try_clone().expect("a second handle"));
            let mut text = String::new();
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) if line == "\r\n" => break,
                    Ok(_) => text.push_str(&line),
                }
            }
            if seen.send(text).is_err() {
                break;
            }
            let _ = stream
                .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            let _ = stream.flush();
        }
    });
    (port, requests)
}

/// The value of header `name` in `request`, whatever the case of its name.
fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim().eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

/// The first line of `request`: the method, the path and its query string.
fn request_line(request: &str) -> &str {
    request.lines().next().unwrap_or_default()
}

/// A Reservoir DDMS at `url` that wants a tenant and an API key as well.
fn demanding(url: String) -> Endpoint {
    Endpoint {
        headers: ExtraHeaders::new()
            .with("data-partition-id", "opendes")
            .and_then(|headers| headers.with("x-tenant", "acme"))
            .and_then(|headers| headers.with("x-api-key", SECRET))
            .expect("taken"),
        ..Endpoint::new(url)
    }
}

fn connect_to(endpoint: Endpoint) -> Connect {
    let (offer, handlers) = reading_client(
        "extra headers test client",
        "0.1",
        Uuid([1; 16]),
        &["resqml20.*".to_string()],
    );
    Connect {
        endpoint,
        offer,
        handlers,
        max_message: None,
    }
}

/// The error of a session the server did not open.
fn refused(endpoint: Endpoint) -> ClientError {
    match connect(connect_to(endpoint)) {
        Ok(_) => panic!("the server answers 401: no session can open"),
        Err(why) => why,
    }
}

fn assert_the_three_are_sent(request: &str) {
    assert_eq!(header(request, "data-partition-id"), Some("opendes"), "{request}");
    assert_eq!(header(request, "x-tenant"), Some("acme"), "{request}");
    assert_eq!(header(request, "x-api-key"), Some(SECRET), "{request}");
    // None of them is in the URL. (A `?` alone proves nothing: the probe has
    // a query of its own, `GetVersion`.)
    let line = request_line(request).to_ascii_lowercase();
    let secret = SECRET.to_ascii_lowercase();
    for word in [
        "data-partition-id",
        "opendes",
        "x-tenant",
        "acme",
        "x-api-key",
        secret.as_str(),
    ] {
        assert!(!line.contains(word), "`{word}` is in the URL: {line}");
    }
}

#[test]
fn the_websocket_handshake_carries_them() {
    let (port, requests) = recording();
    let error = refused(demanding(format!("ws://127.0.0.1:{port}/etp")));
    let request = requests.recv_timeout(WAIT).expect("the upgrade request");
    assert!(request_line(&request).starts_with("GET /etp "), "{request}");
    assert_eq!(header(&request, "etp-encoding"), Some("binary"), "{request}");
    assert_the_three_are_sent(&request);
    // The refusal of the server says nothing of what was sent.
    assert!(!format!("{error} {error:?}").contains(SECRET), "{error:?}");
}

#[test]
fn the_probe_carries_them_as_headers_whatever_the_handshake() {
    for handshake in [Profile::Native, Profile::Browser] {
        let (port, requests) = recording();
        let endpoint = Endpoint {
            handshake,
            ..demanding(format!("ws://127.0.0.1:{port}/etp"))
        };
        let error = fetch_server_capabilities(&endpoint).expect_err("the server answers 401");
        let request = requests.recv_timeout(WAIT).expect("the probe");
        assert!(
            request_line(&request).contains("/.well-known/etp-server-capabilities"),
            "{request}"
        );
        // HTTP has headers, a front end's `fetch` too: nothing goes in the URL here.
        assert_the_three_are_sent(&request);
        assert!(matches!(error, ProbeError::Http(_)), "{error:?}");
        assert!(!format!("{error} {error:?}").contains(SECRET), "{error:?}");
    }
}

#[test]
fn a_rest_call_carries_them() {
    let (port, requests) = recording();
    let endpoint = demanding(format!("http://127.0.0.1:{port}/api"));
    let client = native::rest_client(&endpoint);
    let answer = Remote::Rest(Box::new(client)).health().expect("a REST client");
    let request = requests.recv_timeout(WAIT).expect("the REST request");
    assert!(request_line(&request).starts_with("GET /api/"), "{request}");
    assert_the_three_are_sent(&request);
    let error = answer.expect_err("the server answers 401");
    assert!(!format!("{error} {error:?}").contains(SECRET), "{error:?}");
}

#[cfg(feature = "live")]
#[test]
fn a_live_session_carries_them() {
    use retp_client::live::{LiveSession, LiveSettings};

    let (port, requests) = recording();
    let endpoint = demanding(format!("ws://127.0.0.1:{port}/etp"));
    let error = match LiveSession::connect(connect_to(endpoint), LiveSettings::default()) {
        Ok(_) => panic!("the server answers 401: no session can open"),
        Err(why) => why,
    };
    let request = requests.recv_timeout(WAIT).expect("the upgrade request");
    assert_the_three_are_sent(&request);
    assert!(!format!("{error} {error:?}").contains(SECRET), "{error:?}");
}

#[test]
fn a_front_end_handshake_writes_them_in_the_query_string() {
    let (port, requests) = recording();
    let endpoint = Endpoint {
        handshake: Profile::Browser,
        headers: ExtraHeaders::new()
            .with("data-partition-id", "opendes")
            .and_then(|headers| headers.with("x-tenant", "acme"))
            .expect("taken"),
        ..Endpoint::new(format!("ws://127.0.0.1:{port}/etp"))
    };
    let _ = refused(endpoint);
    let request = requests.recv_timeout(WAIT).expect("the upgrade request");
    let line = request_line(&request);
    assert!(line.starts_with("GET /etp?"), "{line}");
    assert!(
        line.contains("etp-encoding=binary")
            && line.contains("data-partition-id=opendes")
            && line.contains("x-tenant=acme"),
        "{line}"
    );
    for name in ["etp-encoding", "data-partition-id", "x-tenant"] {
        assert_eq!(
            header(&request, name),
            None,
            "a front end sets no header: {request}"
        );
    }
}

/// What a connection that must be refused before any socket said: its
/// `Display`, then its `Debug`.
///
/// The listener accepts nothing. A connection that was made all the same
/// would wait for an answer for ever: it is seen in the listener's queue
/// instead, and the test fails rather than hangs.
fn refused_before_the_socket(
    listener: &TcpListener,
    attempt: impl FnOnce() -> Option<ClientError> + Send + 'static,
) -> (String, String) {
    let (done, answer) = mpsc::channel();
    std::thread::spawn(move || {
        let said = attempt().map(|error| (error.to_string(), format!("{error:?}")));
        let _ = done.send(said);
    });
    let deadline = std::time::Instant::now() + WAIT;
    let said = loop {
        match listener.accept() {
            Ok(_) => panic!("a connection was made: the refusal comes before the socket"),
            Err(why) if why.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(why) => panic!("the listener failed: {why}"),
        }
        match answer.recv_timeout(Duration::from_millis(20)) {
            Ok(Some(said)) => break said,
            Ok(None) => panic!("a session opened"),
            Err(mpsc::RecvTimeoutError::Timeout) if std::time::Instant::now() < deadline => {}
            Err(why) => panic!("no answer: {why}"),
        }
    };
    let nobody = listener.accept().expect_err("nothing was connected");
    assert_eq!(nobody.kind(), std::io::ErrorKind::WouldBlock, "{nobody}");
    said
}

#[test]
fn a_secret_for_a_front_end_handshake_is_refused_before_anything_is_connected() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free local port");
    listener
        .set_nonblocking(true)
        .expect("a listener that does not wait");
    let port = listener.local_addr().expect("a bound address").port();
    let endpoint = Endpoint {
        handshake: Profile::Browser,
        ..demanding(format!("ws://127.0.0.1:{port}/etp"))
    };

    let asked = endpoint.clone();
    let (said, shown) = refused_before_the_socket(&listener, move || connect(connect_to(asked)).err());
    assert!(said.contains("x-api-key") && said.contains("URL"), "{said}");
    assert!(!format!("{said} {shown}").contains(SECRET), "{shown}");

    #[cfg(feature = "live")]
    {
        use retp_client::live::{LiveSession, LiveSettings};
        let (said, shown) = refused_before_the_socket(&listener, move || {
            LiveSession::connect(connect_to(endpoint), LiveSettings::default()).err()
        });
        assert!(said.contains("x-api-key"), "{said}");
        assert!(!format!("{said} {shown}").contains(SECRET), "{shown}");
    }
}
