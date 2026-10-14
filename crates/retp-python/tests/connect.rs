//! What `retp.connect` refuses before any socket is opened, and what it says.

use retp_python::session::{ConnectOptions, Kind, parse_scope, refuse_unless_writing};

fn options(url: &str) -> ConnectOptions {
    ConnectOptions {
        url: url.to_string(),
        token: None,
        partition: None,
        insecure: false,
        writing: false,
        types: Vec::new(),
        keepalive_seconds: None,
        at_once: None,
    }
}

#[test]
fn a_websocket_url_is_accepted() {
    let (endpoint, warnings) = options("wss://etp.example.org/").endpoint().expect("accepted");
    assert_eq!(endpoint.url, "wss://etp.example.org/");
    assert!(endpoint.credentials.is_none());
    assert!(warnings.is_empty(), "{warnings:?}");
}

#[test]
fn a_rest_url_is_refused_by_name() {
    let refusal = options("https://etp.example.org/api/reservoir-ddms/v2")
        .endpoint()
        .expect_err("REST is not bridged yet");
    assert_eq!(refusal.kind, Kind::Unsupported);
    assert!(refusal.message.contains("REST"), "{}", refusal.message);
}

#[test]
fn clear_text_to_another_host_is_said() {
    let (_, warnings) = options("ws://etp.example.org/")
        .endpoint()
        .expect("allowed, and said");
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("not encrypted"), "{}", warnings[0]);
}

#[test]
fn an_unchecked_certificate_is_said() {
    let mut given = options("wss://etp.example.org/");
    given.insecure = true;
    let (_, warnings) = given.endpoint().expect("allowed, and said");
    assert!(warnings.iter().any(|w| w.contains("certificate")), "{warnings:?}");
}

#[test]
fn a_token_that_is_not_a_bearer_token_is_refused_without_echoing_it() {
    let mut given = options("wss://etp.example.org/");
    given.token = Some("Bearer s3cret value".to_string());
    let refusal = given.endpoint().expect_err("a space is not b64token");
    assert_eq!(refusal.kind, Kind::BadArgument);
    assert!(
        !refusal.message.contains("s3cret"),
        "the refusal must name the rule, never the token: {}",
        refusal.message
    );
}

#[test]
fn a_good_token_becomes_credentials_that_hide_it() {
    let mut given = options("wss://etp.example.org/");
    given.token = Some("abc.DEF-123_456".to_string());
    let (endpoint, _) = given.endpoint().expect("accepted");
    let credentials = endpoint.credentials.expect("credentials");
    assert_eq!(credentials.scheme(), "Bearer");
    assert!(!format!("{credentials:?}").contains("abc.DEF"));
}

#[test]
fn zero_requests_in_flight_is_refused() {
    let mut given = options("wss://etp.example.org/");
    given.at_once = Some(0);
    assert_eq!(given.endpoint().expect_err("zero").kind, Kind::BadArgument);
}

#[test]
fn the_five_scopes_parse_and_the_rest_is_refused() {
    for name in ["self", "sources", "targets", "sourcesOrSelf", "targetsOrSelf"] {
        assert!(parse_scope(name).is_ok(), "{name}");
    }
    let refusal = parse_scope("everything").expect_err("not a scope");
    assert_eq!(refusal.kind, Kind::BadArgument);
    assert!(refusal.message.contains("everything"));
}

#[test]
fn a_read_only_session_refuses_to_write_by_name() {
    assert!(refuse_unless_writing(true).is_ok());
    let refusal = refuse_unless_writing(false).expect_err("read-only");
    assert_eq!(refusal.kind, Kind::Unsupported);
    assert!(refusal.message.contains("writing=True"), "{}", refusal.message);
}
