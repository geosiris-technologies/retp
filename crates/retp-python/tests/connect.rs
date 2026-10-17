//! What `retp.connect` refuses before any socket is opened, and what it says.

use retp_python::session::{ConnectOptions, Kind, Session, parse_scope, refuse_unless_writing};

fn options(url: &str) -> ConnectOptions {
    ConnectOptions {
        url: url.to_string(),
        ..ConnectOptions::default()
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
fn an_http_url_without_rest_is_refused_and_names_rest() {
    let refusal = options("https://etp.example.org/api/reservoir-ddms/v2")
        .endpoint()
        .expect_err("an HTTP URL is a REST API's");
    assert_eq!(refusal.kind, Kind::BadArgument);
    assert!(refusal.message.contains("rest=True"), "{}", refusal.message);
}

#[test]
fn a_rest_url_with_rest_is_accepted() {
    let mut given = options("https://etp.example.org/api/reservoir-ddms/v2");
    given.rest = true;
    given.partition = Some("opendes".to_string());
    let (endpoint, warnings) = given.endpoint().expect("accepted");
    assert_eq!(endpoint.url, "https://etp.example.org/api/reservoir-ddms/v2");
    assert!(warnings.is_empty(), "{warnings:?}");
}

#[test]
fn rest_refuses_a_websocket_url_and_what_only_a_session_has() {
    let mut websocket = options("wss://etp.example.org/");
    websocket.rest = true;
    let refusal = websocket.endpoint().expect_err("a WebSocket's URL");
    assert_eq!(refusal.kind, Kind::BadArgument);
    assert!(refusal.message.contains("REST API's URL"), "{}", refusal.message);

    let mut keepalive = options("https://etp.example.org/api/reservoir-ddms/v2");
    keepalive.rest = true;
    keepalive.keepalive_seconds = Some(30);
    let refusal = keepalive.endpoint().expect_err("no session to keep alive");
    assert!(refusal.message.contains("keepalive"), "{}", refusal.message);

    let mut types = options("https://etp.example.org/api/reservoir-ddms/v2");
    types.rest = true;
    types.types = vec!["resqml20.*".to_string()];
    let refusal = types.endpoint().expect_err("no session to negotiate");
    assert!(refusal.message.contains("types"), "{}", refusal.message);
}

#[test]
fn a_rest_session_opens_without_a_network_and_refuses_what_rest_does_not_do() {
    // The REST client opens nothing: each request goes on its own.
    let mut given = options("https://etp.example.org/api/reservoir-ddms/v2");
    given.rest = true;
    let mut session = Session::open(given).expect("nothing to open");
    assert!(session.live().is_none(), "a REST session is not a live one");
    let refusal = session
        .delete_dataspaces(&["demo".to_string()])
        .expect_err("read-only");
    assert_eq!(refusal.kind, Kind::Unsupported);
    assert!(refusal.message.contains("writing=True"), "{}", refusal.message);
    session.close().expect("closes");
}

#[test]
fn a_user_and_a_password_are_http_basic() {
    let mut given = options("wss://etp.example.org/");
    given.login.user = Some("someone".to_string());
    given.login.password = Some("pa55word".to_string());
    let (endpoint, _) = given.endpoint().expect("accepted");
    let credentials = endpoint.credentials.expect("credentials");
    assert_eq!(credentials.scheme(), "Basic");
    assert!(!format!("{credentials:?}").contains("pa55word"));
}

#[test]
fn a_token_comes_before_any_other_login() {
    let mut given = options("wss://etp.example.org/");
    given.token = Some("abc.DEF".to_string());
    given.login.user = Some("someone".to_string());
    let (endpoint, _) = given.endpoint().expect("accepted");
    assert_eq!(endpoint.credentials.expect("credentials").scheme(), "Bearer");
}

#[test]
fn a_login_that_lacks_a_part_is_refused_before_any_request() {
    let mut no_password = options("wss://etp.example.org/");
    no_password.login.user = Some("someone".to_string());
    let refusal = no_password.endpoint().expect_err("no password");
    assert_eq!(refusal.kind, Kind::BadArgument);
    assert!(refusal.message.contains("password"), "{}", refusal.message);

    // The region is checked before Cognito is asked anything.
    let mut cognito = options("wss://etp.example.org/");
    cognito.login.cognito_region = Some("Not A Region".to_string());
    cognito.login.client_id = Some("client".to_string());
    cognito.login.user = Some("someone".to_string());
    cognito.login.password = Some("pa55word".to_string());
    let refusal = cognito.endpoint().expect_err("not a region");
    assert_eq!(refusal.kind, Kind::BadArgument);
    assert!(refusal.message.contains("AWS region"), "{}", refusal.message);
    assert!(!refusal.message.contains("pa55word"), "{}", refusal.message);
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
    assert!(refuse_unless_writing(true, "put objects").is_ok());
    let refusal = refuse_unless_writing(false, "put objects").expect_err("read-only");
    assert_eq!(refusal.kind, Kind::Unsupported);
    assert!(refusal.message.contains("writing=True"), "{}", refusal.message);
}
