//! Tests against a live ETP server. **Opt-in**: compiled only with the
//! `live-server` feature, and each test is `#[ignore]` so that a plain
//! `cargo test --features live-server` still does not touch the network.
//!
//!     RETP_TEST_URL=wss://... RETP_TEST_TOKEN=... \
//!         cargo test --workspace --features live-server -- --ignored live
//!
//! The token comes from the environment, never from a file in the repository.
//! These tests only read. TODO(verify): never run, against any server.

use retp_python::session::{ConnectOptions, Session};

fn open() -> Session {
    let url = std::env::var("RETP_TEST_URL").expect("set RETP_TEST_URL to a ws:// or wss:// URL");
    Session::open(ConnectOptions {
        url,
        token: std::env::var("RETP_TEST_TOKEN").ok(),
        partition: std::env::var("RETP_TEST_PARTITION").ok(),
        insecure: false,
        writing: false,
        types: Vec::new(),
        keepalive_seconds: None,
        at_once: None,
    })
    .expect("a session")
}

#[test]
#[ignore = "needs a live ETP server"]
fn live_dataspaces_can_be_listed() {
    let mut session = open();
    // A server may hold none, and that is not what is checked: the call
    // answers, or fails by name.
    let _ = session.dataspaces().expect("the dataspaces");
    session.close().expect("closes");
}

#[test]
#[ignore = "needs a live ETP server"]
fn live_a_bad_scope_is_refused_before_it_is_sent() {
    let mut session = open();
    let refusal = session
        .resources("", 1, "nowhere", Vec::new(), false, false)
        .expect_err("refused");
    assert_eq!(refusal.kind, retp_python::session::Kind::BadArgument);
}

#[test]
#[ignore = "needs a live ETP server"]
fn live_a_read_only_session_does_not_write() {
    let mut session = open();
    let refusal = session
        .put_objects("demo", &["{}".to_string()])
        .expect_err("read-only");
    assert_eq!(refusal.kind, retp_python::session::Kind::Unsupported);
}
