//! `server test`: the verdict of each step in the order one fails, with a
//! network that is a fake and a token endpoint that is a local socket.

use std::cell::Cell;

use retp_catalog::{AuthSpec, Entry, Network, Outcome, Reserved, SecretRef, ServerSpec, render};
use retp_client::native::Endpoint;

use crate::common::{CANARY, FakeToken, MemorySecrets, Reply, Sandbox, files_containing};

struct FakeNetwork {
    well_known: Result<String, String>,
    session: Result<String, String>,
    session_rest: Cell<Option<bool>>,
    session_had_credentials: Cell<Option<bool>>,
}

impl FakeNetwork {
    fn working() -> Self {
        FakeNetwork {
            well_known: Ok("Fake server 1.2".into()),
            session: Ok("session opened".into()),
            session_rest: Cell::new(None),
            session_had_credentials: Cell::new(None),
        }
    }
}

impl Network for FakeNetwork {
    fn well_known(&self, _endpoint: &Endpoint) -> Result<String, String> {
        self.well_known.clone()
    }

    fn session(&self, endpoint: Endpoint, rest: bool) -> Result<String, String> {
        self.session_rest.set(Some(rest));
        self.session_had_credentials
            .set(Some(endpoint.credentials.is_some()));
        self.session.clone()
    }
}

fn bearer() -> Entry {
    Entry::new(
        ServerSpec::etp("wss://h.example.com/etp"),
        AuthSpec::Bearer {
            token: SecretRef::Env("T".into()),
        },
    )
}

fn names(report: &retp_catalog::TestReport) -> Vec<&'static str> {
    report.steps.iter().map(|s| s.name).collect()
}

#[test]
fn every_step_passes_and_the_record_is_written() {
    let sandbox = Sandbox::new("test-ok");
    let mut catalog = sandbox.catalog();
    catalog
        .add("srv", bearer(), false, &Reserved::default())
        .expect("added");
    let network = FakeNetwork::working();
    let report = catalog.run_test("srv", &MemorySecrets::of(&[("env:T", "tok")]), &network);

    assert!(report.passed(), "{}", render::test(&report));
    assert_eq!(
        names(&report),
        ["definition", "references", "well-known", "token", "session"]
    );
    assert!(
        report
            .steps
            .iter()
            .all(|s| matches!(s.outcome, Outcome::Passed(_)))
    );
    assert_eq!(
        network.session_had_credentials.get(),
        Some(true),
        "the session got the credentials"
    );
    assert_eq!(network.session_rest.get(), Some(false));

    catalog.record_test(&report).expect("recorded");
    let text = std::fs::read_to_string(sandbox.servers().join("srv").join("state").join("last.json"))
        .expect("last.json");
    let json: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    assert_eq!(json["passed"], true);
    assert_eq!(json["steps"].as_array().map(Vec::len), Some(5));
}

#[test]
fn a_reference_that_does_not_resolve_stops_the_test_before_any_network() {
    let sandbox = Sandbox::new("test-missing");
    let mut catalog = sandbox.catalog();
    catalog
        .add("srv", bearer(), false, &Reserved::default())
        .expect("added");
    let network = FakeNetwork::working();
    let report = catalog.run_test("srv", &MemorySecrets::of(&[]), &network);

    assert!(!report.passed());
    assert!(matches!(&report.steps[1].outcome, Outcome::Failed(why) if why.contains("env:T")));
    assert!(
        report.steps[2..]
            .iter()
            .all(|s| matches!(s.outcome, Outcome::Skipped(_)))
    );
    assert_eq!(network.session_rest.get(), None, "the session was not tried");
}

#[test]
fn an_unknown_server_fails_at_the_definition() {
    let sandbox = Sandbox::new("test-unknown");
    let catalog = sandbox.catalog();
    let report = catalog.run_test("nope", &MemorySecrets::of(&[]), &FakeNetwork::working());
    assert!(matches!(&report.steps[0].outcome, Outcome::Failed(why) if why.contains("nope")));
    assert_eq!(report.steps.len(), 5);
    assert!(
        report.steps[1..]
            .iter()
            .all(|s| matches!(s.outcome, Outcome::Skipped(_)))
    );
}

#[test]
fn an_unreachable_capabilities_document_stops_before_the_login() {
    let sandbox = Sandbox::new("test-well-known");
    let mut catalog = sandbox.catalog();
    catalog
        .add("srv", bearer(), false, &Reserved::default())
        .expect("added");
    let mut network = FakeNetwork::working();
    network.well_known = Err("connection refused".into());
    let report = catalog.run_test("srv", &MemorySecrets::of(&[("env:T", "tok")]), &network);
    assert!(matches!(&report.steps[2].outcome, Outcome::Failed(why) if why == "connection refused"));
    assert!(matches!(report.steps[3].outcome, Outcome::Skipped(_)));
    assert!(matches!(report.steps[4].outcome, Outcome::Skipped(_)));
}

#[test]
fn over_rest_there_is_no_capabilities_document() {
    let sandbox = Sandbox::new("test-rest");
    let mut catalog = sandbox.catalog();
    let entry = Entry::new(
        ServerSpec::rest("https://h.example.com/rest"),
        AuthSpec::Bearer {
            token: SecretRef::Env("T".into()),
        },
    );
    catalog
        .add("srv", entry, false, &Reserved::default())
        .expect("added");
    let network = FakeNetwork::working();
    let report = catalog.run_test("srv", &MemorySecrets::of(&[("env:T", "tok")]), &network);
    assert!(matches!(report.steps[2].outcome, Outcome::Skipped(_)));
    assert!(report.passed());
    assert_eq!(network.session_rest.get(), Some(true));
}

#[test]
fn a_refused_login_is_reported_without_the_secret_even_when_the_server_repeats_it() {
    let sandbox = Sandbox::new("test-refused");
    let mut catalog = sandbox.catalog();
    // The server echoes the request body, which holds the client secret on the second try
    // (`client_secret_post`, after `invalid_client`): the worst case.
    let token = FakeToken::start(Reply::EchoBody, 2);
    let entry = Entry::new(
        ServerSpec::etp("wss://h.example.com/etp"),
        AuthSpec::OauthClient {
            token_url: token.url.clone(),
            client_id: "cid".into(),
            client_secret: SecretRef::Env("S".into()),
            scope: None,
        },
    );
    catalog
        .add("srv", entry, false, &Reserved::default())
        .expect("added");
    let network = FakeNetwork::working();
    let report = catalog.run_test("srv", &MemorySecrets::of(&[("env:S", CANARY)]), &network);

    let token_step = &report.steps[3];
    match &token_step.outcome {
        Outcome::Failed(why) => {
            assert!(why.contains("invalid_client"), "{why}");
            assert!(why.contains("[redacted]"), "the echo was not redacted: {why}");
            assert!(!why.contains(CANARY), "{why}");
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(report.steps[4].outcome, Outcome::Skipped(_)));
    assert!(!render::test(&report).contains(CANARY));

    catalog.record_test(&report).expect("recorded");
    assert_eq!(
        files_containing(&sandbox.root, CANARY),
        Vec::<std::path::PathBuf>::new()
    );
}
