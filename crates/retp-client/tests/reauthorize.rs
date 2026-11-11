//! Renewing the authorization of an open session (ETP 1.2 §3.13.5, p.90):
//! the door the clients offer, against a server built here from
//! `retp-proto`'s own `Connection`.
//!
//! The server asks for authorization, grants a token until a moment it
//! chooses, and - like a real one - closes the session when that moment
//! comes without a renewal: `EAUTHORIZATION_EXPIRED` then `CloseSession`
//! (§3.13.5 point 3). The rules come from the specification, not from any
//! server's behaviour:
//!
//! - §3.13.5 point 1.a, b (p.90): a client re-authorizes BEFORE the
//!   authorization expires, with `Authorize` sent at any time;
//! - §5.3.4 (p.121) and §5.3.7 (p.123): `Authorize`, and its
//!   `AuthorizeResponse` with `success` and, on failure, the challenges.
//!
//! The clock is a number the tests move; nothing here sleeps.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use retp_avro::Role;
use retp_client::{
    AsyncClient, Client, ClientError, Clock, CredentialSource, Fresh, GaveUp, Refused, Settings, Sink,
    SourceError, Transport, TransportError, Witness,
};
use retp_proto::connection::{Authorization, Authorizer, Connection, Event, Forward, Handler};
use retp_proto::credentials::Credentials;
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_types::datatypes::{SupportedDataObject, Uuid};
use retp_types::protocol::discovery::GetResources;

const S: i64 = 1_000_000;
const T0: i64 = 1_760_000_000 * S;

/// The moment the first token runs out, in seconds after `T0`.
const FIRST_EXPIRY: i64 = 120;

#[derive(Clone)]
struct ManualClock(Arc<AtomicI64>);

impl ManualClock {
    fn new(now: i64) -> Self {
        Self(Arc::new(AtomicI64::new(now)))
    }
    fn set(&self, now: i64) {
        self.0.store(now, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn bearer(token: &str) -> Credentials {
    Credentials::bearer(token).unwrap()
}

fn offers() -> (Offer, Vec<Box<dyn Handler>>, Offer) {
    let objects = |types: &[&str]| -> Vec<SupportedDataObject> {
        types
            .iter()
            .map(|t| SupportedDataObject {
                qualified_type: t.to_string(),
                data_object_capabilities: BTreeMap::new(),
            })
            .collect()
    };
    let mut client = Offer::new("reauthorize tests", "0.1", Uuid([1; 16]));
    client.data_objects = objects(&["resqml20.*"]);
    let mut server = Offer::new("a test store", "0.1", Uuid([2; 16]));
    server.data_objects = objects(&["resqml20.*"]);
    let handlers: Vec<Box<dyn Handler>> = vec![Box::new(Forward(ProtocolOffer::v12(3, Role::Customer)))];
    (client, handlers, server)
}

/// A server that grants `good` until `T0 + FIRST_EXPIRY`, `renewed` for much
/// longer, and refuses anything else with a challenge.
fn server() -> Connection {
    let (_, _, offer) = offers();
    let authorizer: Box<dyn Authorizer> = Box::new(|authorization: &str, _now: i64| match authorization {
        "Bearer good" => Authorization::Granted {
            expires_at: Some(T0 + FIRST_EXPIRY * S),
        },
        "Bearer renewed" => Authorization::Granted {
            expires_at: Some(T0 + 1000 * S),
        },
        _ => Authorization::Challenge(vec![
            r#"Bearer authz_server="https://auth.example", scope="etp""#.into(),
        ]),
    });
    Connection::server(
        offer,
        vec![Box::new(Forward(ProtocolOffer::v12(3, Role::Store)))],
        Some(authorizer),
        Uuid([0xAB; 16]),
        T0,
    )
    .unwrap()
    .warn_expiring_before(60)
}

// ---------------------------------------------------------------------------
// The asynchronous client, fed by hand
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Outbox(Arc<Mutex<Vec<Vec<u8>>>>);

impl Outbox {
    fn take(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl Sink for Outbox {
    fn send(&mut self, message: &[u8]) -> Result<(), TransportError> {
        self.0.lock().unwrap().push(message.to_vec());
        Ok(())
    }
    fn close(&mut self) {}
}

struct Harness {
    client: AsyncClient<ManualClock>,
    outbox: Outbox,
    server: Connection,
    clock: ManualClock,
}

impl Harness {
    /// A session opened with "good" credentials.
    fn opened() -> Self {
        let clock = ManualClock::new(T0);
        let outbox = Outbox::default();
        let (offer, handlers, _) = offers();
        let client = AsyncClient::start(
            Box::new(outbox.clone()),
            clock.clone(),
            offer,
            handlers,
            Some(bearer("good")),
        )
        .unwrap();
        let mut harness = Harness {
            client,
            outbox,
            server: server(),
            clock,
        };
        let client = harness.client.clone();
        harness.drive(client.opened()).unwrap();
        harness
    }

    /// Carry what the client sent to the server, and the answers back.
    fn carry(&mut self) -> usize {
        let sent = self.outbox.take();
        let mut replies = Vec::new();
        for message in &sent {
            replies.extend(
                self.server
                    .receive(message, self.clock.now())
                    .send
                    .into_iter()
                    .map(|o| o.bytes),
            );
        }
        for reply in &replies {
            self.client.received(reply);
        }
        sent.len() + replies.len()
    }

    /// Poll `future` to its end, carrying messages whenever it waits.
    fn drive<F: std::future::Future>(&mut self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        loop {
            if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
            assert!(self.carry() > 0, "the future waits, and nothing is left to carry");
        }
    }

    /// Let the server's time reach `seconds` after `T0`, and tell the client
    /// whatever the server says on its own.
    fn server_waits_until(&mut self, seconds: i64) {
        self.clock.set(T0 + seconds * S);
        let step = self.server.tick(T0 + seconds * S);
        for out in &step.send {
            self.client.received(&out.bytes);
        }
    }
}

/// The test that proves renewal matters: with it, the session lives past the
/// moment the first authorization ran out; the next test is the same without.
#[test]
fn a_renewed_session_outlives_its_first_authorization() {
    let mut harness = Harness::opened();
    let client = harness.client.clone();

    harness.drive(client.reauthorize(&bearer("renewed"))).unwrap();

    harness.server_waits_until(FIRST_EXPIRY + 80);
    assert!(
        harness.drive(client.ping()).is_ok(),
        "the same session answers after the first authorization's end"
    );
    let heard = client.unsolicited();
    assert!(
        heard.iter().any(|e| matches!(e, Event::Reauthorized { .. })),
        "the renewal is said to whoever listens to the session: {heard:?}"
    );
    assert!(
        !heard.iter().any(|e| matches!(e, Event::Closed { .. })),
        "{heard:?}"
    );
}

/// Nobody renews: the server says `EAUTHORIZATION_EXPIRED`, then closes
/// (§3.13.5 point 3, p.90), and the client is told the session is over.
#[test]
fn without_a_renewal_the_server_closes_the_session() {
    let mut harness = Harness::opened();
    let client = harness.client.clone();

    harness.server_waits_until(FIRST_EXPIRY + 80);

    let heard = client.unsolicited();
    assert!(
        heard.iter().any(|e| matches!(e, Event::Closed { .. })),
        "{heard:?}"
    );
    // A request on the ended session is refused by name, not left waiting.
    assert!(matches!(
        harness.drive(client.call(&GetResources::default())),
        Err(ClientError::Closed { .. })
    ));
}

/// §5.3.7: `success = false` with challenges. The call says it with the
/// challenges, the session hears the event, and the session is still open
/// (its first authorization has not run out): a ping works, and a good
/// renewal after the refused one is still in time.
#[test]
fn a_refused_renewal_gives_its_challenges_and_leaves_the_session_open() {
    let mut harness = Harness::opened();
    let client = harness.client.clone();

    let refused = harness.drive(client.reauthorize(&bearer("stale")));
    let Err(ClientError::AuthorizationNeeded { challenges }) = refused else {
        panic!("the refusal is an AuthorizationNeeded: {refused:?}")
    };
    assert!(challenges[0].contains("authz_server"), "{challenges:?}");

    let heard = client.unsolicited();
    let event = heard
        .iter()
        .find_map(|e| match e {
            Event::AuthorizationRefused { challenges, .. } => Some(challenges.clone()),
            _ => None,
        })
        .expect("the refusal is an event of the session");
    assert_eq!(event, challenges);
    // No credential in the event, whatever its form.
    assert!(!format!("{heard:?}").contains("stale"), "{heard:?}");

    assert!(harness.drive(client.ping()).is_ok(), "still open");
    harness.drive(client.reauthorize(&bearer("renewed"))).unwrap();
    harness.server_waits_until(FIRST_EXPIRY + 80);
    assert!(
        harness.drive(client.ping()).is_ok(),
        "in time: still open past the end"
    );
}

// ---------------------------------------------------------------------------
// The blocking client, over a transport that is the server in memory
// ---------------------------------------------------------------------------

struct InMemory {
    server: Connection,
    replies: VecDeque<Vec<u8>>,
    clock: ManualClock,
}

impl Transport for InMemory {
    fn send(&mut self, message: &[u8]) -> Result<(), TransportError> {
        let step = self.server.receive(message, self.clock.now());
        self.replies.extend(step.send.into_iter().map(|o| o.bytes));
        Ok(())
    }

    fn receive(&mut self, _wait: Duration) -> Result<Option<Vec<u8>>, TransportError> {
        Ok(self.replies.pop_front())
    }

    fn close(&mut self) {}
}

fn blocking() -> (Client<InMemory, ManualClock>, ManualClock) {
    let clock = ManualClock::new(T0);
    let (offer, handlers, _) = offers();
    let transport = InMemory {
        server: server(),
        replies: VecDeque::new(),
        clock: clock.clone(),
    };
    let client = Client::open(
        transport,
        clock.clone(),
        offer,
        handlers,
        Some(bearer("good")),
        Settings::default(),
    )
    .unwrap();
    (client, clock)
}

/// The blocking client has the same door, with the same two answers.
#[test]
fn the_blocking_client_renews_and_hears_a_refusal() {
    let (mut client, _clock) = blocking();

    match client.reauthorize(&bearer("stale")) {
        Err(ClientError::AuthorizationNeeded { challenges }) => {
            assert!(challenges[0].contains("authz_server"), "{challenges:?}");
        }
        other => panic!("a refusal expected: {other:?}"),
    }
    assert!(client.ping().is_ok(), "the session is still open");

    client.reauthorize(&bearer("renewed")).unwrap();
    let heard = client.unsolicited();
    assert!(
        heard
            .iter()
            .any(|e| matches!(e, Event::AuthorizationRefused { .. })),
        "{heard:?}"
    );
    assert!(
        heard.iter().any(|e| matches!(e, Event::Reauthorized { .. })),
        "{heard:?}"
    );
}

// ---------------------------------------------------------------------------
// Renewing from a source
// ---------------------------------------------------------------------------

/// A source that gives the tokens it is scripted to give, in order, with the
/// token itself as the witness of what it gave (the tokens here are fake
/// strings), and records which witnesses came back.
struct Script {
    give: Mutex<VecDeque<&'static str>>,
    refused: Mutex<Vec<(String, Vec<String>)>>,
}

impl Script {
    fn new(tokens: &[&'static str]) -> Self {
        Self {
            give: Mutex::new(tokens.iter().copied().collect()),
            refused: Mutex::new(Vec::new()),
        }
    }

    fn next(&self) -> Option<Fresh> {
        let token = self.give.lock().unwrap().pop_front()?;
        Some(Fresh {
            credentials: bearer(token),
            witness: Witness::new(token.to_string()),
            valid_until: Some(std::time::UNIX_EPOCH + Duration::from_secs(42)),
        })
    }

    fn refusals(&self) -> Vec<String> {
        self.refused
            .lock()
            .unwrap()
            .iter()
            .map(|(token, _)| token.clone())
            .collect()
    }
}

impl CredentialSource for Script {
    fn credentials(&self) -> Result<Fresh, SourceError> {
        self.next()
            .ok_or_else(|| SourceError("nothing left to give".into()))
    }

    fn refused(&self, witness: Witness, why: &Refused) -> Result<Fresh, GaveUp> {
        let token = witness.downcast::<String>().expect("the witness it gave");
        self.refused
            .lock()
            .unwrap()
            .push((token, why.challenges().to_vec()));
        self.next().ok_or_else(|| GaveUp("nothing else to try".into()))
    }
}

/// A refused renewal goes back to the source ONCE, with the witness of the
/// credentials that were refused and the server's challenges; the next
/// credentials are sent and accepted, and when they end is given back.
#[test]
fn a_refused_renewal_goes_back_to_the_source_once() {
    let mut harness = Harness::opened();
    let client = harness.client.clone();
    let source = Script::new(&["stale", "renewed"]);

    let until = harness.drive(client.reauthorize_with(&source)).unwrap();

    assert_eq!(until, Some(std::time::UNIX_EPOCH + Duration::from_secs(42)));
    assert_eq!(source.refusals(), ["stale"]);
    assert!(source.refused.lock().unwrap()[0].1[0].contains("authz_server"));
    harness.server_waits_until(FIRST_EXPIRY + 80);
    assert!(
        harness.drive(client.ping()).is_ok(),
        "renewed with the second credentials"
    );
}

/// The rule is applied once: a second refusal is the error, and the source
/// is not asked a third time.
#[test]
fn a_second_refusal_is_the_error() {
    let mut harness = Harness::opened();
    let client = harness.client.clone();
    let source = Script::new(&["stale", "staler", "renewed"]);

    let result = harness.drive(client.reauthorize_with(&source));

    assert!(
        matches!(result, Err(ClientError::AuthorizationNeeded { .. })),
        "{result:?}"
    );
    assert_eq!(source.refusals(), ["stale"], "told once");
    assert_eq!(
        source.give.lock().unwrap().len(),
        1,
        "the third was never asked for"
    );
    assert!(harness.drive(client.ping()).is_ok(), "the session is still open");
}

/// A source with nothing to give, or that gives up, is named: no credentials
/// is not a refusal of the server's.
#[test]
fn a_source_that_gives_no_credentials_or_gives_up_is_named() {
    let mut harness = Harness::opened();
    let client = harness.client.clone();

    let empty = Script::new(&[]);
    match harness.drive(client.reauthorize_with(&empty)) {
        Err(ClientError::Invalid(incident)) => {
            assert!(
                incident.to_string().contains("nothing left to give"),
                "{incident}"
            );
        }
        other => panic!("not sent, and said: {other:?}"),
    }

    // Fixed credentials are a source with no other to give.
    let result = harness.drive(client.reauthorize_with(&bearer("stale")));
    assert!(
        matches!(result, Err(ClientError::AuthorizationNeeded { .. })),
        "{result:?}"
    );
}

/// The same door on the blocking client.
#[test]
fn the_blocking_client_renews_from_a_source() {
    let (mut client, _clock) = blocking();
    let source = Script::new(&["stale", "renewed"]);

    assert!(client.reauthorize_with(&source).unwrap().is_some());
    assert_eq!(source.refusals(), ["stale"]);
}
