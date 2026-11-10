//! A REST client that takes its credentials from a source: one request, one
//! credential, and a witness that goes back to the source when THAT credential
//! is refused.
//!
//! The server is a fake [`Http`] written here, which refuses the credentials
//! it is told to with a `401` and its challenge (RFC 6750 section 3), and
//! answers `200` with an empty list otherwise. The rule under test is the one
//! the notes of the source give: a `401` goes back to the source **once**; the
//! credentials it gives next are tried; a second `401` is the error.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use retp_client::rest::{Http, HttpRequest, HttpResponse, RestClient};
use retp_client::{
    ClientError, CredentialSource, Fresh, GaveUp, Refused, SourceError, TransportError, Witness, asynchronous,
};
use retp_proto::credentials::Credentials;

/// What a witness holds in these tests: the number of the credentials it came
/// with.
#[derive(Debug, PartialEq, Eq)]
struct Issued(u64);

/// A source that numbers what it gives (`tok0`, `tok1`…) and records which
/// witnesses came back to it.
struct Tokens {
    next: AtomicU64,
    refused: Mutex<Vec<u64>>,
    /// Refusals it was told, with the challenges, to check they are the server's.
    heard: Mutex<Vec<Refused>>,
    gives_up: bool,
}

impl Tokens {
    fn new(gives_up: bool) -> Arc<Self> {
        Arc::new(Self {
            next: AtomicU64::new(0),
            refused: Mutex::new(Vec::new()),
            heard: Mutex::new(Vec::new()),
            gives_up,
        })
    }

    fn issue(&self) -> Fresh {
        let number = self.next.fetch_add(1, Ordering::SeqCst);
        Fresh {
            credentials: Credentials::bearer(&format!("tok{number}")).unwrap(),
            witness: Witness::new(Issued(number)),
            valid_until: None,
        }
    }

    fn refusals(&self) -> Vec<u64> {
        self.refused.lock().unwrap().clone()
    }
}

impl CredentialSource for Tokens {
    fn credentials(&self) -> Result<Fresh, SourceError> {
        Ok(self.issue())
    }

    fn refused(&self, witness: Witness, why: &Refused) -> Result<Fresh, GaveUp> {
        // What it learns of THIS refusal comes from the witness, and from
        // nothing it kept: the same source serves other connections.
        let Issued(number) = witness.downcast::<Issued>().expect("the witness it gave");
        self.refused.lock().unwrap().push(number);
        self.heard.lock().unwrap().push(why.clone());
        if self.gives_up {
            Err(GaveUp("no other credentials".into()))
        } else {
            Ok(self.issue())
        }
    }
}

/// A server that answers 401 to the `authorization` it is told to refuse.
struct FakeHttp {
    seen: Arc<Mutex<Vec<String>>>,
    refuses: Box<dyn Fn(&str) -> bool + Send>,
}

impl FakeHttp {
    fn new(refuses: impl Fn(&str) -> bool + Send + 'static) -> (Self, Arc<Mutex<Vec<String>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                seen: seen.clone(),
                refuses: Box::new(refuses),
            },
            seen,
        )
    }
}

impl Http for FakeHttp {
    fn send(
        &mut self,
        request: HttpRequest,
    ) -> impl std::future::Future<Output = Result<HttpResponse, TransportError>> {
        let authorization = request
            .headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .map(|(_, value)| value.clone())
            .unwrap_or_default();
        self.seen.lock().unwrap().push(authorization.clone());
        let refused = (self.refuses)(&authorization);
        std::future::ready(Ok(HttpResponse {
            status: if refused { 401 } else { 200 },
            challenge: refused.then(|| "Bearer realm=\"api\"".to_string()),
            body: b"[]".to_vec(),
        }))
    }
}

/// A future of the fake is over at its first poll.
fn now<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("the fake transport answers at its first poll"),
    }
}

fn client(http: FakeHttp, source: Arc<Tokens>) -> RestClient<FakeHttp> {
    RestClient::new(http, "http://rest.example/api", None, None).with_source(source)
}

fn list(client: &mut RestClient<FakeHttp>) -> Result<(), ClientError> {
    now(asynchronous::list_dataspaces(client)).map(|_| ())
}

/// A 401 goes back to the source with the witness of the credentials that
/// were refused; what it gives next is sent, and answered 200: the request
/// succeeded, and was sent twice - the first time the server refused before
/// doing anything.
#[test]
fn a_401_then_a_200_asks_the_source_once_more() {
    let source = Tokens::new(false);
    let (http, seen) = FakeHttp::new(|authorization| authorization == "Bearer tok0");
    let mut rest = client(http, source.clone());

    list(&mut rest).expect("the second credentials are accepted");

    assert_eq!(*seen.lock().unwrap(), ["Bearer tok0", "Bearer tok1"]);
    assert_eq!(source.refusals(), [0], "the witness of the credentials refused");
    // The refusal told the source what the server said, and where.
    assert_eq!(
        *source.heard.lock().unwrap(),
        [Refused::Rest {
            challenges: vec!["Bearer realm=\"api\"".to_string()]
        }]
    );
}

/// The rule is applied once: a second 401 is the error, with the server's
/// challenge, and the source is not asked a third time.
#[test]
fn a_second_401_is_the_error() {
    let source = Tokens::new(false);
    let (http, seen) = FakeHttp::new(|_| true);
    let mut rest = client(http, source.clone());

    match list(&mut rest) {
        Err(ClientError::AuthorizationNeeded { challenges }) => {
            assert_eq!(challenges, ["Bearer realm=\"api\""]);
        }
        other => panic!("the second 401 is the error: {other:?}"),
    }
    assert_eq!(seen.lock().unwrap().len(), 2, "tried, tried again, no more");
    assert_eq!(source.refusals(), [0], "the source was told once");
}

/// A source that gives up leaves the 401 as the error, and the request is
/// not sent again.
#[test]
fn a_source_that_gives_up_leaves_the_401() {
    let source = Tokens::new(true);
    let (http, seen) = FakeHttp::new(|_| true);
    let mut rest = client(http, source.clone());

    assert!(matches!(
        list(&mut rest),
        Err(ClientError::AuthorizationNeeded { .. })
    ));
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(source.refusals(), [0]);
}

/// Each request asks the source: two requests, two credentials; none of them
/// refused, the source hears of no refusal.
#[test]
fn every_request_asks_the_source() {
    let source = Tokens::new(false);
    let (http, seen) = FakeHttp::new(|_| false);
    let mut rest = client(http, source.clone());

    list(&mut rest).unwrap();
    list(&mut rest).unwrap();

    assert_eq!(*seen.lock().unwrap(), ["Bearer tok0", "Bearer tok1"]);
    assert!(source.refusals().is_empty());
}

/// Credentials that are fixed stay accepted and behave as before: one 401 is
/// the error, and nothing else is tried.
#[test]
fn fixed_credentials_still_work_and_a_401_is_the_error() {
    let (http, seen) = FakeHttp::new(|_| true);
    let mut rest = RestClient::new(
        http,
        "http://rest.example/api",
        Some(Credentials::bearer("fixed").unwrap()),
        None,
    );
    assert!(matches!(
        list(&mut rest),
        Err(ClientError::AuthorizationNeeded { .. })
    ));
    assert_eq!(*seen.lock().unwrap(), ["Bearer fixed"], "sent once");
}

/// Two connections, ONE source. The server refuses the credentials of the
/// first while the second connection asks the source for its own (here: in
/// the middle of the first's request). The refusal that comes back to the
/// source carries the witness of the credentials that were refused - 0 -, not
/// of the last ones it gave - 1. A source that kept "the last credentials" in
/// a field would answer for the wrong connection.
#[test]
fn two_connections_one_source_the_refusal_names_its_own_credentials() {
    let source = Tokens::new(false);
    let in_the_middle = source.clone();
    let other_connection_asked = Arc::new(Mutex::new(None::<String>));
    let asked = other_connection_asked.clone();
    let (first_http, first_seen) = FakeHttp::new(move |authorization| {
        if authorization == "Bearer tok0" {
            // The other connection (a session, say) opens now.
            let theirs = in_the_middle.issue();
            *asked.lock().unwrap() = Some(theirs.credentials.header().to_string());
            true
        } else {
            false
        }
    });
    let mut first = client(first_http, source.clone());

    list(&mut first).expect("the first connection recovers");

    assert_eq!(
        other_connection_asked.lock().unwrap().as_deref(),
        Some("Bearer tok1"),
        "the other connection holds credentials of its own"
    );
    assert_eq!(
        source.refusals(),
        [0],
        "the witness of the credentials refused, not of the last ones given"
    );
    // The first connection went on with a third credential, and never with the
    // other connection's.
    assert_eq!(*first_seen.lock().unwrap(), ["Bearer tok0", "Bearer tok2"]);
}

/// A secret is in no `Debug`: not of the credentials, not of what a source
/// gave, not of a witness - whatever it holds.
#[test]
fn nothing_secret_is_in_a_debug() {
    let fresh = Fresh {
        credentials: Credentials::bearer("secret-token-value").unwrap(),
        witness: Witness::new("secret-witness-value".to_string()),
        valid_until: None,
    };
    let shown = format!("{fresh:?} {:?} {:?}", fresh.credentials, fresh.witness);
    assert!(!shown.contains("secret-token-value"), "{shown}");
    assert!(!shown.contains("secret-witness-value"), "{shown}");
    let basic = Credentials::basic("someone", "secret-password").unwrap();
    assert!(!format!("{basic:?}").contains("secret-password"));
    assert!(!format!("{basic:?}").contains(&basic.header()["Basic ".len()..]));
}
