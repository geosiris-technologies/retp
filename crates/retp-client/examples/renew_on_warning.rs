//! Keeping a live session open past its token, with a source of credentials of your own.
//!
//! The session is opened from a `CredentialSource`; a `Queue` on `Filter::Session` hands the
//! session's own events to the main thread, which calls `LiveSession::renew` when the server
//! warns that the authorization runs out (`Event::AuthorizationExpiring`) or when its own
//! clock says so (a margin before the end of the credentials in use).
//!
//! The token is read from a file, each time the source is asked: whatever gets the tokens
//! (another program, a script, you) writes the new one there. The file is named by the
//! environment, never the token itself:
//!
//! ```text
//! RETP_TOKEN_FILE=token.txt cargo run --example renew_on_warning --features live -- wss://host/etp
//! ```
//!
//! `RETP_TOKEN_SECONDS` (optional) is how long a token lasts: it gives the source's `valid_until`,
//! and so the program's own clock. Without it the program renews at the server's warning only.
//!
//! Two traps, both avoided here:
//! - never renew from inside a `Destination`: it runs on the session's dispatcher thread, which
//!   would wait for the answer, and the destinations behind it with it. The queue passes the
//!   event to a thread that is not the session's;
//! - never send again credentials the server already has: a server may grant such an `Authorize`
//!   without moving the end and warn again at once, and answering every warning with the same
//!   token loops. The source refuses to give a token it has already given, and the loop stays
//!   quiet for a while after a renewal that did not move the end.

use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use retp_client::live::{Filter, LiveSession, LiveSettings, Queue};
use retp_client::native::{self, Connect, Endpoint};
use retp_client::{
    ClientError, CredentialSource, Fresh, GaveUp, Refused, SourceError, Witness, reading_client,
};
use retp_proto::connection::Event;
use retp_proto::credentials::Credentials;

/// Renew this long before the end of the credentials in use.
const MARGIN: Duration = Duration::from_secs(60);
/// After a renewal that did not move the end, or that failed, do not renew again for this long.
const QUIET: Duration = Duration::from_secs(30);
/// How long to wait for an event when no deadline is known.
const IDLE: Duration = Duration::from_secs(3600);

/// The application's own way of getting a token, here a file that something else rewrites.
/// A real source fetches a new token in `credentials` (a refresh grant, a call to a vault...).
struct TokenFile {
    path: String,
    given: Mutex<Given>,
}

/// What the source gave last.
#[derive(Default)]
struct Given {
    token: Option<String>,
    end: Option<SystemTime>,
}

impl TokenFile {
    /// When the credentials given last end, if the source knows. `connect_with_source` does not
    /// return the end of the credentials it opened with: the source remembers it.
    fn end(&self) -> Option<SystemTime> {
        self.given
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .end
    }
}

impl CredentialSource for TokenFile {
    fn credentials(&self) -> Result<Fresh, SourceError> {
        // The refusal names the file, never what it holds.
        let token = std::fs::read_to_string(&self.path)
            .map_err(|why| SourceError(format!("the token file {} cannot be read: {why}", self.path)))?;
        let token = token.trim();
        let mut given = self.given.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // The rule: credentials newer than the last ones given, or none.
        if given.token.as_deref() == Some(token) {
            return Err(SourceError(
                "the token has not changed since it was last given".into(),
            ));
        }
        let credentials = Credentials::bearer(token).map_err(|why| SourceError(why.to_string()))?;
        let valid_until = std::env::var("RETP_TOKEN_SECONDS")
            .ok()
            .and_then(|seconds| seconds.parse::<u64>().ok())
            .map(|seconds| SystemTime::now() + Duration::from_secs(seconds));
        *given = Given {
            token: Some(token.to_string()),
            end: valid_until,
        };
        Ok(Fresh {
            credentials,
            witness: Witness::none(),
            valid_until,
        })
    }

    fn refused(&self, _: Witness, _: &Refused) -> Result<Fresh, GaveUp> {
        Err(GaveUp("this source has no other token to try".into()))
    }
}

fn connect_to(url: &str) -> Connect {
    let (offer, handlers) = reading_client(
        "renew-on-warning",
        env!("CARGO_PKG_VERSION"),
        native::random_uuid(),
        &[],
    );
    Connect {
        endpoint: Endpoint::new(url),
        offer,
        handlers,
        max_message: None,
    }
}

/// The moment to renew for an authorization that ends at `end`.
fn due_for(end: SystemTime) -> Instant {
    let left = end.duration_since(SystemTime::now()).unwrap_or_default();
    Instant::now() + left.saturating_sub(MARGIN)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .ok_or("usage: renew_on_warning wss://host/etp")?;
    let path = std::env::var("RETP_TOKEN_FILE").map_err(|_| "RETP_TOKEN_FILE names no token file")?;
    let source = Arc::new(TokenFile {
        path,
        given: Mutex::new(Given::default()),
    });
    let session =
        LiveSession::connect_with_source(source.clone(), move || connect_to(&url), LiveSettings::default())?;
    let (queue, events) = Queue::bounded(64);
    session.route(Filter::Session, Arc::new(queue));
    println!("the session is open");

    let mut end = source.end(); // of the credentials in use
    let mut due = end.map(due_for); // when our own clock says to renew
    let mut quiet_until = Instant::now(); // warnings before this are not answered
    loop {
        let wait = due.map_or(IDLE, |at| at.saturating_duration_since(Instant::now()));
        let renew = match events.recv_timeout(wait) {
            Ok(Event::AuthorizationExpiring) => {
                println!("the server warns that the authorization runs out");
                Instant::now() >= quiet_until
            }
            Ok(Event::Closed { reason }) => {
                println!("the session is closed: {reason}");
                break;
            }
            // `Event` is non-exhaustive: anything else is not for this loop, and is not printed.
            Ok(_) => false,
            Err(RecvTimeoutError::Timeout) => due.is_some_and(|at| Instant::now() >= at),
            Err(RecvTimeoutError::Disconnected) => break,
        };
        if !renew {
            continue;
        }
        match session.renew() {
            Ok(new_end) => {
                let moved = match (end, new_end) {
                    (Some(old), Some(new)) => new > old,
                    _ => true,
                };
                end = new_end;
                if moved {
                    println!("renewed");
                    due = new_end.map(due_for);
                } else {
                    println!("renewed, but the end did not move: quiet for a while");
                    quiet_until = Instant::now() + QUIET;
                    due = Some(quiet_until);
                }
            }
            Err(ClientError::AuthorizationNeeded { challenges }) => {
                // The session stays open until the authorization it holds ends. This example
                // stops; an application would tell its user, or fetch other credentials.
                println!("the renewal was refused: {}", challenges.join("; "));
                break;
            }
            Err(ClientError::Closed { reason }) => {
                println!("the session is closed: {reason}");
                break;
            }
            Err(other) => {
                // For instance a source with no newer token yet: try again later.
                println!("could not renew: {other}");
                quiet_until = Instant::now() + QUIET;
                due = Some(quiet_until);
            }
        }
    }
    let _ = session.close("done");
    Ok(())
}
