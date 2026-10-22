//! Long operations: steps, a retry limit, and a runner that reconnects and
//! goes on (ADR-0003).
//!
//! The runner knows no operation. It takes a [`Connector`] - what opens a
//! session, and what waits - and an [`Operation`], which moves one step at a
//! time and keeps its own journal. A step that fails for a reason a second
//! try can cure - the connection fell, a request timed out, an answer came
//! back unreadable - is tried again, after a pause that doubles, in a **new
//! session** opened from the start (probe, login, authorization: no shortcut,
//! #232). A step that succeeds resets the count: the operation moved on. The
//! same step failing [`Retry::max_retries`] times in a row stops the run,
//! by the step's name. And a step that keeps failing is **halved** before
//! the count runs out, if the operation can: a server that falls on a
//! message too big for it will fall there again.

use std::fmt;
use std::time::Duration;

use retp_proto::continuity::{Continuity, FullReloadNeeded};

use crate::caller::Caller;
use crate::{Blocking, ClientError};

/// What opens a session, and what waits between two tries.
pub trait Connector {
    /// The sessions it opens - a WebSocket [`Client`](crate::Client), or a
    /// REST client: any client whose calls are over when they return.
    type Session: Blocking;
    /// Open a new session - the whole way: login, handshake, negotiation.
    fn connect(&mut self) -> Result<Self::Session, Box<dyn std::error::Error + Send + Sync>>;

    /// Ask the server to take back the session it has just lost - OSDU's
    /// extension, tried **before** a new session is opened (ADR-0003).
    /// `None`, the default, means this connector does not offer it, and the
    /// caller opens a fresh session; `Some(Err(…))` means it was tried and
    /// refused, which is said and then followed by a fresh session.
    ///
    /// # What OSDU's resume is, read in `open-etp-server`
    ///
    /// Its server keeps a lost session in a map keyed by the session's uuid
    /// and answers a `ResumeSession { sessionId, currentDateTime }` on its
    /// own Core extension - one of the protocols above 2000 that §3.6.1
    /// reserves for this, and which some OSDU services advertise beside 2400's siblings
    /// 2404 and 2424. `ENOT_FOUND` *Session not found or expired* comes back
    /// when the session is gone.
    ///
    /// **Four things are worth knowing before implementing it**, and they
    /// are why this is a door rather than a path:
    ///
    /// 1. it restores the **envelope, not the work**: the negotiated
    ///    protocols, the compression, the message size - `state_`, which
    ///    holds the requests in flight, is an explicit `TODO` in that code.
    ///    So a resumed session has to re-ask exactly what a new one does;
    /// 2. it is **off unless configured** (`session_keep_alive_duration_`),
    ///    and nothing in the capabilities document says whether it is on:
    ///    a client can only try and be told;
    /// 3. it is the **authorization bypass of issue #232** - `user_` and
    ///    `entitlement_` are not carried over, and a missing entitlement is
    ///    read as unrestricted access. A client that leant on it would hold
    ///    rights nobody granted it, which this client forbids by name (ADR-0003);
    /// 4. its **wire format** is published in `open-etp-server`'s
    ///    `etp-1.2.avpr`: the generated `retp_types::protocol::core_osdu`
    ///    holds it, and the connection speaks it
    ///    (`Connection::client_resuming`, `native::connect_resuming`).
    ///
    /// The default below offers no resume: a connector that wants one
    /// overrides it, with `connect_resuming` - which still passes through the
    /// whole authorization, because what is copied from OSDU is its protocol,
    /// not its hole (ADR-0003).
    fn resume(&mut self) -> Option<Result<Self::Session, Box<dyn std::error::Error + Send + Sync>>> {
        None
    }

    /// Wait before trying again.
    fn pause(&mut self, how_long: Duration);
}

/// How a session came back after a cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Came {
    /// The server took the lost session back - OSDU's extension.
    Resumed,
    /// A new session, the whole way from the probe: the login, the
    /// handshake, the authorization, the negotiation. `refused` says why the
    /// resume did not serve, when one was tried at all.
    Fresh {
        /// Why the resume did not serve.
        refused: Option<String>,
    },
}

/// Open a session again after a cut, in the order ADR-0003 fixes: the
/// server's own resume first when the connector offers one, a whole new
/// session otherwise - or when the resume was refused, which is **said**
/// rather than swallowed.
///
/// What this does **not** do is decide what to re-read: that is the
/// watermark's business ([`Continuity::window`]), and the runner below hands
/// it to the operation. A session that came back is a session that knows
/// nothing of what was in flight - on either path, as OSDU's own code says.
pub fn reconnect<K: Connector>(
    connector: &mut K,
) -> Result<(K::Session, Came), Box<dyn std::error::Error + Send + Sync>> {
    let refused = match connector.resume() {
        Some(Ok(session)) => return Ok((session, Came::Resumed)),
        Some(Err(why)) => Some(why.to_string()),
        None => None,
    };
    connector
        .connect()
        .map(|session| (session, Came::Fresh { refused }))
}

/// What a client keeps of `client`, when it has a watermark to keep.
pub fn kept_of<K: Caller>(client: &K) -> Option<Continuity> {
    client
        .watermark()
        .map(|watermark| Continuity::keep(&client.negotiated(), watermark))
}

/// How hard to try.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retry {
    /// How many failures **in a row** one step may have before the run stops.
    pub max_retries: u32,
    /// The pause after the first failure; it doubles after each.
    pub first_pause: Duration,
    /// The longest pause.
    pub longest_pause: Duration,
}

impl Default for Retry {
    fn default() -> Self {
        Self {
            max_retries: 3,
            first_pause: Duration::from_secs(1),
            longest_pause: Duration::from_secs(30),
        }
    }
}

impl Retry {
    /// The pause after the `failures`-th failure in a row.
    pub fn pause(&self, failures: u32) -> Duration {
        let doubled = self
            .first_pause
            .saturating_mul(1 << failures.saturating_sub(1).min(16));
        doubled.min(self.longest_pause)
    }
}

/// What one step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stepped {
    /// It is done; more steps remain.
    Progressed,
    /// It was the last.
    Finished,
}

/// Why a step failed.
#[derive(Debug)]
pub enum StepFailure<E> {
    /// The session failed in a way another try may cure.
    Session(ClientError),
    /// Another try would fail the same way - a file that cannot be written,
    /// a request the specification forbids. The run stops.
    Final(E),
}

impl<E> From<ClientError> for StepFailure<E> {
    fn from(why: ClientError) -> Self {
        StepFailure::Session(why)
    }
}

/// Whether another try may cure this: the connection fell, a request timed
/// out, an answer was refused. A refusal of the request itself would come
/// back the same.
pub fn worth_retrying(why: &ClientError) -> bool {
    why.is_connection_lost()
        || matches!(
            why,
            ClientError::TimedOut { .. } | ClientError::AnswerRefused { .. }
        )
}

/// An operation the runner can drive.
pub trait Operation<K> {
    /// What it reports as it goes.
    type Progress;
    /// Why it stops for good.
    type Error;
    /// Do the next step.
    fn step(&mut self, client: &mut K) -> Result<Stepped, StepFailure<Self::Error>>;
    /// The step it is at, as a person reads it - `objects 150-199 of 620`.
    fn current_step(&self) -> String;
    /// Make the next step smaller; `false` when it cannot be.
    fn halve(&mut self) -> bool;
    /// What happened since the last call.
    fn take_progress(&mut self) -> Vec<Self::Progress>;

    /// A new session is open after a cut, and the step is about to be tried
    /// again. `since` is the time from which changes can be asked - the
    /// watermark less the propagation delay - and `None` means the watermark
    /// fell out of what the server keeps: what changed cannot be known, and
    /// an operation that relied on reading only the difference must read
    /// everything again (§26.6.3 §3.c).
    ///
    /// Does nothing by default: an operation that re-reads its step in full -
    /// which every operation here does - needs no catch-up of its own.
    fn resumed(&mut self, came: &Came, since: Option<i64>) {
        let _ = (came, since);
    }
}

/// Something the runner went through - said, so that a progress bar does not
/// go quiet while it happens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Setback {
    /// A step failed; a new session will be opened after `pause`.
    Reconnecting {
        /// The step.
        step: String,
        /// Failures of this step in a row, this one included.
        attempt: u32,
        /// The limit.
        max: u32,
        /// The wait before the new session.
        pause: Duration,
        /// Why the step failed.
        why: String,
    },
    /// The step failed again and was made smaller.
    Halved {
        /// The step, as it now is.
        step: String,
    },
    /// A session is open again, and the run goes on.
    Reopened {
        /// Whether the server took the lost session back, rather than a new
        /// session being opened from the probe.
        resumed: bool,
        /// Why the server's own resume did not serve, when one was tried.
        refused: Option<String>,
        /// From when changes can be asked - the watermark less the
        /// propagation delay. `None` when nothing was known of the old
        /// session, or when the watermark fell out of the retention window
        /// and everything must be read again.
        since: Option<i64>,
        /// Whether that `None` is the second case: too late to catch up.
        too_late: bool,
    },
}

/// What the runner reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunEvent<P> {
    /// The operation's own progress.
    Progress(P),
    /// A setback of the runner.
    Setback(Setback),
}

/// Why a run stopped before the end.
#[derive(Debug)]
pub enum RunError<E> {
    /// The same step failed `attempts` times in a row.
    GaveUp {
        /// The step.
        step: String,
        /// How many times.
        attempts: u32,
        /// The last failure.
        last: String,
    },
    /// A step was refused in a way no second try would change.
    Refused {
        /// The step.
        step: String,
        /// Why.
        why: ClientError,
    },
    /// The operation stopped for good.
    Final(E),
}

impl<E: fmt::Display> fmt::Display for RunError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunError::GaveUp { step, attempts, last } => {
                write!(
                    f,
                    "the step \u{201c}{step}\u{201d} failed {attempts} times in a row: {last}"
                )
            }
            RunError::Refused { step, why } => {
                write!(f, "the step \u{201c}{step}\u{201d} was refused: {why}")
            }
            RunError::Final(why) => write!(f, "{why}"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for RunError<E> {}

/// Drive `operation` to its end over the sessions `connector` opens.
///
/// `session` is the session to start with, if one is open, and the one left
/// open at the end - so that a shell keeps its own.
pub fn run<K, O>(
    connector: &mut K,
    session: &mut Option<K::Session>,
    operation: &mut O,
    retry: &Retry,
    mut on_event: impl FnMut(RunEvent<O::Progress>),
) -> Result<(), RunError<O::Error>>
where
    K: Connector,
    O: Operation<K::Session>,
{
    let mut failures = 0u32;
    // What the session that is running knew, kept as it goes: once it is
    // gone, its watermark cannot be read any more (ADR-0003).
    let mut kept: Option<Continuity> = session.as_ref().and_then(kept_of);
    // Whether a session has been lost: the first opening of a run is not a
    // coming back, and there is nothing to ask a server to take back.
    let mut came_back = false;
    loop {
        let failed: String = match session.as_mut() {
            None => match if came_back {
                reconnect(connector)
            } else {
                connector
                    .connect()
                    .map(|fresh| (fresh, Came::Fresh { refused: None }))
            } {
                Ok((fresh, came)) => {
                    // What the operation must know before its next step: how
                    // the session came back, and from when changes can be
                    // asked. Both paths lose what was in flight - OSDU's
                    // resume carries the envelope, not the work - so this is
                    // the same news either way.
                    let reached = kept
                        .as_ref()
                        .map(|kept| kept.window(fresh.watermark().unwrap_or(kept.watermark)));
                    let since = match &reached {
                        Some(Ok(since)) => Some(*since),
                        _ => None,
                    };
                    if came_back {
                        on_event(RunEvent::Setback(Setback::Reopened {
                            resumed: came == Came::Resumed,
                            refused: match &came {
                                Came::Fresh { refused } => refused.clone(),
                                Came::Resumed => None,
                            },
                            since,
                            too_late: matches!(&reached, Some(Err(FullReloadNeeded { .. }))),
                        }));
                        operation.resumed(&came, since);
                    }
                    kept = kept_of(&fresh);
                    *session = Some(fresh);
                    continue;
                }
                Err(why) => format!("opening a session: {why}"),
            },
            Some(client) => {
                let stepped = operation.step(client);
                for progress in operation.take_progress() {
                    on_event(RunEvent::Progress(progress));
                }
                match stepped {
                    Ok(Stepped::Finished) => return Ok(()),
                    Ok(Stepped::Progressed) => {
                        failures = 0;
                        // The watermark moved with the step: what a catch-up
                        // would ask from, if this session were to fall now.
                        if let Some(fresh) = session.as_ref().and_then(kept_of) {
                            kept = Some(fresh);
                        }
                        continue;
                    }
                    Err(StepFailure::Final(why)) => return Err(RunError::Final(why)),
                    Err(StepFailure::Session(why)) if !worth_retrying(&why) => {
                        return Err(RunError::Refused {
                            step: operation.current_step(),
                            why,
                        });
                    }
                    Err(StepFailure::Session(why)) => {
                        if why.is_connection_lost() || matches!(why, ClientError::TimedOut { .. }) {
                            // The session is gone, or cannot be trusted with
                            // the next request: a new one.
                            if let Some(old) = session.take() {
                                old.end("energyml: starting over in a new session");
                            }
                            came_back = true;
                        }
                        why.to_string()
                    }
                }
            }
        };
        failures += 1;
        if failures >= retry.max_retries {
            return Err(RunError::GaveUp {
                step: operation.current_step(),
                attempts: failures,
                last: failed,
            });
        }
        if failures >= 2 && operation.halve() {
            on_event(RunEvent::Setback(Setback::Halved {
                step: operation.current_step(),
            }));
        }
        let pause = retry.pause(failures);
        on_event(RunEvent::Setback(Setback::Reconnecting {
            step: operation.current_step(),
            attempt: failures,
            max: retry.max_retries,
            pause,
            why: failed,
        }));
        connector.pause(pause);
    }
}
