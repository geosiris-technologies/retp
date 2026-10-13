//! What survives a session: the watermark, and nothing that would pass for
//! a session (ADR-0003, issue #232).
//!
//! ETP v1.2 has **no session survivability** (§5.2.2 row 2.5): a session dies
//! with its WebSocket, and its `sessionId` is for debugging. What a client
//! keeps from one session to the next is the last time it heard from the
//! server - the watermark - and what it needs to read it: the server's
//! `ChangeRetentionPeriod` and `earliestRetainedChangeTime` (§26.6.2.1).
//! [`Continuity`] holds exactly that, and **has no field for a session id**:
//! a later session is a new one, authorized again, from the start of the
//! [`Lifecycle`].

use std::collections::BTreeMap;

use retp_avro::Role;

use crate::capabilities::names;
use crate::negotiate::Negotiated;
use crate::wire::{EtpError, Incident};

/// The seven states of a client's connection, and the one it starts from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lifecycle {
    /// Nothing open.
    Disconnected,
    /// `GET …/.well-known/etp-server-capabilities` (§4.3.1).
    Probing,
    /// The WebSocket handshake (§4.3).
    Connecting,
    /// `Authorize`/`AuthorizeResponse` - passed through even when the server
    /// asks nothing, never skipped (§5.2.1.1).
    Authorizing,
    /// `RequestSession` → `OpenSession`.
    Negotiating,
    /// The watermark against the retention window (§26.6.3).
    CatchingUp,
    /// Ordinary traffic.
    Open,
    /// The WebSocket fell; requests in flight have an unknown outcome.
    Interrupted,
}

impl Lifecycle {
    /// The next state on the way to `Open`.
    ///
    /// From `Interrupted` the way back **starts over at `Probing`** - the
    /// probe, the handshake, the authorization, the negotiation, every one of
    /// them. A shortcut that resumed at `Negotiating` is the authorization
    /// bypass of OSDU's #232, reproduced here.
    pub fn next(self) -> Lifecycle {
        match self {
            Lifecycle::Disconnected | Lifecycle::Interrupted => Lifecycle::Probing,
            Lifecycle::Probing => Lifecycle::Connecting,
            Lifecycle::Connecting => Lifecycle::Authorizing,
            Lifecycle::Authorizing => Lifecycle::Negotiating,
            Lifecycle::Negotiating => Lifecycle::CatchingUp,
            Lifecycle::CatchingUp | Lifecycle::Open => Lifecycle::Open,
        }
    }
}

/// What a client keeps from one session to the next - and nothing else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Continuity {
    /// The last `currentDateTime` heard from the server, in microseconds.
    pub watermark: i64,
    /// The server's `ChangeRetentionPeriod`, in seconds.
    pub retention_seconds: i64,
    /// The server's `earliestRetainedChangeTime`, in microseconds.
    pub earliest_retained: i64,
    /// The server's `ChangePropagationPeriod`, in seconds: how long a change
    /// may take to become visible. Subtracted from the watermark when asking
    /// what changed (§26.6.3 §3.a).
    pub propagation_seconds: i64,
    /// The protocols of the session, and this end's role in each - to know
    /// what to ask again.
    pub protocols: BTreeMap<i32, Role>,
}

/// The watermark fell outside what the server keeps: catching up is not
/// possible, everything must be reloaded (§26.6.3 §3.c).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullReloadNeeded {
    /// The watermark kept.
    pub watermark: i64,
    /// The oldest change the server still has.
    pub oldest_kept: i64,
}

impl FullReloadNeeded {
    /// The error the server would answer a catch-up that far back with.
    pub fn incident(&self) -> Incident {
        EtpError::ERETENTION_PERIOD_EXCEEDED.with_detail(format!(
            "the last change seen is at {}, and the server keeps changes from {} on: a full reload is needed",
            self.watermark, self.oldest_kept
        ))
    }
}

impl Continuity {
    /// What to keep of `session`, whose server was last heard at `watermark`.
    pub fn keep(session: &Negotiated, watermark: i64) -> Self {
        Self {
            watermark,
            retention_seconds: session
                .theirs
                .long(names::CHANGE_RETENTION_PERIOD)
                .expect("ChangeRetentionPeriod has a default"),
            earliest_retained: session.peer_earliest_retained,
            propagation_seconds: session
                .theirs
                .long(names::CHANGE_PROPAGATION_PERIOD)
                .expect("ChangePropagationPeriod has a default"),
            protocols: session.protocols.iter().map(|(p, a)| (*p, a.role)).collect(),
        }
    }

    /// From when to ask for changes, at `now` - or the news that it is too late.
    ///
    /// The oldest change a server keeps is `now` less its retention period -
    /// **or** its `earliestRetainedChangeTime`, when that is more recent: a
    /// server that restarted yesterday has lost what came before, whatever
    /// its retention period says (§26.6.2.1).
    ///
    /// What is asked for is the watermark **less the propagation period**,
    /// and the spec recommends it for a reason worth stating: a change made
    /// just before the cut may only have become visible just after, and
    /// asking from the watermark exactly would step over it (§26.6.3 §3.a).
    /// Asking too early costs a few objects read twice - a read is
    /// idempotent - and asking too late loses a change silently, which is
    /// the failure this repository refuses everywhere else.
    ///
    /// The same moved time decides whether it is too late: a watermark the
    /// delta pushes out of the window is out of it, and the server would
    /// answer `ERETENTION_PERIOD_EXCEEDED` (5001) to the request.
    pub fn window(&self, now: i64) -> Result<i64, FullReloadNeeded> {
        let by_retention = now.saturating_sub(self.retention_seconds.saturating_mul(1_000_000));
        let oldest_kept = by_retention.max(self.earliest_retained);
        let asked = self
            .watermark
            .saturating_sub(self.propagation_seconds.saturating_mul(1_000_000));
        if asked < oldest_kept {
            return Err(FullReloadNeeded {
                watermark: self.watermark,
                oldest_kept,
            });
        }
        Ok(asked)
    }
}
