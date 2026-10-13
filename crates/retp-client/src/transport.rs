//! What carries the bytes: one complete WebSocket message at a time.

use std::fmt;
use std::time::Duration;

/// A transport failed - the connection is gone or never came.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportError {
    /// What failed, as the transport says it.
    pub why: String,
}

impl TransportError {
    /// A failure, described.
    pub fn new(why: impl Into<String>) -> Self {
        Self { why: why.into() }
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.why)
    }
}

impl std::error::Error for TransportError {}

/// What a browser's own message about a failed `fetch` is worth adding, when
/// the message itself says nothing of why: `Failed to fetch` (Chrome, Edge),
/// `NetworkError when attempting to fetch resource` (Firefox) and `Load
/// failed` (Safari) are the one text a browser gives for CORS, a certificate
/// it refuses, DNS, or a server simply unreachable alike - it never says
/// which. A front end cannot tell those apart from the message alone, so this
/// names the most likely one **as a guess**, never a diagnosis: a native
/// client (`energyml etp`, `curl`) is not subject to CORS and would say more.
pub fn browser_fetch_hint(message: &str) -> Option<&'static str> {
    const GENERIC: [&str; 3] = [
        "Failed to fetch",
        "NetworkError when attempting to fetch resource",
        "Load failed",
    ];
    GENERIC.iter().any(|needle| message.contains(needle)).then_some(
        "a browser gives this same message for CORS, a refused certificate, DNS or an unreachable server alike; \
most often it is CORS - the server did not answer the preflight, or did not name this page's origin. \
A command-line client is not subject to CORS and would say more",
    )
}

/// Carries whole WebSocket messages - never frames: ETP works on the complete
/// message (§3.5.3 §2.a), and reassembling frames is the transport's job.
pub trait Transport {
    /// Send one message.
    fn send(&mut self, message: &[u8]) -> Result<(), TransportError>;

    /// Wait at most `wait` for one message; `None` if none came.
    fn receive(&mut self, wait: Duration) -> Result<Option<Vec<u8>>, TransportError>;

    /// Close the connection. Closing twice is not an error.
    fn close(&mut self);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The generic message browsers give for a fetch that failed at the
    /// network level gets a CORS-first hint; anything else is left alone.
    #[test]
    fn a_browsers_generic_fetch_failure_gets_a_cors_hint() {
        assert!(browser_fetch_hint("Failed to fetch").is_some());
        assert!(browser_fetch_hint("NetworkError when attempting to fetch resource").is_some());
        assert!(browser_fetch_hint("Load failed").is_some());
        assert!(browser_fetch_hint("HTTP 404 Not Found").is_none());
        assert!(browser_fetch_hint("").is_none());
    }
}
