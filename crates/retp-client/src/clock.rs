//! The time the connection is given.
//!
//! `retp-proto` never reads a clock (`timeouts_read_the_clock_they_are_given`
//! holds it to that); the client reads one here and passes it on, so a test
//! can replace it with a number it moves.

/// Microseconds since the Unix epoch, UTC - ETP's `currentDateTime` unit.
pub trait Clock {
    /// Now.
    fn now(&self) -> i64;
}

/// The system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
    }
}
