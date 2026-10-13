//! Which side of the connection this is, and the message ids it may use.

use retp_avro::Role;

/// Which end opened the WebSocket - the **connection role** (ADR-0001).
///
/// It decides the parity of message ids and who opens the session, and
/// nothing else: what a peer may send in a sub-protocol is decided by its
/// **session role** (`store`, `customer`, …), negotiated per protocol, and
/// independent of this (§3.1.2.1, p.33). The two are two types so that one
/// cannot be passed for the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    /// Opened the connection: even message ids.
    Client,
    /// Accepted it: odd message ids.
    Server,
}

impl Side {
    /// The other end.
    pub const fn peer(self) -> Side {
        match self {
            Side::Client => Side::Server,
            Side::Server => Side::Client,
        }
    }

    /// The role Core's `senderRole` calls this side.
    pub const fn role(self) -> Role {
        match self {
            Side::Client => Role::Client,
            Side::Server => Role::Server,
        }
    }

    /// Whether `id` is one this side may use: non-zero, even for a client,
    /// odd for a server (§3.5.4.1 §5.c, p.56).
    pub const fn may_use(self, id: i64) -> bool {
        id > 0
            && match self {
                Side::Client => id % 2 == 0,
                Side::Server => id % 2 == 1,
            }
    }
}

/// The ids one side hands out: strictly increasing, of its parity, never 0
/// (§3.5.4.1 §5, p.56).
///
/// The specification does not ask for consecutive ids; these are, by two,
/// because nothing is gained by skipping.
#[derive(Debug, Clone)]
pub struct MessageIds {
    side: Side,
    last: i64,
}

impl MessageIds {
    /// The counter of `side`, before its first message.
    pub const fn new(side: Side) -> Self {
        Self { side, last: 0 }
    }

    /// Hand out the next id.
    pub fn issue(&mut self) -> i64 {
        self.last = match (self.side, self.last) {
            (Side::Client, 0) => 2,
            (Side::Server, 0) => 1,
            (_, last) => last
                .checked_add(2)
                .expect("2^63 messages in one session: the id space is exhausted"),
        };
        self.last
    }

    /// The last id handed out, `0` before the first.
    pub const fn last(&self) -> i64 {
        self.last
    }

    /// Whether `id` is one this counter has handed out.
    pub const fn issued(&self, id: i64) -> bool {
        self.side.may_use(id) && id <= self.last
    }
}
