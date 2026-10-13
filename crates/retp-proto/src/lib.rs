//! The ETP v1.2 protocol, for a client and a server alike.
//!
//! What ETP **is** - the header, the numbering, the correlation, the
//! multipart rules, the errors - written once, with no transport in it:
//! bytes go in, bytes come out. `retp-client` puts a WebSocket around
//! it; a server could put another. The design is in ADR-0001.
//!
//! **Nothing here assumes which side of the wire it is on.** Which side it is
//! is a parameter ([`wire::Side`]), and the test that fails if that ever stops
//! being true is `server_ids_are_odd`.

#![forbid(unsafe_code)]

pub mod capabilities;
pub mod connection;
pub mod continuity;
pub mod credentials;
pub mod describe;
pub mod negotiate;
pub mod probe;
pub mod uris;
pub mod wire;
