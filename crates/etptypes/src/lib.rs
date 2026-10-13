//! **Moved to [`retp-types`](https://crates.io/crates/retp-types).**
//!
//! This crate used to hold ETP v1.2 types generated for an early proof of
//! concept. Its successor is the `retp` stack
//! (<https://github.com/geosiris-technologies/retp>), and the types now live in
//! `retp-types`. This crate is kept so that existing dependents keep building:
//! it only re-exports `retp-types`.
//!
//! New code should depend on `retp-types` directly. Note that 2.0 is a new
//! major version: the types are not those of 1.x.

pub use retp_types::*;
