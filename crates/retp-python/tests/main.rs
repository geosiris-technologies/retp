//! Every integration test of `retp-python`, as **one** binary.
//!
//! The same rule as `energyml-python`'s: one binary links the generated crates
//! once, instead of one binary per file filling a small runner's disk.
//!
//! **A new test file is a `mod` line here**, otherwise it does not run.
//!
//! None of these needs an ETP server or an interpreter: `operations` talks to a
//! server connection in the same process (`retp_client::memory`). `live` does
//! need a server, and is compiled only with the `live-server` feature.

mod connect;
mod errors;
mod operations;
mod scope;
mod stage;

#[cfg(feature = "live-server")]
mod live;
