//! The Python bridge of `retp` - **conversion, and nothing else**.
//!
//! A Python user talks to an ETP v1.2 server through the Rust client:
//! `retp-client`'s blocking API for the session and the operations,
//! `energyml-etp-store` for what turns an EnergyML object into a data object
//! and back. The bridge converts arguments and results; it owns no operation.
//! What a Python user needs and the Rust API lacks is a gap to add in Rust
//! first (`DESIGN.md`, section 9), not a function to write here.
//!
//! # Nothing here decides anything
//!
//! What can be wrong lives in [`session`], plain Rust that `cargo test`
//! exercises with no interpreter and no server. A pyo3 binding only runs after
//! a `maturin develop`; code written at the boundary is code the suite never
//! sees.
//!
//! # Arrays cross into numpy **by copy**
//!
//! For the reason `energyml-rust`'s bridge gives: a view would share memory
//! whose lifetime is not Python's.
//!
//! Usage: `docs/python.md`.

pub mod session;

// **The binding itself sits behind the `python` feature, and the default is
// off**: a workspace member that requires an interpreter requires it for every
// other member, through feature unification. maturin turns `python` on.
#[cfg(feature = "python")]
pub mod bindings;
