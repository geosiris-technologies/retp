//! End-to-end tests over the generated ETP types.
//!
//! Like `energyml-tests` for the EnergyML side: nothing ships from here, the
//! crate exists so the tests have somewhere to live that may depend on the
//! generated code without the generated code depending on it.
//!
//! What is checked, and by which file:
//!
//! * `crossaudit.rs` - the generated types against `fastavro`, on all 138
//!   messages. This is the one that proves the generator, because the bytes
//!   were written by an implementation that has never seen our code.
//! * `metadata.rs` - that what a message says about itself matches the schema
//!   it came from, read again independently.
