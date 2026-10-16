//! One test binary for the crate: a new test file is one `mod` line.
//!
//! Every test works on data it makes itself, in a folder of its own under the
//! system's temporary folder, removed at the end. Nothing is read outside the
//! repository, no real server is reached (a token endpoint is a socket on
//! 127.0.0.1 opened by the test), and no process-wide environment variable is
//! set (the sources and the location take their variables from a closure).

mod common;
mod expand;
mod leak;
mod methods;
mod names;
mod save;
mod secrets;
mod server_test;
mod server_toml;
