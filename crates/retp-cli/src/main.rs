//! `retp` - the ETP command line. Everything is in `etp.rs`; this file only
//! parses the arguments and returns the exit code.

use std::process::ExitCode;

use clap::Parser;

mod etp;

fn main() -> ExitCode {
    match etp::run(etp::EtpArgs::parse()) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}
