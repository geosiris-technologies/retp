//! `retp` - the ETP command line. Everything is in `etp.rs`; this file only
//! parses the arguments and returns the exit code.

use std::process::ExitCode;

use clap::Parser;

mod etp;

fn main() -> ExitCode {
    // `retp azure`, `retp @azure`, `retp --server azure`: rewritten before clap sees the line.
    let argv = match etp::expanded_args() {
        Ok(argv) => argv,
        Err(e) => {
            eprintln!("error: {e:#}");
            return ExitCode::from(2);
        }
    };
    match etp::run(etp::EtpArgs::parse_from(argv)) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}
