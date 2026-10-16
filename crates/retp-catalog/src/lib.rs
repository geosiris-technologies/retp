//! A catalogue of ETP servers for the `retp` command line.
//!
//! A person who talks to the same servers every day should say `retp azure` and
//! not the URL, the tenant and the way to log in. The catalogue keeps one
//! **entry** per server (a `server.toml` in a folder of its own), and turns an
//! entry into what `retp-client` connects with.
//!
//! The rules that shape it:
//!
//! - **An entry holds no secret, only a reference** ([`SecretRef`]: an
//!   environment variable or a file). A literal value for `password`, `token`
//!   or `client_secret` is refused when the file is read, naming the key and the
//!   line. The value lives in a [`Secret`], whose `Debug` shows nothing, from
//!   the moment it is read to the moment the login is built.
//! - **One door to the login.** An entry fills the `Choice` of `retp-client`;
//!   `Choice::login` decides the method, as it does for the command line. The
//!   catalogue only checks that the declared method is the one it gives.
//! - **The ambient `ETP_*` variables are ignored** for a catalogue server: the
//!   token set for one server must not go to another.
//! - **No logic in the faces.** The command line (`retp`, `retp-conformance`)
//!   parses and prints; it calls this crate for the rest. Texts are returned
//!   as strings ([`render`]) so a test reads what a person would.
//!
//! This crate is native only: it reads files and talks to authorization
//! servers. `retp-client` compiles for the browser and does not depend on it.
#![cfg(not(target_arch = "wasm32"))]

mod catalog;
mod check;
mod entry;
mod error;
mod expand;
mod observe;
pub mod render;
mod resolve;
mod secret;
mod util;

pub use catalog::{Catalog, Found, Listed, Location, Reserved};
pub use check::{Network, Outcome, Step, TestReport};
pub use entry::{
    AuthSpec, Draft, Entry, Handshake, Header, HeaderValue, ServerSpec, Tls, Transport, WriteDefaults,
};
pub use error::CatalogError;
pub use expand::{Accepts, Expanded, Grammar, LOGIN_OPTIONS, expand};
pub use observe::{Connection, Observed, observe};
pub use resolve::{Description, Opened, Overrides, ReferenceLine, Resolved};
pub use secret::{OsSecrets, RefStatus, Secret, SecretError, SecretRef, SecretSource};

use std::path::PathBuf;

/// Whether a write to a protected server was confirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirmation {
    /// `--yes`.
    Yes,
    /// What was typed at the prompt.
    Typed(String),
    /// There was no way to ask (no terminal) and no `--yes`.
    Unavailable,
}

/// Open the catalogue where the environment and the OS put it.
pub fn open_default() -> Result<Catalog, CatalogError> {
    Catalog::open(&Catalog::locate()?)
}

/// Decide whether a command that writes may go to `entry`.
///
/// A server that is not `protected` takes it. A protected one needs `--yes`,
/// or its own name typed at a terminal: any other answer, and the absence of a
/// way to ask, refuse by name.
pub fn confirm_write(
    entry: &str,
    protected: bool,
    command: &str,
    confirmation: &Confirmation,
) -> Result<(), CatalogError> {
    let refusal = || CatalogError::Protected {
        entry: entry.to_string(),
        command: command.to_string(),
    };
    if !protected {
        return Ok(());
    }
    match confirmation {
        Confirmation::Yes => Ok(()),
        Confirmation::Typed(text) if text.trim() == entry => Ok(()),
        _ => Err(refusal()),
    }
}

/// The path `catalog init` proposes: `<data folder>/servers`.
pub fn default_folder() -> Option<PathBuf> {
    Catalog::default_dir()
}

/// The error for a first word that is neither a command nor a server, with the
/// commands and servers that look like it. `commands` is read from the parser.
pub fn unknown_word(
    word: &str,
    commands: &[String],
    catalog: &Result<Catalog, CatalogError>,
) -> CatalogError {
    let mut candidates: Vec<String> = commands.to_vec();
    if let Ok(catalog) = catalog {
        candidates.extend(catalog.words());
    }
    CatalogError::UnknownWord {
        word: word.to_string(),
        near: util::near(word, candidates),
    }
}
