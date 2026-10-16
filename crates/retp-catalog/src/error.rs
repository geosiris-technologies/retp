//! What can go wrong, in words that name a key, a line, a reference or a
//! server and **never a secret value**: no variant holds one.

use std::fmt;
use std::path::PathBuf;

use crate::secret::SecretError;

/// Why the catalogue refused or failed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CatalogError {
    /// No configuration file and no `RETP_CATALOG`: nothing chose a folder yet.
    NotInitialised {
        /// Where the configuration file was looked for.
        config_file: PathBuf,
    },
    /// The configuration file, or the place it lives, is wrong.
    Config {
        /// The file.
        file: PathBuf,
        /// Why, in words.
        why: String,
    },
    /// A file or folder could not be read or written.
    Io {
        /// What was being done, and to what.
        what: String,
        /// The kind of failure, in words.
        why: String,
    },
    /// Not a name a server can have.
    Name {
        /// The name given.
        name: String,
        /// The rule it breaks.
        why: String,
    },
    /// A name that is a command of `retp`: `retp NAME` would never reach the server.
    Reserved {
        /// The name given.
        name: String,
    },
    /// A name or an alias that another server already has.
    Taken {
        /// The name or alias.
        word: String,
        /// The server that has it.
        by: String,
    },
    /// A server of that name exists, and was not to be replaced.
    Exists {
        /// Its name.
        name: String,
    },
    /// No server by that name or alias.
    Unknown {
        /// The word looked up.
        word: String,
        /// Names and aliases that look like it.
        near: Vec<String>,
    },
    /// A first word that is neither a command nor a server.
    UnknownWord {
        /// The word.
        word: String,
        /// Commands, names and aliases that look like it.
        near: Vec<String>,
    },
    /// A server whose `server.toml` cannot be used.
    Broken {
        /// Its name.
        name: String,
        /// Why, in words.
        why: String,
    },
    /// A fault in a definition (`server.toml`, or what `server add` built).
    Entry {
        /// The key at fault, when there is one.
        key: Option<String>,
        /// Its line, when it is known.
        line: Option<usize>,
        /// Why, in words, never quoting a value.
        why: String,
    },
    /// A secret reference that is not one.
    Reference {
        /// Why, in words; the text given is not repeated.
        why: String,
    },
    /// A reference that did not give a value.
    Secret {
        /// The server.
        entry: String,
        /// The field that points at the secret.
        field: String,
        /// Why.
        error: SecretError,
    },
    /// The login failed. Literal occurrences of the secrets are removed from `message`.
    Login {
        /// The server.
        entry: String,
        /// What the client said.
        message: String,
    },
    /// The method an entry declares is not the one the client deduces from its fields.
    Inconsistent {
        /// The server.
        entry: String,
        /// The declared method.
        declared: String,
        /// The deduced one.
        inferred: String,
    },
    /// The entry has no URL for the transport asked for.
    NoUrl {
        /// The server.
        entry: String,
        /// `ETP` or `REST`.
        wanted: String,
    },
    /// A URL that is not the entry's: its credentials go to its own URLs only.
    ForeignUrl {
        /// The server.
        entry: String,
    },
    /// A request the catalogue refuses by rule.
    Refused {
        /// Which rule, in words.
        why: String,
    },
    /// A write to a protected server that nobody confirmed.
    Protected {
        /// The server.
        entry: String,
        /// The command.
        command: String,
    },
}

impl CatalogError {
    pub(crate) fn io(what: impl Into<String>, error: &std::io::Error) -> Self {
        CatalogError::Io {
            what: what.into(),
            why: error.kind().to_string(),
        }
    }

    pub(crate) fn refused(why: impl Into<String>) -> Self {
        CatalogError::Refused { why: why.into() }
    }

    pub(crate) fn entry(key: Option<&str>, line: Option<usize>, why: impl Into<String>) -> Self {
        CatalogError::Entry {
            key: key.map(str::to_string),
            line,
            why: why.into(),
        }
    }
}

fn closest(near: &[String]) -> String {
    if near.is_empty() {
        String::new()
    } else {
        format!("; closest: {}", near.join(", "))
    }
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogError::NotInitialised { config_file } => write!(
                f,
                "there is no server catalogue yet: run `retp catalog init` to choose its folder (the configuration file would be {})",
                config_file.display()
            ),
            CatalogError::Config { file, why } => write!(f, "configuration file {}: {why}", file.display()),
            CatalogError::Io { what, why } => write!(f, "{what}: {why}"),
            CatalogError::Name { name, why } => write!(f, "`{name}` is not a valid server name: {why}"),
            CatalogError::Reserved { name } => write!(
                f,
                "`{name}` is a command of retp: a server of that name could not be reached as `retp {name}`"
            ),
            CatalogError::Taken { word, by } => write!(f, "`{word}` is already used by the server `{by}`"),
            CatalogError::Exists { name } => {
                write!(
                    f,
                    "the server `{name}` already exists (pass --force to replace it)"
                )
            }
            CatalogError::Unknown { word, near } => {
                write!(f, "no server named `{word}` in the catalogue{}", closest(near))
            }
            CatalogError::UnknownWord { word, near } => {
                write!(f, "no server and no command named `{word}`{}", closest(near))
            }
            CatalogError::Broken { name, why } => write!(f, "the server `{name}` cannot be used: {why}"),
            CatalogError::Entry { key, line, why } => match (key, line) {
                (Some(key), Some(line)) => write!(f, "key `{key}`, line {line}: {why}"),
                (Some(key), None) => write!(f, "key `{key}`: {why}"),
                (None, Some(line)) => write!(f, "line {line}: {why}"),
                (None, None) => write!(f, "{why}"),
            },
            CatalogError::Reference { why } => write!(f, "{why}"),
            CatalogError::Secret { entry, field, error } => {
                write!(f, "server `{entry}`, `{field}`: {error}")
            }
            CatalogError::Login { entry, message } => write!(f, "logging in to `{entry}`: {message}"),
            CatalogError::Inconsistent {
                entry,
                declared,
                inferred,
            } => write!(
                f,
                "server `{entry}` declares the method `{declared}`, but its fields give `{inferred}`: this is a bug in retp"
            ),
            CatalogError::NoUrl { entry, wanted } => {
                write!(f, "server `{entry}` has no {wanted} URL")
            }
            CatalogError::ForeignUrl { entry } => write!(
                f,
                "the URL given is not one of server `{entry}`: the credentials of a catalogue entry go to its own URLs only"
            ),
            CatalogError::Refused { why } => write!(f, "{why}"),
            CatalogError::Protected { entry, command } => write!(
                f,
                "server `{entry}` is protected and `{command}` writes to it: run it from a terminal and type the server's name to confirm, or pass --yes"
            ),
        }
    }
}

impl std::error::Error for CatalogError {}
