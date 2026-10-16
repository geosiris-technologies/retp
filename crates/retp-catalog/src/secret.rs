//! Secrets: what a catalogue entry points at, and the value once read.
//!
//! An entry never holds a secret, only a [`SecretRef`]. The value exists only
//! as a [`Secret`], from the moment a [`SecretSource`] reads it to the moment
//! the login is built.

use std::fmt;
use std::path::PathBuf;

use crate::error::CatalogError;

/// What replaces a secret that turned up in a text meant to be shown.
pub(crate) const REDACTED: &str = "[redacted]";

/// A secret value, in memory.
///
/// No `Display`, no `Clone`, and a `Debug` that shows nothing: a value that
/// reaches a log through `{:?}` is a leaked value (the rule of
/// `retp_proto::credentials::Credentials`). Its memory is overwritten when it
/// is dropped; see the limits in [`Secret::new`].
pub struct Secret(Vec<u8>);

impl Secret {
    /// A secret holding `value`.
    ///
    /// **Limits.** The wipe on drop is written in safe Rust, with
    /// `std::hint::black_box` against the optimiser removing it: a best effort,
    /// not the guarantee of a volatile write (the `zeroize` crate gives that,
    /// and is not a dependency here). It cannot reach copies made elsewhere:
    /// the `String`s of `Choice` and `Login` in `retp-client`, the buffers of
    /// the operating system.
    pub fn new(value: String) -> Self {
        Secret(value.into_bytes())
    }

    /// The value. The catalogue calls this at one place only (`resolve.rs`),
    /// where the login is built; a test counts the call sites in the source.
    pub fn expose(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or("")
    }

    /// Whether the value is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// `text` with every literal occurrence of this value replaced.
    pub(crate) fn redact_in(&self, text: &str) -> String {
        if self.0.is_empty() {
            return text.to_string();
        }
        text.replace(self.expose(), REDACTED)
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(…)")
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        for byte in self.0.iter_mut() {
            *byte = 0;
        }
        std::hint::black_box(&self.0);
    }
}

/// Where a secret is read from. Written in `server.toml` as `{ env = "NAME" }`
/// or `{ file = "PATH" }`, and on a command line as `env:NAME` or `file:PATH`.
///
/// The other sources the design names (the OS keyring, a command, a prompt)
/// are refused by name until they exist.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SecretRef {
    /// An environment variable.
    Env(String),
    /// A file; `~` and `${HOME}` at the start are expanded.
    File(String),
}

pub(crate) fn not_yet(source: &str) -> String {
    format!("the `{source}` secret source is not available yet in this version of retp; use `env` or `file`")
}

impl SecretRef {
    /// A reference to an environment variable, whose name is checked.
    pub(crate) fn env_named(name: &str) -> Result<Self, String> {
        let mut chars = name.chars();
        let valid = chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
        if valid {
            Ok(SecretRef::Env(name.to_string()))
        } else {
            Err("an environment variable name is letters, digits and `_`, not starting with a digit (the text given is not shown)".into())
        }
    }

    /// A reference to a file.
    pub(crate) fn file_named(path: &str) -> Result<Self, String> {
        if path.is_empty() {
            Err("a file reference needs a path".into())
        } else {
            Ok(SecretRef::File(path.to_string()))
        }
    }

    /// What a command line says: `env:NAME` or `file:PATH`.
    ///
    /// A text that is neither is refused **without being repeated**: it may be
    /// the secret itself, typed where a reference was asked for.
    pub fn parse(text: &str) -> Result<Self, CatalogError> {
        let refused = |why: String| CatalogError::Reference { why };
        if text == "prompt" {
            return Err(refused(not_yet("prompt")));
        }
        let Some((source, rest)) = text.split_once(':') else {
            return Err(refused(
                "a secret reference looks like `env:NAME` or `file:PATH` (the text given is not shown)"
                    .into(),
            ));
        };
        match source {
            "env" => Self::env_named(rest).map_err(refused),
            "file" => Self::file_named(rest).map_err(refused),
            "keyring" | "cmd" => Err(refused(not_yet(source))),
            _ => Err(refused(
                "a secret reference looks like `env:NAME` or `file:PATH` (the text given is not shown)"
                    .into(),
            )),
        }
    }

    /// The command-line form, `env:NAME` or `file:PATH`.
    pub fn to_text(&self) -> String {
        match self {
            SecretRef::Env(name) => format!("env:{name}"),
            SecretRef::File(path) => format!("file:{path}"),
        }
    }

    /// The `server.toml` form, `{ env = "NAME" }` or `{ file = "PATH" }`.
    pub(crate) fn to_toml(&self) -> String {
        match self {
            SecretRef::Env(name) => format!("{{ env = {} }}", crate::util::quote(name)),
            SecretRef::File(path) => format!("{{ file = {} }}", crate::util::quote(path)),
        }
    }
}

/// Why a reference could not be turned into a value. None of these holds a value.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SecretError {
    /// The environment variable is not defined, or empty.
    EnvMissing {
        /// Its name.
        variable: String,
    },
    /// The file does not exist.
    FileMissing {
        /// The path as the reference spells it.
        path: String,
    },
    /// The file exists and cannot be read as a secret.
    FileUnreadable {
        /// The path as the reference spells it.
        path: String,
        /// Why, in words.
        why: String,
    },
    /// The file can be read by others (Unix: any permission beyond `0600`).
    FileTooOpen {
        /// The path as the reference spells it.
        path: String,
        /// Its mode, in octal.
        mode: String,
    },
    /// The source gave an empty value.
    Empty {
        /// The reference, in its command-line form.
        reference: String,
    },
    /// The source does not exist yet, or not here.
    Unavailable {
        /// Why, in words.
        reason: String,
    },
}

impl fmt::Display for SecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecretError::EnvMissing { variable } => {
                write!(f, "the variable {variable} is not defined (or is empty)")
            }
            SecretError::FileMissing { path } => write!(f, "the file {path} does not exist"),
            SecretError::FileUnreadable { path, why } => write!(f, "the file {path} cannot be read: {why}"),
            SecretError::FileTooOpen { path, mode } => {
                write!(
                    f,
                    "the file {path} is readable by others: mode {mode}, expected 0600"
                )
            }
            SecretError::Empty { reference } => write!(f, "{reference} is empty"),
            SecretError::Unavailable { reason } => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for SecretError {}

/// What can be said of a reference without showing its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefStatus {
    /// Whether reading it would give a value.
    pub resolves: bool,
    /// Why, in words: `variable is set`, `mode 0644, expected 0600`…
    pub detail: String,
}

/// A place secrets are read from. A registry rather than a `match` on the
/// reference, so that a test brings its own source and a later source (the
/// keyring) is one more implementation.
pub trait SecretSource {
    /// Read the value `reference` points at.
    fn read(&self, reference: &SecretRef) -> Result<Secret, SecretError>;

    /// Say whether `reference` resolves, without returning its value.
    fn inspect(&self, reference: &SecretRef) -> RefStatus;
}

/// How the environment is read: a variable's value by its name.
type ReadVariable = Box<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// The operating system's sources: environment variables and files.
pub struct OsSecrets {
    env: ReadVariable,
}

impl Default for OsSecrets {
    fn default() -> Self {
        Self::new()
    }
}

impl OsSecrets {
    /// Reads the process environment.
    pub fn new() -> Self {
        Self::with_env(|name| std::env::var(name).ok())
    }

    /// Reads variables through `env` instead of the process environment (a
    /// test sets no variable of the real process, which would be shared by
    /// every test thread).
    pub fn with_env(env: impl Fn(&str) -> Option<String> + Send + Sync + 'static) -> Self {
        Self { env: Box::new(env) }
    }

    fn variable(&self, name: &str) -> Option<String> {
        (self.env)(name).filter(|value| !value.is_empty())
    }

    fn home(&self) -> Option<PathBuf> {
        self.variable("HOME")
            .or_else(|| self.variable("USERPROFILE"))
            .map(PathBuf::from)
            .or_else(|| directories::BaseDirs::new().map(|dirs| dirs.home_dir().to_path_buf()))
    }

    /// The path a file reference names: `~` and `${HOME}` at the start become the home folder.
    fn path_of(&self, text: &str) -> PathBuf {
        let tail = text.strip_prefix("${HOME}").or_else(|| text.strip_prefix('~'));
        match (tail, self.home()) {
            (Some(tail), Some(home))
                if tail.is_empty() || tail.starts_with('/') || tail.starts_with('\\') =>
            {
                home.join(tail.trim_start_matches(['/', '\\']))
            }
            _ => PathBuf::from(text),
        }
    }

    fn read_file(&self, text: &str) -> Result<Secret, SecretError> {
        let path = self.path_of(text);
        let shown = text.to_string();
        let metadata = std::fs::metadata(&path).map_err(|error| file_error(&shown, &error))?;
        if !metadata.is_file() {
            return Err(SecretError::FileUnreadable {
                path: shown,
                why: "it is not a regular file".into(),
            });
        }
        if let Some(mode) = too_open(&metadata) {
            return Err(SecretError::FileTooOpen { path: shown, mode });
        }
        let bytes = std::fs::read(&path).map_err(|error| file_error(&shown, &error))?;
        // The error of `from_utf8` would hold the bytes: it is replaced, not passed on.
        let mut value = String::from_utf8(bytes).map_err(|_| SecretError::FileUnreadable {
            path: shown.clone(),
            why: "it is not valid UTF-8".into(),
        })?;
        // One line ending, the one `echo secret > file` leaves; a second one is part of the value.
        if value.ends_with('\n') {
            value.pop();
            if value.ends_with('\r') {
                value.pop();
            }
        }
        if value.is_empty() {
            return Err(SecretError::Empty {
                reference: format!("file:{shown}"),
            });
        }
        Ok(Secret::new(value))
    }
}

fn file_error(path: &str, error: &std::io::Error) -> SecretError {
    if error.kind() == std::io::ErrorKind::NotFound {
        SecretError::FileMissing {
            path: path.to_string(),
        }
    } else {
        SecretError::FileUnreadable {
            path: path.to_string(),
            why: error.kind().to_string(),
        }
    }
}

/// The mode of a file other users can read, in octal; `None` when it is private
/// - and always `None` off Unix, where the check is not made (TODO(verify): the
/// Windows ACL check the design leaves open).
#[cfg(unix)]
fn too_open(metadata: &std::fs::Metadata) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let mode = metadata.permissions().mode() & 0o777;
    (mode & 0o077 != 0).then(|| format!("{mode:04o}"))
}

#[cfg(not(unix))]
fn too_open(_metadata: &std::fs::Metadata) -> Option<String> {
    None
}

impl SecretSource for OsSecrets {
    fn read(&self, reference: &SecretRef) -> Result<Secret, SecretError> {
        match reference {
            SecretRef::Env(name) => {
                tracing::debug!(reference = %format!("env:{name}"), "catalog: reading the variable {name}");
                self.variable(name)
                    .map(Secret::new)
                    .ok_or_else(|| SecretError::EnvMissing {
                        variable: name.clone(),
                    })
            }
            SecretRef::File(path) => {
                tracing::debug!(reference = %format!("file:{path}"), "catalog: reading the file {path}");
                self.read_file(path)
            }
        }
    }

    fn inspect(&self, reference: &SecretRef) -> RefStatus {
        match reference {
            SecretRef::Env(name) => {
                let set = self.variable(name).is_some();
                RefStatus {
                    resolves: set,
                    detail: if set {
                        "variable is set".into()
                    } else {
                        "variable is not defined, or empty".into()
                    },
                }
            }
            SecretRef::File(text) => {
                let path = self.path_of(text);
                let Ok(metadata) = std::fs::metadata(&path) else {
                    return RefStatus {
                        resolves: false,
                        detail: "file not found".into(),
                    };
                };
                if !metadata.is_file() {
                    return RefStatus {
                        resolves: false,
                        detail: "not a regular file".into(),
                    };
                }
                let link = std::fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink());
                let suffix = if link { ", symbolic link followed" } else { "" };
                match too_open(&metadata) {
                    Some(mode) => RefStatus {
                        resolves: false,
                        detail: format!("mode {mode}, expected 0600{suffix}"),
                    },
                    None => RefStatus {
                        resolves: true,
                        detail: format!("file exists{suffix}"),
                    },
                }
            }
        }
    }
}
