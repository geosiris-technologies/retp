//! The catalogue: a configuration file that names a folder, and in the folder
//! one sub-folder per server.
//!
//! ```text
//! <catalog_dir>/
//!   azure-test/
//!     server.toml     the definition, no secret in it: it can be shared
//!     state/          local: history of the shell, the record of `server test`
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use directories::ProjectDirs;
use serde::Deserialize;

use crate::entry::{Entry, Problem, read_toml};
use crate::error::CatalogError;
use crate::secret::SecretSource;
use crate::util::{check_name, near, quote};

/// The names `retp` itself answers to: commands and their aliases. A server
/// cannot take one, or `retp NAME` would run the command instead.
#[derive(Debug, Clone, Default)]
pub struct Reserved(BTreeSet<String>);

impl Reserved {
    /// The reserved names. The command line reads them from its parser, so
    /// that a command added tomorrow is reserved tomorrow.
    pub fn new<I: IntoIterator<Item = String>>(names: I) -> Self {
        Reserved(names.into_iter().collect())
    }

    /// Whether `name` is reserved.
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains(name)
    }

    /// Every reserved name.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}

/// Where the configuration file is, and the folder an environment override names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    config_file: PathBuf,
    catalog_dir: Option<PathBuf>,
}

impl Location {
    /// A location from a configuration file and, optionally, a catalogue folder that takes precedence.
    pub fn new(config_file: PathBuf, catalog_dir: Option<PathBuf>) -> Self {
        Location {
            config_file,
            catalog_dir,
        }
    }

    /// The configuration file.
    pub fn config_file(&self) -> &Path {
        &self.config_file
    }

    /// The folder `RETP_CATALOG` named, if it did.
    pub fn catalog_dir(&self) -> Option<&Path> {
        self.catalog_dir.as_deref()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    schema: Option<u32>,
    catalog_dir: Option<String>,
    default_server: Option<String>,
}

/// An entry found by name or alias.
#[derive(Debug, Clone, Copy)]
pub struct Found<'a> {
    /// The server's name (never the alias that found it).
    pub name: &'a str,
    /// Its definition.
    pub entry: &'a Entry,
}

/// What `server list` shows of a server: nothing secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    /// The name.
    pub name: String,
    /// Its aliases.
    pub aliases: Vec<String>,
    /// The URL of its default transport.
    pub url: String,
    /// The login method, or `unreadable`.
    pub method: String,
    /// Whether writes ask for a confirmation.
    pub protected: bool,
    /// Whether it is the default server.
    pub is_default: bool,
    /// How many references there are and how many do not resolve.
    pub references: (usize, usize),
    /// Why the definition cannot be used, when it cannot.
    pub problem: Option<String>,
}

/// The catalogue, read.
pub struct Catalog {
    location: Location,
    root: PathBuf,
    default_server: Option<String>,
    entries: BTreeMap<String, Entry>,
    broken: BTreeMap<String, String>,
    aliases: BTreeMap<String, String>,
    warnings: Vec<String>,
}

fn config_error(file: &Path, why: impl Into<String>) -> CatalogError {
    CatalogError::Config {
        file: file.to_path_buf(),
        why: why.into(),
    }
}

impl Catalog {
    /// Where the configuration file is: `RETP_CONFIG`, else the OS's
    /// configuration folder. `RETP_CATALOG` names the catalogue folder and
    /// takes precedence over the configuration.
    pub fn locate() -> Result<Location, CatalogError> {
        Self::locate_with(&|name| std::env::var(name).ok())
    }

    /// [`Catalog::locate`], reading variables through `env`.
    pub fn locate_with(env: &dyn Fn(&str) -> Option<String>) -> Result<Location, CatalogError> {
        let catalog_dir = env("RETP_CATALOG").filter(|v| !v.is_empty()).map(PathBuf::from);
        let config_file = match env("RETP_CONFIG").filter(|v| !v.is_empty()) {
            Some(file) => PathBuf::from(file),
            None => ProjectDirs::from("", "", "retp")
                .ok_or_else(|| {
                    config_error(Path::new(""), "no home folder to look for the configuration in")
                })?
                .config_dir()
                .join("config.toml"),
        };
        Ok(Location::new(config_file, catalog_dir))
    }

    /// The folder `catalog init` proposes: `<data folder>/servers`.
    pub fn default_dir() -> Option<PathBuf> {
        ProjectDirs::from("", "", "retp").map(|dirs| dirs.data_dir().join("servers"))
    }

    /// Choose the catalogue folder: create it, write the configuration file, open the catalogue.
    ///
    /// Refused when a configuration file exists, unless `force`.
    pub fn init(location: &Location, dir: &Path, force: bool) -> Result<Catalog, CatalogError> {
        let file = location.config_file();
        if file.exists() && !force {
            return Err(config_error(
                file,
                "it exists already; pass --force to point it at another folder",
            ));
        }
        let dir = std::path::absolute(dir)
            .map_err(|e| CatalogError::io(format!("resolving the folder {}", dir.display()), &e))?;
        let text = dir
            .to_str()
            .ok_or_else(|| config_error(file, "the folder's path is not valid UTF-8"))?;
        std::fs::create_dir_all(&dir)
            .map_err(|e| CatalogError::io(format!("creating {}", dir.display()), &e))?;
        if let Some(parent) = file.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)
                .map_err(|e| CatalogError::io(format!("creating {}", parent.display()), &e))?;
        }
        let config = format!("schema = 1\ncatalog_dir = {}\n", quote(text));
        std::fs::write(file, config)
            .map_err(|e| CatalogError::io(format!("writing {}", file.display()), &e))?;
        // What is local to a machine stays out of a shared folder.
        let ignore = dir.join(".gitignore");
        if !ignore.exists() {
            let _ = std::fs::write(&ignore, "*/state/\n");
        }
        Catalog::open(&Location::new(file.to_path_buf(), None))
    }

    /// Read the catalogue. A server whose `server.toml` is wrong does not stop
    /// the others: it is listed as unusable, and refused by name when asked for.
    pub fn open(location: &Location) -> Result<Catalog, CatalogError> {
        let file = location.config_file();
        let config = match std::fs::read_to_string(file) {
            Ok(text) => {
                let parsed: FileConfig = read_toml(&text).map_err(|(line, why)| match line {
                    Some(line) => config_error(file, format!("line {line}: {why}")),
                    None => config_error(file, why),
                })?;
                if parsed.schema != Some(1) {
                    return Err(config_error(file, "`schema` is required and must be 1"));
                }
                Some(parsed)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(CatalogError::io(format!("reading {}", file.display()), &e)),
        };
        let (root, default_server) = match (location.catalog_dir(), config) {
            (Some(dir), config) => (dir.to_path_buf(), config.and_then(|c| c.default_server)),
            (None, Some(config)) => {
                let dir = config
                    .catalog_dir
                    .ok_or_else(|| config_error(file, "`catalog_dir` is missing"))?;
                (PathBuf::from(dir), config.default_server)
            }
            (None, None) => {
                return Err(CatalogError::NotInitialised {
                    config_file: file.to_path_buf(),
                });
            }
        };
        let mut catalog = Catalog {
            location: location.clone(),
            root,
            default_server,
            entries: BTreeMap::new(),
            broken: BTreeMap::new(),
            aliases: BTreeMap::new(),
            warnings: Vec::new(),
        };
        catalog.load()?;
        Ok(catalog)
    }

    fn load(&mut self) -> Result<(), CatalogError> {
        let read = std::fs::read_dir(&self.root).map_err(|e| {
            CatalogError::io(
                format!("reading the catalogue folder {}", self.root.display()),
                &e,
            )
        })?;
        for item in read.flatten() {
            let path = item.path();
            let file = path.join("server.toml");
            if !path.is_dir() || !file.is_file() {
                continue;
            }
            let Some(name) = item.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if let Err(why) = check_name(&name) {
                self.broken
                    .insert(name, format!("not a valid server name: {why}"));
                continue;
            }
            match std::fs::read_to_string(&file) {
                Err(e) => {
                    self.broken
                        .insert(name, format!("server.toml cannot be read: {}", e.kind()));
                }
                Ok(text) => match Entry::parse(&text) {
                    Ok(entry) => {
                        self.entries.insert(name, entry);
                    }
                    Err(error) => {
                        self.broken.insert(name, error.to_string());
                    }
                },
            }
        }
        let taken: BTreeSet<String> = self.entries.keys().chain(self.broken.keys()).cloned().collect();
        let mut aliases = BTreeMap::new();
        for (name, entry) in &self.entries {
            for alias in &entry.aliases {
                if taken.contains(alias) || aliases.contains_key(alias) {
                    self.warnings.push(format!(
                        "the alias `{alias}` of `{name}` is already taken and is ignored"
                    ));
                } else {
                    aliases.insert(alias.clone(), name.clone());
                }
            }
        }
        self.aliases = aliases;
        Ok(())
    }

    /// What went wrong without stopping the reading (an alias that two servers claim).
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// The catalogue folder.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn all_words(&self) -> Vec<String> {
        self.entries
            .keys()
            .chain(self.broken.keys())
            .chain(self.aliases.keys())
            .cloned()
            .collect()
    }

    /// Every name and alias: what `retp NAME` can mean here.
    pub fn words(&self) -> Vec<String> {
        self.all_words()
    }

    /// The server a name or an alias stands for.
    pub fn find(&self, word: &str) -> Result<Found<'_>, CatalogError> {
        let name = if self.entries.contains_key(word) || self.broken.contains_key(word) {
            Some(word.to_string())
        } else {
            self.aliases.get(word).cloned()
        };
        match name {
            Some(name) => match self.entries.get_key_value(&name) {
                Some((name, entry)) => Ok(Found {
                    name: name.as_str(),
                    entry,
                }),
                None => Err(CatalogError::Broken {
                    why: self.broken.get(&name).cloned().unwrap_or_default(),
                    name,
                }),
            },
            None => Err(CatalogError::Unknown {
                word: word.to_string(),
                near: near(word, self.all_words()),
            }),
        }
    }

    /// Whether `word` is a name or an alias.
    pub fn knows(&self, word: &str) -> bool {
        self.entries.contains_key(word) || self.broken.contains_key(word) || self.aliases.contains_key(word)
    }

    /// The default server, when one is set and still exists.
    pub fn default_entry(&self) -> Option<Found<'_>> {
        self.default_server
            .as_deref()
            .and_then(|name| self.find(name).ok())
    }

    /// The servers, with what can be said of them without showing a secret.
    pub fn listed(&self, secrets: &dyn SecretSource) -> Vec<Listed> {
        let default = self.default_entry().map(|found| found.name.to_string());
        let mut rows = Vec::new();
        for (name, entry) in &self.entries {
            let (_, refs) = entry.auth.parts();
            let unresolved = refs.iter().filter(|(_, r)| !secrets.inspect(r).resolves).count();
            rows.push(Listed {
                name: name.clone(),
                aliases: entry.aliases.clone(),
                url: entry.server.selected_url().unwrap_or("").to_string(),
                method: entry.auth.method().to_string(),
                protected: entry.server.protected,
                is_default: default.as_deref() == Some(name.as_str()),
                references: (refs.len(), unresolved),
                problem: None,
            });
        }
        for (name, why) in &self.broken {
            rows.push(Listed {
                name: name.clone(),
                aliases: Vec::new(),
                url: String::new(),
                method: "unreadable".into(),
                protected: false,
                is_default: false,
                references: (0, 0),
                problem: Some(why.clone()),
            });
        }
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        rows
    }

    /// Whether `name` and `aliases` can be given to a new server (or, with
    /// `force`, to the server `name` already is).
    pub fn check_new(
        &self,
        name: &str,
        aliases: &[String],
        force: bool,
        reserved: &Reserved,
    ) -> Result<(), CatalogError> {
        for word in std::iter::once(name).chain(aliases.iter().map(String::as_str)) {
            check_name(word).map_err(|why| CatalogError::Name {
                name: word.to_string(),
                why: why.to_string(),
            })?;
            if reserved.contains(word) {
                return Err(CatalogError::Reserved {
                    name: word.to_string(),
                });
            }
        }
        if !force && (self.entries.contains_key(name) || self.broken.contains_key(name)) {
            return Err(CatalogError::Exists {
                name: name.to_string(),
            });
        }
        for word in std::iter::once(name).chain(aliases.iter().map(String::as_str)) {
            let owner = if self.entries.contains_key(word) || self.broken.contains_key(word) {
                Some(word.to_string())
            } else {
                self.aliases.get(word).cloned()
            };
            if let Some(owner) = owner
                && owner != name
            {
                return Err(CatalogError::Taken {
                    word: word.to_string(),
                    by: owner,
                });
            }
        }
        Ok(())
    }

    /// Add a server: its folder and its `server.toml`.
    pub fn add(
        &mut self,
        name: &str,
        entry: Entry,
        force: bool,
        reserved: &Reserved,
    ) -> Result<(), CatalogError> {
        self.check_new(name, &entry.aliases, force, reserved)?;
        entry
            .validate()
            .map_err(|problem: Problem| problem.located(None))?;
        let dir = self.root.join(name);
        std::fs::create_dir_all(&dir)
            .map_err(|e| CatalogError::io(format!("creating {}", dir.display()), &e))?;
        let file = dir.join("server.toml");
        std::fs::write(&file, entry.to_toml())
            .map_err(|e| CatalogError::io(format!("writing {}", file.display()), &e))?;
        self.aliases.retain(|_, owner| owner.as_str() != name);
        self.broken.remove(name);
        for alias in &entry.aliases {
            self.aliases.insert(alias.clone(), name.to_string());
        }
        self.entries.insert(name.to_string(), entry);
        Ok(())
    }

    /// Remove a server: its `server.toml` and its `state/`. Its folder goes
    /// too, unless something else is in it, which is then left and said.
    pub fn remove(&mut self, word: &str) -> Result<(), CatalogError> {
        let name = match self.find(word) {
            Ok(found) => found.name.to_string(),
            Err(CatalogError::Broken { name, .. }) => name,
            Err(other) => return Err(other),
        };
        let dir = self.root.join(&name);
        let file = dir.join("server.toml");
        std::fs::remove_file(&file)
            .map_err(|e| CatalogError::io(format!("removing {}", file.display()), &e))?;
        let state = dir.join("state");
        if state.is_dir() {
            std::fs::remove_dir_all(&state)
                .map_err(|e| CatalogError::io(format!("removing {}", state.display()), &e))?;
        }
        if let Err(e) = std::fs::remove_dir(&dir) {
            return Err(CatalogError::io(
                format!(
                    "the server is removed, but {} holds other files and stays",
                    dir.display()
                ),
                &e,
            ));
        }
        self.entries.remove(&name);
        self.broken.remove(&name);
        self.aliases.retain(|_, owner| *owner != name);
        if self.default_server.as_deref() == Some(name.as_str()) {
            self.default_server = None;
            self.write_config()?;
        }
        Ok(())
    }

    /// Make a server the default one.
    pub fn set_default(&mut self, word: &str) -> Result<(), CatalogError> {
        let name = self.find(word)?.name.to_string();
        self.default_server = Some(name);
        self.write_config()
    }

    fn write_config(&self) -> Result<(), CatalogError> {
        let file = self.location.config_file();
        let dir = self
            .root
            .to_str()
            .ok_or_else(|| config_error(file, "the folder's path is not valid UTF-8"))?;
        let mut text = format!("schema = 1\ncatalog_dir = {}\n", quote(dir));
        if let Some(name) = &self.default_server {
            text.push_str(&format!("default_server = {}\n", quote(name)));
        }
        if let Some(parent) = file.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)
                .map_err(|e| CatalogError::io(format!("creating {}", parent.display()), &e))?;
        }
        std::fs::write(file, text).map_err(|e| CatalogError::io(format!("writing {}", file.display()), &e))
    }

    /// The local `state/` folder of a server, created if needed.
    pub fn state_dir(&self, word: &str) -> Result<PathBuf, CatalogError> {
        let name = match self.find(word) {
            Ok(found) => found.name.to_string(),
            Err(CatalogError::Broken { name, .. }) => name,
            Err(other) => return Err(other),
        };
        let dir = self.root.join(name).join("state");
        std::fs::create_dir_all(&dir)
            .map_err(|e| CatalogError::io(format!("creating {}", dir.display()), &e))?;
        Ok(dir)
    }

    /// Where the shell of this server keeps its history.
    pub fn history_path(&self, word: &str) -> Option<PathBuf> {
        self.state_dir(word).ok().map(|dir| dir.join("history"))
    }
}
