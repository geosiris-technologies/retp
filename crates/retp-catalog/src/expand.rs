//! Turning `retp azure dataspaces`, `retp @azure …`, `retp --server azure …`
//! (and a default server) into the command line `retp` already parses.
//!
//! The rewrite happens **before** the argument parser: the catalogue server's
//! URL is inserted as the first positional of the command, followed by a
//! hidden `--catalog-entry NAME` and by the options of the entry that the
//! command line did not give (the line wins). No secret passes through the
//! argument vector; the secrets are read later, at connection time.
//!
//! The words that are commands are not known here: the caller passes them
//! ([`Grammar`]), read from its parser. A built-in command always wins over a
//! server of the same name, and `@name` always means the server.

use std::ffi::OsString;

use crate::catalog::{Catalog, Found, Reserved};
use crate::entry::Transport;
use crate::error::CatalogError;
use crate::util::{is_url, near};

/// The login options of the command line. Given together with a catalogue
/// server they are refused: the entry is the only source of its login.
pub const LOGIN_OPTIONS: [&str; 9] = [
    "--token",
    "--user",
    "--password",
    "--token-url",
    "--azure-tenant",
    "--cognito-region",
    "--client-id",
    "--client-secret",
    "--oauth-scope",
];

/// The options of an entry a program accepts.
#[derive(Debug, Clone, Copy)]
pub struct Accepts {
    /// `--as-browser`.
    pub as_browser: bool,
    /// `--keepalive`.
    pub keepalive: bool,
    /// `--at-once`.
    pub at_once: bool,
    /// `--type`.
    pub types: bool,
    /// `--offer`.
    pub offer: bool,
}

/// What the program parses, as far as the rewrite needs to know.
#[derive(Debug, Clone)]
pub struct Grammar {
    /// `true` for `retp` (commands), `false` for a program with one command
    /// (`retp-conformance`: the URL is its first positional).
    pub subcommands: bool,
    /// Every command and alias, read from the parser (`help` included).
    pub commands: Reserved,
    /// The commands that take no server (`server`, `catalog`, `help`).
    pub without_server: Vec<String>,
    /// The commands that write: a default server is refused for them.
    pub writing: Vec<String>,
    /// The command opened when only a server is named.
    pub default_command: String,
    /// The command whose `--custom` entries receive the entry's write defaults.
    pub create_command: String,
    /// The options of an entry this program accepts.
    pub accepts: Accepts,
    /// Options that stop `--insecure` being added (`--tls-fallback`).
    pub excludes_insecure: Vec<String>,
}

/// The command line after the rewrite.
#[derive(Debug, Clone)]
pub struct Expanded {
    /// The arguments to parse, program name first.
    pub argv: Vec<OsString>,
    /// The catalogue server in use, if any.
    pub entry: Option<String>,
    /// The server was the default one: say so on stderr.
    pub via_default: bool,
}

fn refused(why: impl Into<String>) -> CatalogError {
    CatalogError::refused(why)
}

fn text(arg: &OsString) -> Option<&str> {
    arg.to_str()
}

/// Whether `args` hold `name` or `name=...`, before a `--`.
fn has(args: &[OsString], names: &[&str]) -> bool {
    args.iter()
        .map_while(|arg| text(arg).or(Some("")).filter(|t| *t != "--"))
        .any(|t| {
            names
                .iter()
                .any(|n| t == *n || t.strip_prefix(*n).is_some_and(|rest| rest.starts_with('=')))
        })
}

/// Whether `--custom KEY=...` is already given.
fn has_custom(args: &[OsString], key: &str) -> bool {
    let prefix = format!("{key}=");
    let direct = format!("--custom={prefix}");
    args.iter().enumerate().any(|(i, arg)| match text(arg) {
        Some("--custom") => args
            .get(i + 1)
            .and_then(text)
            .is_some_and(|value| value.starts_with(&prefix)),
        Some(t) => t.starts_with(&direct),
        None => false,
    })
}

/// Take the `--server NAME` option out of `args`: `--server`, `--serveur`,
/// `-s`, with a value or `=value`, and `-sNAME`.
fn take_server_option(args: &[OsString]) -> Result<(Option<String>, Vec<OsString>), CatalogError> {
    let mut named: Option<String> = None;
    let mut rest = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        let Some(t) = text(arg) else {
            rest.push(arg.clone());
            i += 1;
            continue;
        };
        if t == "--" {
            rest.extend(args[i..].iter().cloned());
            break;
        }
        let (value, consumed) = if matches!(t, "--server" | "--serveur" | "-s") {
            match args.get(i + 1).and_then(text).filter(|n| !n.starts_with('-')) {
                Some(name) => (name.to_string(), 2),
                None => return Err(refused("--server needs the name of a catalogue server")),
            }
        } else if let Some(value) = t
            .strip_prefix("--server=")
            .or_else(|| t.strip_prefix("--serveur="))
        {
            (value.to_string(), 1)
        } else if let Some(value) = t
            .strip_prefix("-s")
            .filter(|v| !v.is_empty() && !v.starts_with('-'))
        {
            (value.to_string(), 1)
        } else {
            rest.push(arg.clone());
            i += 1;
            continue;
        };
        if named.is_some() {
            return Err(refused("the server is named twice"));
        }
        named = Some(value.trim_start_matches('@').to_string());
        i += consumed;
    }
    Ok((named, rest))
}

/// The pieces inserted after the command: the URL, the entry, the options of the entry.
fn inserted(
    found: Found<'_>,
    grammar: &Grammar,
    command: &str,
    after: &[OsString],
) -> Result<Vec<OsString>, CatalogError> {
    let name = found.name;
    for arg in after
        .iter()
        .map_while(|a| text(a).or(Some("")).filter(|t| *t != "--"))
    {
        if let Some(option) = LOGIN_OPTIONS
            .iter()
            .find(|o| arg == **o || arg.strip_prefix(**o).is_some_and(|r| r.starts_with('=')))
        {
            return Err(refused(format!(
                "`{option}` and the server `{name}` do not go together: the catalogue entry is the only source of its login"
            )));
        }
        if is_url(arg) {
            return Err(refused(format!(
                "both the server `{name}` and a URL were given: use one of them"
            )));
        }
    }
    let entry = found.entry;
    let server = &entry.server;
    let rest = has(after, &["--rest"]) || server.transport == Transport::Rest;
    let (url, kind) = if rest {
        (&server.rest_url, "REST")
    } else {
        (&server.etp_url, "ETP")
    };
    let Some(url) = url else {
        return Err(CatalogError::NoUrl {
            entry: name.to_string(),
            wanted: kind.to_string(),
        });
    };
    let mut out: Vec<OsString> = vec![url.into(), "--catalog-entry".into(), name.into()];
    let mut add = |option: &str, value: Option<String>| {
        out.push(option.into());
        if let Some(value) = value {
            out.push(value.into());
        }
    };
    if rest && !has(after, &["--rest"]) {
        add("--rest", None);
    }
    if let Some(partition) = &server.partition
        && !has(after, &["--partition"])
    {
        add("--partition", Some(partition.clone()));
    }
    if server.tls == crate::entry::Tls::Skip
        && !has(after, &["--insecure", "--unsecure"])
        && !grammar
            .excludes_insecure
            .iter()
            .any(|o| has(after, &[o.as_str()]))
    {
        add("--insecure", None);
    }
    if !rest {
        if server.handshake == crate::entry::Handshake::Browser
            && grammar.accepts.as_browser
            && !has(after, &["--as-browser"])
        {
            add("--as-browser", None);
        }
        if let Some(seconds) = server.keepalive
            && grammar.accepts.keepalive
            && !has(after, &["--keepalive"])
        {
            add("--keepalive", Some(seconds.to_string()));
        }
        if grammar.accepts.types && !has(after, &["--type", "--offer"]) {
            for kind in &server.types {
                add("--type", Some(kind.clone()));
            }
        }
        if let Some(offer) = &server.offer
            && grammar.accepts.offer
            && !has(after, &["--offer", "--type"])
            && server.types.is_empty()
        {
            add("--offer", Some(offer.clone()));
        }
    }
    if let Some(count) = server.at_once
        && grammar.accepts.at_once
        && !has(after, &["--at-once", "--parallel"])
    {
        add("--at-once", Some(count.to_string()));
    }
    if !grammar.create_command.is_empty() && command == grammar.create_command {
        for (key, value) in entry.defaults.custom_entries() {
            if !has_custom(after, key) {
                add("--custom", Some(format!("{key}={value}")));
            }
        }
    }
    Ok(out)
}

fn passthrough(argv: &[OsString]) -> Expanded {
    Expanded {
        argv: argv.to_vec(),
        entry: None,
        via_default: false,
    }
}

fn opened(catalog: &Result<Catalog, CatalogError>) -> Result<&Catalog, CatalogError> {
    catalog.as_ref().map_err(Clone::clone)
}

/// Rewrite the command line (`argv[0]` is the program) for the catalogue.
///
/// `catalog` is what opening the catalogue gave: when there is none (not
/// initialised, or unreadable) a command line that does not use it is left
/// alone, and one that does gets the reason as its error.
pub fn expand(
    argv: &[OsString],
    catalog: &Result<Catalog, CatalogError>,
    grammar: &Grammar,
) -> Result<Expanded, CatalogError> {
    let Some((program, args)) = argv.split_first() else {
        return Ok(passthrough(argv));
    };
    let (named, rest) = take_server_option(args)?;
    let position = rest
        .iter()
        .position(|a| text(a).is_none_or(|t| !t.starts_with('-')));
    let word = position.and_then(|p| text(&rest[p])).map(str::to_string);

    if !grammar.subcommands {
        return expand_flat(program, named, &rest, position, word.as_deref(), catalog, grammar);
    }

    // Which server, and where the command is.
    let mut default_used = false;
    let (found_name, before, command, after): (Option<String>, Vec<OsString>, Option<String>, Vec<OsString>) =
        match (named, word.as_deref()) {
            (Some(name), _) => {
                let p = position.unwrap_or(rest.len());
                let command = rest.get(p).and_then(text).map(str::to_string);
                let after = if command.is_some() {
                    rest[p + 1..].to_vec()
                } else {
                    Vec::new()
                };
                (Some(name), rest[..p].to_vec(), command, after)
            }
            (None, Some(w)) if w.starts_with('@') => {
                let p = position.unwrap_or(0);
                let next = rest
                    .get(p + 1)
                    .and_then(text)
                    .filter(|t| !t.starts_with('-'))
                    .map(str::to_string);
                let skip = if next.is_some() { p + 2 } else { p + 1 };
                (
                    Some(w[1..].to_string()),
                    rest[..p].to_vec(),
                    next,
                    rest[skip..].to_vec(),
                )
            }
            (None, Some(w)) if grammar.commands.contains(w) => {
                let p = position.unwrap_or(0);
                let command = w.to_string();
                let after = rest[p + 1..].to_vec();
                let takes_server = !grammar.without_server.contains(&command);
                let url_given = after
                    .iter()
                    .map_while(|a| text(a).or(Some("")).filter(|t| *t != "--"))
                    .any(is_url);
                let default = match catalog {
                    Ok(catalog) if takes_server && !url_given => {
                        catalog.default_entry().map(|f| f.name.to_string())
                    }
                    _ => None,
                };
                match default {
                    Some(name) => {
                        if grammar.writing.contains(&command) {
                            return Err(refused(format!(
                                "`{command}` writes, so it never goes to the default server `{name}`: name the server (`--server NAME`)"
                            )));
                        }
                        default_used = true;
                        (Some(name), rest[..p].to_vec(), Some(command), after)
                    }
                    None => return Ok(passthrough(argv)),
                }
            }
            (None, Some(w)) => {
                // Neither a command nor `@name`: a server's name, or a mistake.
                let p = position.unwrap_or(0);
                match catalog {
                    Ok(c) if c.knows(w) => {
                        let next = rest
                            .get(p + 1)
                            .and_then(text)
                            .filter(|t| !t.starts_with('-'))
                            .map(str::to_string);
                        let skip = if next.is_some() { p + 2 } else { p + 1 };
                        (
                            Some(w.to_string()),
                            rest[..p].to_vec(),
                            next,
                            rest[skip..].to_vec(),
                        )
                    }
                    _ => {
                        let mut words: Vec<String> = grammar.commands.iter().map(str::to_string).collect();
                        if let Ok(c) = catalog {
                            words.extend(c.words());
                        }
                        return Err(CatalogError::UnknownWord {
                            word: w.to_string(),
                            near: near(w, words),
                        });
                    }
                }
            }
            (None, None) => return Ok(passthrough(argv)),
        };
    let Some(found_name) = found_name else {
        return Ok(passthrough(argv));
    };
    let catalog = opened(catalog)?;
    let found = catalog.find(&found_name)?;
    let command = command.unwrap_or_else(|| grammar.default_command.clone());
    if !grammar.commands.contains(&command) {
        let mut words: Vec<String> = grammar.commands.iter().map(str::to_string).collect();
        words.extend(catalog.words());
        return Err(CatalogError::UnknownWord {
            near: near(&command, words),
            word: command,
        });
    }
    if grammar.without_server.contains(&command) {
        return Err(refused(format!("`{command}` takes no server")));
    }
    let pieces = inserted(found, grammar, &command, &after)?;
    let mut out = vec![program.clone()];
    out.extend(before);
    out.push(command.into());
    out.extend(pieces);
    out.extend(after);
    Ok(Expanded {
        argv: out,
        entry: Some(found.name.to_string()),
        via_default: default_used,
    })
}

/// A program with one command: `--server NAME` or a first positional `@NAME`
/// becomes the URL as its first positional.
fn expand_flat(
    program: &OsString,
    named: Option<String>,
    rest: &[OsString],
    position: Option<usize>,
    word: Option<&str>,
    catalog: &Result<Catalog, CatalogError>,
    grammar: &Grammar,
) -> Result<Expanded, CatalogError> {
    let at_word = word.filter(|w| w.starts_with('@'));
    let (name, remaining): (String, Vec<OsString>) = match (named, at_word, position) {
        (Some(_), Some(_), _) => return Err(refused("the server is named twice")),
        (Some(name), None, _) => (name, rest.to_vec()),
        (None, Some(w), Some(p)) => {
            let mut remaining = rest.to_vec();
            remaining.remove(p);
            (w[1..].to_string(), remaining)
        }
        _ => {
            return Ok(Expanded {
                argv: std::iter::once(program.clone())
                    .chain(rest.iter().cloned())
                    .collect(),
                entry: None,
                via_default: false,
            });
        }
    };
    let catalog = opened(catalog)?;
    let found = catalog.find(&name)?;
    let pieces = inserted(found, grammar, "", &remaining)?;
    let mut out = vec![program.clone()];
    out.extend(pieces);
    out.extend(remaining);
    Ok(Expanded {
        argv: out,
        entry: Some(found.name.to_string()),
        via_default: false,
    })
}
