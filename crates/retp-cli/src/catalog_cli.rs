//! `retp`'s side of the server catalogue: argument parsing, prompts, printing.
//!
//! No logic lives here. What a catalogue is, how a name becomes a URL, how a
//! secret is read and kept out of every output, is `retp-catalog`'s; this
//! module parses the options of `retp catalog` and `retp server`, asks the
//! questions of the guided form, and prints what the library returns.
//!
//! A **child of `etp`** (`#[path = "catalog_cli.rs"] mod catalog_cli;` inside
//! `etp.rs`), because it reads `Server` and `LoginArgs`, which are private there.

use std::ffi::OsString;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Args, CommandFactory, Parser, Subcommand};

use retp_catalog::{
    Accepts, Catalog, Confirmation, Connection, Draft, Expanded, Grammar, Network, Observed, OsSecrets,
    Outcome, Overrides, Reserved, SecretRef, render,
};
use retp_client::native::{self, CertificateCheck, Connect, Endpoint};
use retp_client::reading_client;

use super::{EtpArgs, EtpCommand, Server};

// ---- the grammar the catalogue needs from the parser ------------------------

/// Every command and alias of `retp`, **read from the parser**: a command added
/// tomorrow is reserved tomorrow, and `server add` refuses it as a name.
fn command_words() -> Vec<String> {
    let command = EtpArgs::command();
    let mut words = vec!["help".to_string()];
    for sub in command.get_subcommands() {
        words.push(sub.get_name().to_string());
        words.extend(sub.get_all_aliases().map(str::to_string));
    }
    words
}

fn reserved() -> Reserved {
    Reserved::new(command_words())
}

/// The commands that write. TODO(verify): a test compares this list with the
/// commands the shell refuses to replay (`replays` in `etp.rs`).
const WRITING: [&str; 3] = ["create-dataspace", "delete-dataspace", "push"];

fn grammar() -> Grammar {
    Grammar {
        subcommands: true,
        commands: reserved(),
        without_server: ["server", "catalog", "help"].map(String::from).to_vec(),
        writing: WRITING.map(String::from).to_vec(),
        default_command: "shell".into(),
        create_command: "create-dataspace".into(),
        accepts: Accepts {
            as_browser: true,
            keepalive: true,
            at_once: true,
            types: true,
            offer: true,
        },
        excludes_insecure: Vec::new(),
    }
}

/// The command line, rewritten for the catalogue (`retp azure` and the like),
/// before the parser sees it.
pub(super) fn expand_args() -> Result<Vec<OsString>> {
    let argv: Vec<OsString> = std::env::args_os().collect();
    let opened = retp_catalog::open_default();
    let Expanded {
        argv,
        entry,
        via_default,
    } = retp_catalog::expand(&argv, &opened, &grammar())?;
    if let (true, Some(name)) = (via_default, entry) {
        eprintln!("server: {name} (default)");
    }
    Ok(argv)
}

/// What `retp` answers to a first word it does not know: the commands and the
/// servers that look like it.
pub(super) fn unknown(words: &[OsString]) -> anyhow::Error {
    let word = words
        .first()
        .map(|w| w.to_string_lossy().to_string())
        .unwrap_or_default();
    let opened = retp_catalog::open_default();
    retp_catalog::unknown_word(&word, &command_words(), &opened).into()
}

// ---- connecting -----------------------------------------------------------------

/// The endpoint of a catalogue server: its secrets read, its login done, its
/// warnings said. `None` when the command names no catalogue server.
pub(super) fn endpoint(server: &Server) -> Result<Option<Endpoint>> {
    let Some(name) = &server.catalog_entry else {
        return Ok(None);
    };
    let catalog = retp_catalog::open_default()?;
    let found = catalog.find(name)?;
    let overrides = Overrides {
        url: Some(server.url.clone()),
        rest: Some(server.rest),
        partition: server.partition.clone(),
        insecure: server.insecure,
        as_browser: server.as_browser,
    };
    let resolved = found.entry.resolve(found.name, &OsSecrets::new(), &overrides)?;
    let opened = resolved.obtain_endpoint()?;
    for note in &opened.notes {
        eprintln!("{note}");
    }
    Ok(Some(opened.endpoint))
}

/// A `Server` for `connect NAME` in a shell: the same expansion as `retp @NAME`.
pub(super) fn server_named(name: &str) -> Result<Server> {
    let argv: Vec<OsString> = ["retp", "shell", &format!("@{name}")]
        .iter()
        .map(OsString::from)
        .collect();
    let opened = retp_catalog::open_default();
    let expanded = retp_catalog::expand(&argv, &opened, &grammar())?;
    let args = EtpArgs::try_parse_from(expanded.argv).map_err(|e| anyhow::anyhow!("{e}"))?;
    match args.command {
        EtpCommand::Shell(server) => Ok(server),
        _ => anyhow::bail!("`{name}` did not give a shell"),
    }
}

/// The prompt of a shell: the server's name when it is a catalogue server.
pub(super) fn prompt(server: &Server) -> String {
    match &server.catalog_entry {
        Some(name) => format!("{name}> "),
        None => "etp> ".to_string(),
    }
}

/// Where the shell of a catalogue server keeps its history.
pub(super) fn history_path(server: &Server) -> Option<PathBuf> {
    let name = server.catalog_entry.as_deref()?;
    retp_catalog::open_default().ok()?.history_path(name)
}

/// A command that writes, about to go to a server: a protected one asks first.
///
/// `can_ask` is false in a shell, whose input is the command stream: there,
/// only `--yes` confirms.
pub(super) fn guard_write(server: &Server, command: &str, yes: bool, can_ask: bool) -> Result<()> {
    let Some(name) = &server.catalog_entry else {
        return Ok(());
    };
    let catalog = retp_catalog::open_default()?;
    let found = catalog.find(name)?;
    let protected = found.entry.server.protected;
    if !protected {
        return Ok(());
    }
    let confirmation = if yes {
        Confirmation::Yes
    } else if can_ask && std::io::stdin().is_terminal() && std::io::stderr().is_terminal() {
        eprint!(
            "server `{}` is protected and `{command}` writes to it. Type the server's name to confirm: ",
            found.name
        );
        std::io::stderr().flush().ok();
        let mut typed = String::new();
        std::io::stdin()
            .read_line(&mut typed)
            .context("reading the confirmation")?;
        Confirmation::Typed(typed)
    } else {
        Confirmation::Unavailable
    };
    retp_catalog::confirm_write(found.name, protected, command, &confirmation)?;
    Ok(())
}

// ---- --save-as ----------------------------------------------------------------------

/// A connection to save as a catalogue entry once its command has succeeded.
pub(super) struct SavePlan {
    name: String,
    observed: Observed,
    force: bool,
}

/// Prepare `--save-as`: everything that can be refused is refused **before**
/// the command runs, so that a long command is not run for nothing.
pub(super) fn plan_save(server: &Server) -> Result<Option<SavePlan>> {
    let Some(name) = &server.save_as else {
        return Ok(None);
    };
    if server.catalog_entry.is_some() {
        anyhow::bail!("--save-as: this server is already in the catalogue");
    }
    let catalog = retp_catalog::open_default()?;
    catalog.check_new(name, &[], server.force, &reserved())?;
    let connection = Connection {
        url: server.url.clone(),
        rest: server.rest,
        partition: server.partition.clone(),
        insecure: server.insecure,
        as_browser: server.as_browser,
        keepalive: server.keepalive,
        at_once: server.at_once,
        types: server.types.clone(),
        offer: server.offer.as_ref().map(|path| path.display().to_string()),
    };
    let observed = retp_catalog::observe(name, &connection, &server.login.choice(), &super::from_env)?;
    Ok(Some(SavePlan {
        name: name.clone(),
        observed,
        force: server.force,
    }))
}

/// Save after a command that succeeded, never after one that failed. No value
/// is saved, and the advice says what to do about a secret that came from the line.
pub(super) fn finish_save(plan: Option<SavePlan>, outcome: &Result<ExitCode>) -> Result<()> {
    let Some(plan) = plan else {
        return Ok(());
    };
    // `ExitCode` has no `PartialEq`: its `Debug` tells success from failure.
    let succeeded = matches!(outcome, Ok(code) if format!("{code:?}") == format!("{:?}", ExitCode::SUCCESS));
    if !succeeded {
        eprintln!(
            "--save-as: `{}` was not saved, since the command did not succeed",
            plan.name
        );
        return Ok(());
    }
    let mut catalog = retp_catalog::open_default()?;
    catalog.add(&plan.name, plan.observed.entry, plan.force, &reserved())?;
    eprintln!("saved as `{}`: reach it with `retp {}`", plan.name, plan.name);
    for line in &plan.observed.advice {
        eprintln!("{line}");
    }
    Ok(())
}

// ---- the network steps of `server test` and of the guided form --------------------

struct Net;

impl Network for Net {
    fn well_known(&self, endpoint: &Endpoint) -> std::result::Result<String, String> {
        native::fetch_server_capabilities(endpoint)
            .map(|(_, capabilities)| {
                format!(
                    "{} {}",
                    capabilities.application_name, capabilities.application_version
                )
            })
            .map_err(|why| why.to_string())
    }

    fn session(&self, endpoint: Endpoint, rest: bool) -> std::result::Result<String, String> {
        if rest {
            let mut remote = native::Remote::Rest(Box::new(native::rest_client(&endpoint)));
            return match remote.health() {
                Some(Ok(health)) => Ok(format!("the REST API answers: {health}")),
                Some(Err(why)) => Err(why.to_string()),
                None => Err("a REST session has a health route; this one gave none".into()),
            };
        }
        let (offer, handlers) = reading_client(
            "retp command line",
            env!("CARGO_PKG_VERSION"),
            native::random_uuid(),
            &[],
        );
        let offer = native::fit_to_server(&endpoint, offer).0;
        let connect = Connect {
            endpoint,
            offer,
            handlers,
            max_message: None,
        };
        let client = native::connect(connect).map_err(|why| why.to_string())?;
        let remote = native::Remote::Etp(Box::new(client));
        remote.close("done").map_err(|why| why.to_string())?;
        Ok("session opened and closed".into())
    }
}

// ---- `retp catalog` ----------------------------------------------------------------

/// `retp catalog`.
#[derive(Subcommand)]
pub(super) enum CatalogCommand {
    /// Choose the folder that holds the servers, and write the configuration file.
    Init {
        /// The folder. Asked for on a terminal (a default is proposed); required otherwise.
        dir: Option<PathBuf>,
        /// Point an existing configuration at another folder.
        #[arg(long)]
        force: bool,
    },
}

pub(super) fn run_catalog(command: CatalogCommand) -> Result<ExitCode> {
    match command {
        CatalogCommand::Init { dir, force } => {
            let location = Catalog::locate()?;
            let dir = match dir {
                Some(dir) => dir,
                None if std::io::stdin().is_terminal() => {
                    let proposed = retp_catalog::default_folder();
                    let hint = proposed.as_ref().map(|p| p.display().to_string());
                    let answer = ask_text(&format!(
                        "Folder for the servers{}",
                        hint.as_ref().map(|h| format!(" [{h}]")).unwrap_or_default()
                    ))?;
                    match (answer, proposed) {
                        (Some(text), _) => PathBuf::from(text),
                        (None, Some(proposed)) => proposed,
                        (None, None) => anyhow::bail!("give the folder: `retp catalog init FOLDER`"),
                    }
                }
                None => anyhow::bail!(
                    "`retp catalog init` needs the folder as an argument when it is not run from a terminal"
                ),
            };
            let catalog = Catalog::init(&location, &dir, force)?;
            println!("catalogue folder   {}", catalog.root().display());
            println!("configuration      {}", location.config_file().display());
            println!("add a server with `retp server add`");
            Ok(ExitCode::SUCCESS)
        }
    }
}

// ---- `retp server` ---------------------------------------------------------------------

/// `retp server`.
#[derive(Subcommand)]
pub(super) enum ServerCommand {
    /// Add a server. With no argument, on a terminal, a guided form asks for each field;
    /// with arguments, the same fields from the options, and nothing is ever asked.
    Add(Box<AddArgs>),
    /// The servers, one per line: no secret, and whether the references resolve.
    List,
    /// Everything one server's definition says: references and whether they resolve, no value.
    Show {
        /// The name or an alias.
        name: String,
    },
    /// Remove a server from the catalogue.
    Remove {
        /// The name or an alias.
        name: String,
    },
    /// Check a server step by step: definition, references, capabilities document, login, session.
    Test {
        /// The name or an alias.
        name: String,
    },
    /// Make a server the one `retp COMMAND` uses when none is named (never for a command that writes).
    #[command(name = "default")]
    SetDefault {
        /// The name or an alias.
        name: String,
    },
}

/// The options of `retp server add`. A secret is given as a reference: `env:NAME` or `file:PATH`.
#[derive(Args)]
pub(super) struct AddArgs {
    /// The server's name: lowercase letters, digits, `.`, `_`, `-`.
    name: Option<String>,
    /// The server's URL: `wss://...` for ETP, `https://...` for the REST API.
    url: Option<String>,
    /// The URL of the other transport, when the server has both.
    #[arg(long, value_name = "URL")]
    other_url: Option<String>,
    /// Which transport commands use unless told otherwise: `etp` or `rest`.
    #[arg(long, value_name = "etp|rest")]
    transport: Option<String>,
    /// An OSDU data partition.
    #[arg(long)]
    partition: Option<String>,
    /// Do not check the certificate (said again at every connection).
    #[arg(long)]
    insecure: bool,
    /// Connect as a web page can.
    #[arg(long)]
    as_browser: bool,
    /// A command that writes to this server asks for a confirmation (`--yes` for a script).
    #[arg(long)]
    protected: bool,
    /// A line about the server.
    #[arg(long)]
    description: Option<String>,
    /// Another name for it, repeatable.
    #[arg(long = "alias", value_name = "NAME")]
    aliases: Vec<String>,
    /// The login method: none, basic, bearer, oauth-password, oauth-client, azure, cognito.
    #[arg(long = "auth", value_name = "METHOD")]
    method: Option<String>,
    /// An Azure AD tenant.
    #[arg(long)]
    tenant: Option<String>,
    /// The OAuth or Cognito app client's id.
    #[arg(long)]
    client_id: Option<String>,
    /// The scope to ask the token for.
    #[arg(long)]
    scope: Option<String>,
    /// An OAuth 2.0 token endpoint.
    #[arg(long)]
    token_url: Option<String>,
    /// The AWS region of a Cognito user pool.
    #[arg(long)]
    region: Option<String>,
    /// The user (not a secret).
    #[arg(long)]
    user: Option<String>,
    /// Where the token is read from: `env:NAME` or `file:PATH`.
    #[arg(long, value_name = "REF")]
    token_from: Option<String>,
    /// Where the password is read from: `env:NAME` or `file:PATH`.
    #[arg(long, value_name = "REF")]
    password_from: Option<String>,
    /// Where the client's secret is read from: `env:NAME` or `file:PATH`.
    #[arg(long, value_name = "REF")]
    client_secret_from: Option<String>,
    /// An extra request header with a literal value, `NAME=VALUE`, repeatable. Refused for a name
    /// that looks secret (`authorization`, `x-api-key`...): use `--header-from`.
    #[arg(long = "header", value_name = "NAME=VALUE")]
    headers: Vec<String>,
    /// An extra request header read from a reference, `NAME=env:VAR` or `NAME=file:PATH`, repeatable.
    #[arg(long = "header-from", value_name = "NAME=REF")]
    headers_from: Vec<String>,
    /// A legal tag for a dataspace created on this server, repeatable.
    #[arg(long = "legal-tag", value_name = "TAG")]
    legal_tags: Vec<String>,
    /// An owner of a dataspace created on this server, repeatable.
    #[arg(long = "owner", value_name = "OWNER")]
    owners: Vec<String>,
    /// A viewer of a dataspace created on this server, repeatable.
    #[arg(long = "viewer", value_name = "VIEWER")]
    viewers: Vec<String>,
    /// Replace a server of the same name.
    #[arg(long)]
    force: bool,
}

/// `NAME=VALUE`, the value never shown in an error.
fn pairs(options: &[String], option: &str) -> Result<Vec<(String, String)>> {
    options
        .iter()
        .map(|text| {
            text.split_once('=')
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .ok_or_else(|| anyhow::anyhow!("{option} takes NAME=VALUE (the text given is not shown)"))
        })
        .collect()
}

fn draft_of(args: &AddArgs, url: String) -> Result<Draft> {
    Ok(Draft {
        url,
        other_url: args.other_url.clone(),
        transport: args.transport.clone(),
        partition: args.partition.clone(),
        insecure: args.insecure,
        as_browser: args.as_browser,
        protected: args.protected,
        description: args.description.clone(),
        aliases: args.aliases.clone(),
        method: args.method.clone(),
        tenant: args.tenant.clone(),
        client_id: args.client_id.clone(),
        scope: args.scope.clone(),
        token_url: args.token_url.clone(),
        region: args.region.clone(),
        user: args.user.clone(),
        token_from: args.token_from.clone(),
        password_from: args.password_from.clone(),
        client_secret_from: args.client_secret_from.clone(),
        headers: pairs(&args.headers, "--header")?,
        headers_from: pairs(&args.headers_from, "--header-from")?,
        legal_tags: args.legal_tags.clone(),
        owners: args.owners.clone(),
        viewers: args.viewers.clone(),
    })
}

pub(super) fn run_servers(command: ServerCommand) -> Result<ExitCode> {
    match command {
        ServerCommand::Add(args) => add(*args),
        ServerCommand::List => {
            let catalog = retp_catalog::open_default()?;
            for warning in catalog.warnings() {
                eprintln!("warning: {warning}");
            }
            print!("{}", render::list(&catalog.listed(&OsSecrets::new())));
            Ok(ExitCode::SUCCESS)
        }
        ServerCommand::Show { name } => {
            let catalog = retp_catalog::open_default()?;
            let found = catalog.find(&name)?;
            print!(
                "{}",
                render::show(&found.entry.describe(found.name, &OsSecrets::new()))
            );
            Ok(ExitCode::SUCCESS)
        }
        ServerCommand::Remove { name } => {
            let mut catalog = retp_catalog::open_default()?;
            catalog.remove(&name)?;
            println!("removed `{name}`");
            Ok(ExitCode::SUCCESS)
        }
        ServerCommand::SetDefault { name } => {
            let mut catalog = retp_catalog::open_default()?;
            catalog.set_default(&name)?;
            println!("`{name}` is the default server, for the commands that do not write");
            Ok(ExitCode::SUCCESS)
        }
        ServerCommand::Test { name } => {
            let catalog = retp_catalog::open_default()?;
            let report = catalog.run_test(&name, &OsSecrets::new(), &Net);
            print!("{}", render::test(&report));
            if matches!(report.steps.first().map(|s| &s.outcome), Some(Outcome::Passed(_))) {
                catalog.record_test(&report)?;
            }
            Ok(if report.passed() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
    }
}

fn add(args: AddArgs) -> Result<ExitCode> {
    let mut catalog = retp_catalog::open_default()?;
    let (name, draft, probe) = match (&args.name, &args.url) {
        (Some(name), Some(url)) => (name.clone(), draft_of(&args, url.clone())?, false),
        (None, None) if std::io::stdin().is_terminal() && std::io::stderr().is_terminal() => {
            let (name, draft) = guided()?;
            (name, draft, true)
        }
        (None, None) => anyhow::bail!(
            "`retp server add` needs NAME and URL (and the options of the login) when it is not run from a terminal: a script is never asked a question"
        ),
        _ => anyhow::bail!("`retp server add` needs both NAME and URL, or neither for the guided form"),
    };
    let entry = draft.into_entry()?;
    catalog.check_new(&name, &entry.aliases, args.force, &reserved())?;
    if probe && let Some(url) = &entry.server.etp_url {
        let mut endpoint = Endpoint::new(url.clone());
        endpoint.certificates = match entry.server.tls {
            retp_catalog::Tls::Skip => CertificateCheck::Skip,
            retp_catalog::Tls::Verify => CertificateCheck::Verify,
        };
        match Net.well_known(&endpoint) {
            Ok(said) => eprintln!("the server answers at /.well-known: {said}"),
            Err(why) => {
                eprintln!("the server did not answer at /.well-known: {why}");
                if !ask_yes_no("Save it anyway?", false)? {
                    anyhow::bail!("not saved");
                }
            }
        }
    }
    let has_headers = !entry.headers.is_empty();
    catalog.add(&name, entry, args.force, &reserved())?;
    println!("added `{name}`; check it with `retp server test {name}`");
    if has_headers {
        eprintln!(
            "note: this version of retp cannot send extra headers yet; a connection to `{name}` will say so rather than leave them out"
        );
    }
    Ok(ExitCode::SUCCESS)
}

// ---- the guided form ------------------------------------------------------------------------

const METHODS: [&str; 7] = [
    "none",
    "basic",
    "bearer",
    "oauth-password",
    "oauth-client",
    "azure",
    "cognito",
];

fn line(prompt: &str) -> Result<String> {
    eprint!("{prompt}: ");
    std::io::stderr().flush().ok();
    let mut text = String::new();
    if std::io::stdin()
        .read_line(&mut text)
        .context("reading the answer")?
        == 0
    {
        anyhow::bail!("the input ended before the form was complete");
    }
    Ok(text.trim().to_string())
}

/// A text, or `None` for an empty answer.
fn ask_text(prompt: &str) -> Result<Option<String>> {
    let answer = line(prompt)?;
    Ok((!answer.is_empty()).then_some(answer))
}

fn ask_required(prompt: &str) -> Result<String> {
    loop {
        if let Some(answer) = ask_text(prompt)? {
            return Ok(answer);
        }
        eprintln!("an answer is needed");
    }
}

fn ask_yes_no(prompt: &str, default: bool) -> Result<bool> {
    loop {
        let hint = if default { "[Y/n]" } else { "[y/N]" };
        match line(&format!("{prompt} {hint}"))?.to_ascii_lowercase().as_str() {
            "" => return Ok(default),
            "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => eprintln!("answer y or n"),
        }
    }
}

fn ask_list(prompt: &str) -> Result<Vec<String>> {
    Ok(ask_text(prompt)?
        .map(|text| {
            text.split(',')
                .map(|part| part.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect()
        })
        .unwrap_or_default())
}

/// A secret is a reference: what is typed is checked as one, and a value
/// typed by mistake is refused without being echoed.
fn ask_reference(prompt: &str) -> Result<String> {
    loop {
        let text = ask_required(&format!("{prompt} (env:NAME or file:PATH, never the value)"))?;
        match SecretRef::parse(&text) {
            Ok(_) => return Ok(text),
            Err(why) => eprintln!("{why}"),
        }
    }
}

fn guided() -> Result<(String, Draft)> {
    eprintln!("Adding a server. A secret is never typed here: it is a reference, env:NAME or file:PATH.");
    let name = ask_required("Name (lowercase letters, digits, `.`, `_`, `-`)")?;
    let method = loop {
        let answer = ask_required(&format!("Authentication method ({})", METHODS.join(", ")))?;
        if METHODS.contains(&answer.as_str()) {
            break answer;
        }
        eprintln!("one of: {}", METHODS.join(", "));
    };
    let etp = ask_text("ETP WebSocket URL (wss://..., empty for none)")?;
    let rest = ask_text("REST URL (https://..., empty for none)")?;
    let (url, other_url) = match (etp, rest) {
        (Some(etp), rest) => (etp, rest),
        (None, Some(rest)) => (rest, None),
        (None, None) => anyhow::bail!("a server needs an ETP URL or a REST URL"),
    };
    let mut draft = Draft {
        url,
        other_url,
        method: Some(method.clone()),
        ..Draft::default()
    };
    match method.as_str() {
        "basic" => {
            draft.user = Some(ask_required("User")?);
            draft.password_from = Some(ask_reference("Password")?);
        }
        "bearer" => draft.token_from = Some(ask_reference("Token")?),
        "oauth-password" => {
            draft.token_url = Some(ask_required("Token endpoint URL")?);
            draft.user = Some(ask_required("User")?);
            draft.password_from = Some(ask_reference("Password")?);
            draft.client_id = ask_text("Client id (empty for none)")?;
            if draft.client_id.is_some() && ask_yes_no("Does the client have a secret?", false)? {
                draft.client_secret_from = Some(ask_reference("Client secret")?);
            }
            draft.scope = ask_text("Scope (empty for none)")?;
        }
        "oauth-client" => {
            draft.token_url = Some(ask_required("Token endpoint URL")?);
            draft.client_id = Some(ask_required("Client id")?);
            draft.client_secret_from = Some(ask_reference("Client secret")?);
            draft.scope = ask_text("Scope (empty for none)")?;
        }
        "azure" => {
            draft.tenant = Some(ask_required("Azure AD tenant (a GUID or a domain)")?);
            draft.client_id = Some(ask_required("Application (client) id")?);
            draft.scope = ask_text("Scope, for example api://ID/.default (empty for none)")?;
            if ask_yes_no(
                "Log in as a user (password grant) rather than as the application?",
                false,
            )? {
                draft.user = Some(ask_required("User")?);
                draft.password_from = Some(ask_reference("Password")?);
            } else {
                draft.client_secret_from = Some(ask_reference("Client secret")?);
            }
        }
        "cognito" => {
            draft.region = Some(ask_required(
                "AWS region of the user pool, for example us-east-2",
            )?);
            draft.client_id = Some(ask_required("App client id")?);
            draft.user = Some(ask_required("User")?);
            draft.password_from = Some(ask_reference("Password")?);
            if ask_yes_no("Does the app client have a secret?", false)? {
                draft.client_secret_from = Some(ask_reference("Client secret")?);
            }
        }
        _ => {}
    }
    while let Some(header) = ask_text("Extra request header name (empty to finish)")? {
        let value = ask_required(&format!(
            "  value of {header} (env:NAME or file:PATH for a header that carries a secret; any other text is a literal)"
        ))?;
        if value.starts_with("env:") || value.starts_with("file:") {
            draft.headers_from.push((header, value));
        } else {
            draft.headers.push((header, value));
        }
    }
    draft.partition = ask_text("OSDU data partition (empty for none)")?;
    eprintln!(
        "What a dataspace created on this server needs (not secrets; comma-separated, empty for none):"
    );
    draft.legal_tags = ask_list("  legal tags")?;
    draft.owners = ask_list("  owners")?;
    draft.viewers = ask_list("  viewers")?;
    draft.insecure = ask_yes_no(
        "Skip the certificate check (a known-bad certificate only)?",
        false,
    )?;
    draft.protected = ask_yes_no("Protected (a command that writes asks for confirmation)?", false)?;
    draft.description = ask_text("A line about the server (empty for none)")?;
    draft.aliases = ask_list("Other names (comma-separated, empty for none)")?;
    Ok((name, draft))
}

// ---- tests -------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The login options the library refuses with a catalogue server are exactly the ones
    /// `LoginArgs` declares: two sources of one fact, one of them the parser.
    #[test]
    fn the_refused_login_options_are_the_login_options_of_the_parser() {
        let command = super::super::LoginArgs::augment_args(clap::Command::new("x"));
        let mut declared: Vec<String> = command
            .get_arguments()
            .filter_map(|a| a.get_long().map(|l| format!("--{l}")))
            .collect();
        let mut refused: Vec<String> = retp_catalog::LOGIN_OPTIONS
            .iter()
            .map(|s| s.to_string())
            .collect();
        declared.sort();
        refused.sort();
        assert_eq!(declared, refused);
    }

    /// Every command of the parser is reserved, and the ones that write are real commands.
    #[test]
    fn the_reserved_words_are_read_from_the_parser() {
        let words = command_words();
        for sub in EtpArgs::command().get_subcommands() {
            assert!(words.contains(&sub.get_name().to_string()), "{}", sub.get_name());
        }
        for word in ["server", "catalog", "shell", "help"] {
            assert!(words.iter().any(|w| w == word), "{word}");
        }
        for writing in WRITING {
            assert!(words.iter().any(|w| w == writing), "{writing} is not a command");
        }
    }

    /// What the library rewrites, the parser takes: the URL as the first positional, the entry's name in
    /// the hidden option, the options of the entry.
    #[test]
    fn a_rewritten_line_parses_into_the_server_of_the_entry() {
        use retp_catalog::{AuthSpec, Entry, Location, ServerSpec};
        let root = std::env::temp_dir().join(format!("retp-cli-catalog-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let location = Location::new(root.join("config.toml"), None);
        let mut catalog = Catalog::init(&location, &root.join("servers"), false).expect("init");
        let mut spec = ServerSpec::etp("wss://adme.example.com/etp");
        spec.partition = Some("opendes".into());
        catalog
            .add("azure", Entry::new(spec, AuthSpec::None), false, &reserved())
            .expect("add");
        let opened = Ok(catalog);
        let argv: Vec<OsString> = ["retp", "azure", "dataspaces"]
            .iter()
            .map(OsString::from)
            .collect();
        let expanded = retp_catalog::expand(&argv, &opened, &grammar()).expect("expanded");
        let args = EtpArgs::try_parse_from(expanded.argv).expect("parsed");
        match args.command {
            EtpCommand::Dataspaces(server) => {
                assert_eq!(server.url, "wss://adme.example.com/etp");
                assert_eq!(server.catalog_entry.as_deref(), Some("azure"));
                assert_eq!(server.partition.as_deref(), Some("opendes"));
            }
            _ => panic!("not `dataspaces`"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }
}
