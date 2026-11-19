//! `--save-as`: an entry written from a connection that worked.
//!
//! The command line has already decided the login (`Choice::login`), so the
//! method is deduced from the `Login` it gave, not guessed again. For each
//! secret the entry gets a **reference**, never the value:
//!
//! - the value came from an `ETP_*` variable: the entry reads that variable,
//!   and says it is shared with every other server;
//! - the value came from the command line (`--password`…): nothing of it is
//!   saved, since it is already in the shell's history; the entry points at a
//!   variable of its own name, and the advice says to define it.
//!
//! A header given with `--header` follows the same rule: its value is saved
//! when its name does not say it is a secret, as `server add --header` saves
//! it; one whose name does is saved as a reference to a variable of its own.

use retp_client::login::{Choice, Login};
use retp_client::{ExtraHeaders, sensitive_header};

use crate::entry::{AuthSpec, Entry, Handshake, Header, HeaderValue, ServerSpec, Tls, Transport};
use crate::error::CatalogError;
use crate::secret::SecretRef;
use crate::util::shout;

/// What a command connected with, minus every secret but one: the value of a
/// header given with `--header`, which is held as the client holds it (never
/// shown) and is not saved when the header's name says it carries a secret.
#[derive(Debug, Clone, Default)]
pub struct Connection {
    /// The URL.
    pub url: String,
    /// REST rather than ETP.
    pub rest: bool,
    /// The headers given in addition (`--header`). Their `Debug` shows the
    /// names only.
    pub headers: ExtraHeaders,
    /// The certificate was not checked.
    pub insecure: bool,
    /// The handshake was a web page's.
    pub as_browser: bool,
    /// The keepalive, in seconds.
    pub keepalive: Option<u64>,
    /// How many requests were in flight.
    pub at_once: Option<usize>,
    /// The data object types asked for.
    pub types: Vec<String>,
    /// The offer file.
    pub offer: Option<String>,
}

/// An entry to save, and what to tell the person about it.
#[derive(Debug, Clone)]
pub struct Observed {
    /// The entry.
    pub entry: Entry,
    /// Lines for stderr: what was saved as a shared variable, what was not saved.
    pub advice: Vec<String>,
}

fn given(value: &Option<String>) -> bool {
    value.as_deref().is_some_and(|v| !v.is_empty())
}

/// The entry `name` for a connection whose login options were `choice`.
///
/// `env` reads the `ETP_*` variables the way the command did. The values of
/// `choice` are looked at to know *where* each secret came from, and are not kept.
pub fn observe(
    name: &str,
    connection: &Connection,
    choice: &Choice,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<Observed, CatalogError> {
    let login = choice
        .login(env)
        .map_err(|error| CatalogError::refused(format!("the login options are not usable: {error}")))?;
    let mut advice = Vec::new();
    let mut reference = |field: &str, on_command_line: bool, variable: &str| -> SecretRef {
        let option = field.replace('_', "-");
        if on_command_line {
            let own = format!("RETP_{}_{}", shout(name), shout(field));
            advice.push(format!(
                "the value given with --{option} was NOT saved (it is already in your shell history); the entry reads the variable {own}: define it, or save the entry again with --force and --{option}-from env:NAME or file:PATH"
            ));
            SecretRef::Env(own)
        } else {
            advice.push(format!(
                "the entry reads {variable} for its {field}: that variable is shared with every other server, give this one a variable of its own with --{option}-from"
            ));
            SecretRef::Env(variable.to_string())
        }
    };
    let azure = choice.azure_tenant.clone();
    let auth = match login {
        None => AuthSpec::None,
        Some(Login::Bearer(_)) => AuthSpec::Bearer {
            token: reference("token", given(&choice.token), "ETP_TOKEN"),
        },
        Some(Login::Basic { user, .. }) => AuthSpec::Basic {
            user,
            password: reference("password", given(&choice.password), "ETP_PASSWORD"),
        },
        Some(Login::ClientCredentials {
            token_url,
            client_id,
            scope,
            ..
        }) => {
            let secret = reference("client_secret", given(&choice.client_secret), "ETP_CLIENT_SECRET");
            match azure {
                Some(tenant) => AuthSpec::Azure {
                    tenant,
                    client_id,
                    scope,
                    client_secret: Some(secret),
                    user: None,
                    password: None,
                },
                None => AuthSpec::OauthClient {
                    token_url,
                    client_id,
                    client_secret: secret,
                    scope,
                },
            }
        }
        Some(Login::Password {
            token_url,
            user,
            client_id,
            client_secret,
            scope,
            ..
        }) => {
            let password = reference("password", given(&choice.password), "ETP_PASSWORD");
            let client_secret = client_secret
                .is_some()
                .then(|| reference("client_secret", given(&choice.client_secret), "ETP_CLIENT_SECRET"));
            match azure {
                Some(tenant) => AuthSpec::Azure {
                    tenant,
                    client_id: client_id.ok_or_else(|| {
                        CatalogError::refused("an Azure login needs --client-id to be saved")
                    })?,
                    scope,
                    client_secret,
                    user: Some(user),
                    password: Some(password),
                },
                None => AuthSpec::OauthPassword {
                    token_url,
                    user,
                    password,
                    client_id,
                    client_secret,
                    scope,
                },
            }
        }
        Some(Login::Cognito {
            region,
            client_id,
            client_secret,
            user,
            ..
        }) => {
            let password = reference("password", given(&choice.password), "ETP_PASSWORD");
            let client_secret = client_secret
                .is_some()
                .then(|| reference("client_secret", given(&choice.client_secret), "ETP_CLIENT_SECRET"));
            AuthSpec::Cognito {
                region,
                client_id,
                user,
                password,
                client_secret,
            }
        }
    };
    let mut server = if connection.rest {
        ServerSpec::rest(&connection.url)
    } else {
        ServerSpec::etp(&connection.url)
    };
    server.transport = if connection.rest {
        Transport::Rest
    } else {
        Transport::Etp
    };
    server.tls = if connection.insecure {
        Tls::Skip
    } else {
        Tls::Verify
    };
    server.handshake = if connection.as_browser {
        Handshake::Browser
    } else {
        Handshake::Native
    };
    server.keepalive = connection.keepalive;
    server.at_once = connection.at_once;
    server.types = connection.types.clone();
    server.offer = connection.offer.clone();
    let mut entry = Entry::new(server, auth);
    for (header, value) in connection.headers.iter() {
        let value = if sensitive_header(header) {
            let own = format!("RETP_{}_HEADER_{}", shout(name), shout(header));
            advice.push(format!(
                "the value given with --header {header} was NOT saved (it is already in your shell history); the entry reads the variable {own}: define it, or give the entry another reference with `retp server add --force` and --header-from {header}=env:NAME or {header}=file:PATH"
            ));
            HeaderValue::Secret(SecretRef::Env(own))
        } else {
            HeaderValue::Literal(value.to_string())
        };
        entry.headers.push(Header {
            name: header.to_string(),
            value,
        });
    }
    entry.validate().map_err(|problem| problem.located(None))?;
    Ok(Observed { entry, advice })
}
