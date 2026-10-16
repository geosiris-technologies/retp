//! A server entry: the definition `server.toml` holds, **without any secret**.
//!
//! A secret is a [`SecretRef`] (`{ env = "NAME" }`, `{ file = "PATH" }`); a
//! literal value for `password`, `token`, `client_secret` or `secret` is
//! refused when the file is read, naming the key and the line and never the
//! value. The messages of the TOML parser quote the source, so they are never
//! shown as they are: only their line number and a message with every quoted
//! piece blanked.

use std::fmt::Write as _;

use serde::Deserialize;

use crate::error::CatalogError;
use crate::secret::{SecretRef, not_yet};
use crate::util::{check_name, has_credentials, quote};

/// A fault in one key of a definition, before it knows its line.
pub(crate) struct Problem {
    pub(crate) key: String,
    pub(crate) why: String,
}

impl Problem {
    pub(crate) fn new(key: &str, why: impl Into<String>) -> Self {
        Problem {
            key: key.to_string(),
            why: why.into(),
        }
    }

    /// The error, with the line of its key found in `text` when there is one.
    pub(crate) fn located(self, text: Option<&str>) -> CatalogError {
        let line = text.and_then(|text| line_of(text, &self.key));
        CatalogError::entry(Some(&self.key), line, self.why)
    }
}

/// Which URL a command uses by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// The ETP WebSocket (`etp_url`).
    Etp,
    /// OSDU's Reservoir DDMS REST API (`rest_url`).
    Rest,
}

impl Transport {
    /// `etp` or `rest`.
    pub fn as_str(self) -> &'static str {
        match self {
            Transport::Etp => "etp",
            Transport::Rest => "rest",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "etp" => Some(Transport::Etp),
            "rest" => Some(Transport::Rest),
            _ => None,
        }
    }
}

/// Whether the server's certificate is checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tls {
    /// Checked (the default).
    Verify,
    /// Not checked (`--insecure`): said again at every connection.
    Skip,
}

/// How the handshake is made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handshake {
    /// Credentials and attributes in headers.
    Native,
    /// As a web page can (`--as-browser`).
    Browser,
}

/// Where the server is and how to speak to it: nothing secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerSpec {
    /// The ETP WebSocket URL (`ws://` or `wss://`).
    pub etp_url: Option<String>,
    /// The REST API's URL (`http://` or `https://`).
    pub rest_url: Option<String>,
    /// Which of the two a command uses unless told otherwise.
    pub transport: Transport,
    /// An OSDU data partition.
    pub partition: Option<String>,
    /// Whether the certificate is checked.
    pub tls: Tls,
    /// How the handshake is made.
    pub handshake: Handshake,
    /// Writing commands ask for a confirmation on this server.
    pub protected: bool,
    /// The keepalive, in seconds (`--keepalive`).
    pub keepalive: Option<u64>,
    /// How many requests are in flight at once (`--at-once`).
    pub at_once: Option<usize>,
    /// The data object types to ask for (`--type`).
    pub types: Vec<String>,
    /// A JSON offer file (`--offer`).
    pub offer: Option<String>,
}

impl ServerSpec {
    /// A server reached through an ETP WebSocket, certificate checked.
    pub fn etp(url: impl Into<String>) -> Self {
        ServerSpec {
            etp_url: Some(url.into()),
            rest_url: None,
            transport: Transport::Etp,
            partition: None,
            tls: Tls::Verify,
            handshake: Handshake::Native,
            protected: false,
            keepalive: None,
            at_once: None,
            types: Vec::new(),
            offer: None,
        }
    }

    /// A server reached through its REST API, certificate checked.
    pub fn rest(url: impl Into<String>) -> Self {
        ServerSpec {
            etp_url: None,
            rest_url: Some(url.into()),
            transport: Transport::Rest,
            ..ServerSpec::etp("")
        }
    }

    /// The URL of the default transport.
    pub fn selected_url(&self) -> Option<&str> {
        match self.transport {
            Transport::Etp => self.etp_url.as_deref(),
            Transport::Rest => self.rest_url.as_deref(),
        }
    }
}

/// How the client gets in: what `Choice` of `retp-client` knows, one method at
/// a time. Every secret is a reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthSpec {
    /// No credentials. Ambient `ETP_*` variables are not used either.
    None,
    /// HTTP Basic.
    Basic {
        /// The user-id.
        user: String,
        /// Its password.
        password: SecretRef,
    },
    /// A bearer token already in hand.
    Bearer {
        /// The token.
        token: SecretRef,
    },
    /// OAuth 2.0 password grant.
    OauthPassword {
        /// The token endpoint.
        token_url: String,
        /// The user.
        user: String,
        /// Its password.
        password: SecretRef,
        /// The client's id, for a public or confidential client.
        client_id: Option<String>,
        /// The client's secret, when it has one.
        client_secret: Option<SecretRef>,
        /// The scope to ask for.
        scope: Option<String>,
    },
    /// OAuth 2.0 client credentials.
    OauthClient {
        /// The token endpoint.
        token_url: String,
        /// The client's id.
        client_id: String,
        /// The client's secret.
        client_secret: SecretRef,
        /// The scope to ask for.
        scope: Option<String>,
    },
    /// Azure AD: client credentials, or with a `user` the password grant.
    Azure {
        /// The tenant (a GUID or a domain).
        tenant: String,
        /// The application's id.
        client_id: String,
        /// The scope, often `api://ID/.default`.
        scope: Option<String>,
        /// The client's secret (client credentials, optional for a password grant).
        client_secret: Option<SecretRef>,
        /// With `password`: the user of a password grant.
        user: Option<String>,
        /// The user's password.
        password: Option<SecretRef>,
    },
    /// AWS Cognito, `USER_PASSWORD_AUTH`.
    Cognito {
        /// The AWS region of the user pool.
        region: String,
        /// The app client's id.
        client_id: String,
        /// The user.
        user: String,
        /// Its password.
        password: SecretRef,
        /// The app client's secret, when it has one.
        client_secret: Option<SecretRef>,
    },
}

/// The non-secret fields of a method, and its secret references, in file order.
type Parts<'a> = (Vec<(&'static str, String)>, Vec<(&'static str, &'a SecretRef)>);

impl AuthSpec {
    /// The method's name, as `method = "..."` spells it.
    pub fn method(&self) -> &'static str {
        match self {
            AuthSpec::None => "none",
            AuthSpec::Basic { .. } => "basic",
            AuthSpec::Bearer { .. } => "bearer",
            AuthSpec::OauthPassword { .. } => "oauth-password",
            AuthSpec::OauthClient { .. } => "oauth-client",
            AuthSpec::Azure { .. } => "azure",
            AuthSpec::Cognito { .. } => "cognito",
        }
    }

    /// The non-secret fields and the secret references, in the order a file shows them.
    pub(crate) fn parts(&self) -> Parts<'_> {
        let mut plain: Vec<(&'static str, String)> = Vec::new();
        let mut refs: Vec<(&'static str, &SecretRef)> = Vec::new();
        fn text(plain: &mut Vec<(&'static str, String)>, key: &'static str, value: &Option<String>) {
            if let Some(value) = value {
                plain.push((key, value.clone()));
            }
        }
        fn secret<'a>(
            refs: &mut Vec<(&'static str, &'a SecretRef)>,
            key: &'static str,
            value: &'a Option<SecretRef>,
        ) {
            if let Some(value) = value {
                refs.push((key, value));
            }
        }
        match self {
            AuthSpec::None => {}
            AuthSpec::Basic { user, password } => {
                plain.push(("user", user.clone()));
                refs.push(("password", password));
            }
            AuthSpec::Bearer { token } => refs.push(("token", token)),
            AuthSpec::OauthPassword {
                token_url,
                user,
                password,
                client_id,
                client_secret,
                scope,
            } => {
                plain.push(("token_url", token_url.clone()));
                plain.push(("user", user.clone()));
                text(&mut plain, "client_id", client_id);
                text(&mut plain, "scope", scope);
                refs.push(("password", password));
                secret(&mut refs, "client_secret", client_secret);
            }
            AuthSpec::OauthClient {
                token_url,
                client_id,
                client_secret,
                scope,
            } => {
                plain.push(("token_url", token_url.clone()));
                plain.push(("client_id", client_id.clone()));
                text(&mut plain, "scope", scope);
                refs.push(("client_secret", client_secret));
            }
            AuthSpec::Azure {
                tenant,
                client_id,
                scope,
                client_secret,
                user,
                password,
            } => {
                plain.push(("tenant", tenant.clone()));
                plain.push(("client_id", client_id.clone()));
                text(&mut plain, "scope", scope);
                text(&mut plain, "user", user);
                secret(&mut refs, "client_secret", client_secret);
                secret(&mut refs, "password", password);
            }
            AuthSpec::Cognito {
                region,
                client_id,
                user,
                password,
                client_secret,
            } => {
                plain.push(("region", region.clone()));
                plain.push(("client_id", client_id.clone()));
                plain.push(("user", user.clone()));
                refs.push(("password", password));
                secret(&mut refs, "client_secret", client_secret);
            }
        }
        (plain, refs)
    }
}

/// The value of an extra request header: a literal for a header that carries
/// nothing secret, a reference for one that does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderValue {
    /// Written in `server.toml`. Refused for a header whose name says it carries a secret.
    Literal(String),
    /// Read at connection time.
    Secret(SecretRef),
}

/// An extra header sent with the requests to the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// The header's name.
    pub name: String,
    /// Its value, or where to read it.
    pub value: HeaderValue,
}

/// What a write needs that is not a secret: the `customData` of a dataspace
/// created on this server (an OSDU store asks for legal tags, owners and viewers).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteDefaults {
    /// Legal tags (`legaltags`).
    pub legal_tags: Vec<String>,
    /// Owners (`owners`).
    pub owners: Vec<String>,
    /// Viewers (`viewers`).
    pub viewers: Vec<String>,
}

impl WriteDefaults {
    /// Whether nothing is set.
    pub fn is_empty(&self) -> bool {
        self.legal_tags.is_empty() && self.owners.is_empty() && self.viewers.is_empty()
    }

    /// The `KEY=VALUE` entries of `create-dataspace --custom`, a list joined by commas.
    pub fn custom_entries(&self) -> Vec<(&'static str, String)> {
        [
            ("legaltags", &self.legal_tags),
            ("owners", &self.owners),
            ("viewers", &self.viewers),
        ]
        .into_iter()
        .filter(|(_, values)| !values.is_empty())
        .map(|(key, values)| (key, values.join(",")))
        .collect()
    }
}

/// A header whose name says it carries a secret: its value is a reference, never a literal.
fn sensitive_header(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    ["authorization", "cookie", "key", "token", "secret", "password"]
        .iter()
        .any(|word| lower.contains(word))
}

/// A catalogue entry: the contents of one `server.toml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Extra request headers.
    pub headers: Vec<Header>,
    /// What a write needs by default.
    pub defaults: WriteDefaults,
    /// A line about the server.
    pub description: Option<String>,
    /// Other names for it; they share the namespace of the names.
    pub aliases: Vec<String>,
    /// Where it is.
    pub server: ServerSpec,
    /// How to get in.
    pub auth: AuthSpec,
}

// ---- reading ---------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRoot {
    schema: Option<u32>,
    description: Option<String>,
    #[serde(default)]
    alias: Vec<String>,
    server: FileServer,
    auth: FileAuth,
    #[serde(default)]
    headers: std::collections::BTreeMap<String, FileHeader>,
    #[serde(default)]
    defaults: FileDefaults,
}

/// A header's value: a text, or a reference.
#[derive(Deserialize)]
#[serde(untagged)]
enum FileHeader {
    Text(String),
    Ref(FileRef),
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FileDefaults {
    #[serde(default)]
    legal_tags: Vec<String>,
    #[serde(default)]
    owners: Vec<String>,
    #[serde(default)]
    viewers: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileServer {
    etp_url: Option<String>,
    rest_url: Option<String>,
    transport: Option<String>,
    partition: Option<String>,
    tls: Option<String>,
    handshake: Option<String>,
    protected: Option<bool>,
    keepalive: Option<u64>,
    at_once: Option<usize>,
    #[serde(default)]
    types: Vec<String>,
    offer: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileAuth {
    method: String,
    tenant: Option<String>,
    client_id: Option<String>,
    scope: Option<String>,
    token_url: Option<String>,
    region: Option<String>,
    user: Option<String>,
    token: Option<FileRef>,
    password: Option<FileRef>,
    client_secret: Option<FileRef>,
}

/// `{ env = "NAME" }` or `{ file = "PATH" }`. The sources not available yet are
/// read (as any value) so that they can be refused by name.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRef {
    env: Option<String>,
    file: Option<String>,
    keyring: Option<toml::Value>,
    cmd: Option<toml::Value>,
    prompt: Option<toml::Value>,
}

impl FileRef {
    fn into_ref(self, key: &str) -> Result<SecretRef, Problem> {
        let present = [
            ("env", self.env.is_some()),
            ("file", self.file.is_some()),
            ("keyring", self.keyring.is_some()),
            ("cmd", self.cmd.is_some()),
            ("prompt", self.prompt.is_some()),
        ];
        let named: Vec<&str> = present
            .iter()
            .filter(|(_, set)| *set)
            .map(|(name, _)| *name)
            .collect();
        match named.as_slice() {
            ["env"] => {
                SecretRef::env_named(&self.env.unwrap_or_default()).map_err(|why| Problem::new(key, why))
            }
            ["file"] => {
                SecretRef::file_named(&self.file.unwrap_or_default()).map_err(|why| Problem::new(key, why))
            }
            [other @ ("keyring" | "cmd" | "prompt")] => Err(Problem::new(key, not_yet(other))),
            [] => Err(Problem::new(key, "a secret reference sets `env` or `file`")),
            _ => Err(Problem::new(key, "a secret reference sets exactly one source")),
        }
    }
}

/// The fields of an `[auth]` table (or of the command line), the references already read.
#[derive(Default)]
pub(crate) struct AuthFields {
    pub(crate) method: String,
    pub(crate) tenant: Option<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) scope: Option<String>,
    pub(crate) token_url: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) user: Option<String>,
    pub(crate) token: Option<SecretRef>,
    pub(crate) password: Option<SecretRef>,
    pub(crate) client_secret: Option<SecretRef>,
}

fn need<T>(value: Option<T>, method: &str, key: &str) -> Result<T, Problem> {
    value.ok_or_else(|| Problem::new(key, format!("the method `{method}` needs `{key}`")))
}

impl AuthFields {
    fn set(&self) -> Vec<&'static str> {
        [
            ("tenant", self.tenant.is_some()),
            ("client_id", self.client_id.is_some()),
            ("scope", self.scope.is_some()),
            ("token_url", self.token_url.is_some()),
            ("region", self.region.is_some()),
            ("user", self.user.is_some()),
            ("token", self.token.is_some()),
            ("password", self.password.is_some()),
            ("client_secret", self.client_secret.is_some()),
        ]
        .into_iter()
        .filter(|(_, set)| *set)
        .map(|(key, _)| key)
        .collect()
    }

    /// The method these fields spell, refusing a field the method does not use
    /// and a field it needs that is missing.
    pub(crate) fn build(self) -> Result<AuthSpec, Problem> {
        let method = self.method.clone();
        let allowed: &[&str] = match method.as_str() {
            "none" => &[],
            "basic" => &["user", "password"],
            "bearer" => &["token"],
            "oauth-password" => &[
                "token_url",
                "user",
                "password",
                "client_id",
                "client_secret",
                "scope",
            ],
            "oauth-client" => &["token_url", "client_id", "client_secret", "scope"],
            "azure" => &[
                "tenant",
                "client_id",
                "scope",
                "client_secret",
                "user",
                "password",
            ],
            "cognito" => &["region", "client_id", "user", "password", "client_secret"],
            "oauth-code-pkce" | "azure-device-code" => {
                return Err(Problem::new(
                    "method",
                    "this method is not available yet in this version of retp",
                ));
            }
            _ => {
                return Err(Problem::new(
                    "method",
                    "`method` takes one of: none, basic, bearer, oauth-password, oauth-client, azure, cognito",
                ));
            }
        };
        for key in self.set() {
            if !allowed.contains(&key) {
                return Err(Problem::new(
                    key,
                    format!("`{key}` is not used by the method `{method}`"),
                ));
            }
        }
        let m = method.as_str();
        Ok(match m {
            "basic" => AuthSpec::Basic {
                user: need(self.user, m, "user")?,
                password: need(self.password, m, "password")?,
            },
            "bearer" => AuthSpec::Bearer {
                token: need(self.token, m, "token")?,
            },
            "oauth-password" => AuthSpec::OauthPassword {
                token_url: need(self.token_url, m, "token_url")?,
                user: need(self.user, m, "user")?,
                password: need(self.password, m, "password")?,
                client_id: self.client_id,
                client_secret: self.client_secret,
                scope: self.scope,
            },
            "oauth-client" => AuthSpec::OauthClient {
                token_url: need(self.token_url, m, "token_url")?,
                client_id: need(self.client_id, m, "client_id")?,
                client_secret: need(self.client_secret, m, "client_secret")?,
                scope: self.scope,
            },
            "azure" => {
                let tenant = need(self.tenant, m, "tenant")?;
                let client_id = need(self.client_id, m, "client_id")?;
                match (&self.user, &self.password, &self.client_secret) {
                    (Some(_), None, _) => {
                        return Err(Problem::new("password", "a `user` needs its `password`"));
                    }
                    (None, Some(_), _) => {
                        return Err(Problem::new("password", "a `password` needs its `user`"));
                    }
                    (None, None, None) => {
                        return Err(Problem::new(
                            "client_secret",
                            "the method `azure` needs `client_secret` (client credentials), or `user` and `password`",
                        ));
                    }
                    _ => {}
                }
                AuthSpec::Azure {
                    tenant,
                    client_id,
                    scope: self.scope,
                    client_secret: self.client_secret,
                    user: self.user,
                    password: self.password,
                }
            }
            "cognito" => AuthSpec::Cognito {
                region: need(self.region, m, "region")?,
                client_id: need(self.client_id, m, "client_id")?,
                user: need(self.user, m, "user")?,
                password: need(self.password, m, "password")?,
                client_secret: self.client_secret,
            },
            _ => AuthSpec::None,
        })
    }
}

/// The line, from 1, of the first key `key` in `text`: `key = ...`, `key.x = ...` or a `[table.key]` header.
pub(crate) fn line_of(text: &str, key: &str) -> Option<usize> {
    let header_end = format!("{key}]");
    text.lines()
        .position(|line| {
            let trimmed = line.trim_start();
            let assigned = trimmed.strip_prefix(key).is_some_and(|rest| {
                let rest = rest.trim_start();
                rest.starts_with('=') || rest.starts_with('.')
            });
            assigned || (trimmed.starts_with('[') && trimmed.trim_end().ends_with(&header_end))
        })
        .map(|index| index + 1)
}

/// Blank what a TOML or serde message quotes: a value it saw in the source.
fn blank_quoted(text: &str) -> String {
    let mut out = String::new();
    let mut open: Option<char> = None;
    for c in text.chars() {
        match open {
            None => {
                out.push(c);
                if c == '"' || c == '`' {
                    open = Some(c);
                }
            }
            Some(quote) if c == quote => {
                out.push('…');
                out.push(c);
                open = None;
            }
            Some(_) => {}
        }
    }
    out
}

/// A parser's message with no value of the source in it. A field *name* is kept
/// (`unknown field `pasword``): it is a key, not a value.
fn scrub(message: &str) -> String {
    let first = message.lines().next().unwrap_or("");
    let keeps_names = ["unknown field", "missing field", "duplicate key"]
        .iter()
        .any(|start| first.starts_with(start));
    if keeps_names {
        return first.to_string();
    }
    let (head, tail) = match first.split_once(", expected") {
        Some((head, tail)) => (head, Some(tail)),
        None => (first, None),
    };
    let head = if head.starts_with("unknown variant") {
        "unknown value".to_string()
    } else {
        blank_quoted(head)
    };
    match tail {
        Some(tail) => format!("{head}, expected{}", blank_quoted(tail)),
        None => head,
    }
}

/// Parse a TOML text. The error holds a line and a message, **never the TOML
/// crate's own text**, which quotes the source.
pub(crate) fn read_toml<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, (Option<usize>, String)> {
    toml::from_str::<T>(text).map_err(|error| {
        let line = error
            .span()
            .and_then(|span| text.get(..span.start))
            .map(|before| before.matches('\n').count() + 1);
        (line, scrub(error.message()))
    })
}

/// The keys whose value must be a reference, never a literal.
const SECRET_KEYS: [&str; 4] = ["password", "token", "client_secret", "secret"];

/// Refuse a literal value for a secret key, before any parsing: the error
/// names the key and the line, and the value is not read into it.
pub(crate) fn scan_literals(text: &str) -> Result<(), CatalogError> {
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') || trimmed.starts_with('[') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        let key = key.trim().trim_matches(|c| c == '"' || c == '\'');
        let last = key.rsplit('.').next().unwrap_or(key);
        if SECRET_KEYS.contains(&last) && !value.trim_start().starts_with('{') {
            return Err(CatalogError::entry(
                Some(last),
                Some(index + 1),
                format!(
                    "`{last}` holds a literal value: a secret is never written in server.toml, only a reference such as `{last} = {{ env = \"NAME\" }}` or `{last} = {{ file = \"PATH\" }}`"
                ),
            ));
        }
    }
    Ok(())
}

fn check_url(key: &str, url: &str, schemes: &[&str]) -> Result<(), Problem> {
    if !schemes.iter().any(|scheme| url.starts_with(scheme)) {
        return Err(Problem::new(
            key,
            format!("`{key}` must start with {}", schemes.join(" or ")),
        ));
    }
    if has_credentials(url) {
        return Err(Problem::new(
            key,
            format!(
                "`{key}` carries credentials (user:password@host): a URL appears in error messages, so a secret never goes in it"
            ),
        ));
    }
    Ok(())
}

impl Entry {
    /// An entry with no description and no alias.
    pub fn new(server: ServerSpec, auth: AuthSpec) -> Self {
        Entry {
            headers: Vec::new(),
            defaults: WriteDefaults::default(),
            description: None,
            aliases: Vec::new(),
            server,
            auth,
        }
    }

    /// Read a `server.toml`.
    ///
    /// Refused, by key and line and never by value: a literal secret, an
    /// unknown key, a URL with credentials, a field the method does not use, a
    /// source of secrets that does not exist yet.
    pub fn parse(text: &str) -> Result<Entry, CatalogError> {
        scan_literals(text)?;
        let root: FileRoot = read_toml(text).map_err(|(line, why)| CatalogError::entry(None, line, why))?;
        let located = |problem: Problem| problem.located(Some(text));
        if root.schema != Some(1) {
            return Err(located(Problem::new(
                "schema",
                "`schema` is required and must be 1",
            )));
        }
        let server = server_spec(root.server).map_err(located)?;
        let auth = root.auth;
        let reference = |value: Option<FileRef>, key: &str| value.map(|r| r.into_ref(key)).transpose();
        let fields = AuthFields {
            method: auth.method,
            tenant: auth.tenant,
            client_id: auth.client_id,
            scope: auth.scope,
            token_url: auth.token_url,
            region: auth.region,
            user: auth.user,
            token: reference(auth.token, "token").map_err(located)?,
            password: reference(auth.password, "password").map_err(located)?,
            client_secret: reference(auth.client_secret, "client_secret").map_err(located)?,
        };
        let mut headers = Vec::new();
        for (name, value) in root.headers {
            let value = match value {
                FileHeader::Text(text) => HeaderValue::Literal(text),
                FileHeader::Ref(reference) => {
                    HeaderValue::Secret(reference.into_ref(&name).map_err(located)?)
                }
            };
            headers.push(Header { name, value });
        }
        let entry = Entry {
            headers,
            defaults: WriteDefaults {
                legal_tags: root.defaults.legal_tags,
                owners: root.defaults.owners,
                viewers: root.defaults.viewers,
            },
            description: root.description,
            aliases: root.alias,
            server,
            auth: fields.build().map_err(located)?,
        };
        entry.validate().map_err(located)?;
        Ok(entry)
    }

    /// The cross-field rules: URLs, aliases, the transport's URL, the tenant and the region.
    pub(crate) fn validate(&self) -> Result<(), Problem> {
        let server = &self.server;
        if server.etp_url.is_none() && server.rest_url.is_none() {
            return Err(Problem::new(
                "etp_url",
                "a server needs an `etp_url` or a `rest_url`",
            ));
        }
        if let Some(url) = &server.etp_url {
            check_url("etp_url", url, &["ws://", "wss://"])?;
        }
        if let Some(url) = &server.rest_url {
            check_url("rest_url", url, &["http://", "https://"])?;
        }
        match server.transport {
            Transport::Etp if server.etp_url.is_none() => {
                return Err(Problem::new(
                    "transport",
                    "the transport `etp` needs an `etp_url`",
                ));
            }
            Transport::Rest if server.rest_url.is_none() => {
                return Err(Problem::new(
                    "transport",
                    "the transport `rest` needs a `rest_url`",
                ));
            }
            _ => {}
        }
        if server.partition.as_deref() == Some("") {
            return Err(Problem::new("partition", "`partition` cannot be empty"));
        }
        for (index, alias) in self.aliases.iter().enumerate() {
            check_name(alias).map_err(|why| Problem::new("alias", why))?;
            if self.aliases[..index].contains(alias) {
                return Err(Problem::new("alias", "an alias is listed twice"));
            }
        }
        for (index, header) in self.headers.iter().enumerate() {
            let name = header.name.as_str();
            let token = !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'));
            if !token {
                return Err(Problem::new(
                    "headers",
                    "a header name is letters, digits, `-` and `_`",
                ));
            }
            if self.headers[..index]
                .iter()
                .any(|other| other.name.eq_ignore_ascii_case(name))
            {
                return Err(Problem::new(name, "a header is listed twice"));
            }
            if matches!(header.value, HeaderValue::Literal(_)) && sensitive_header(name) {
                return Err(Problem::new(
                    name,
                    "this header looks like it carries a secret by its name: give a reference ({ env = \"NAME\" } or { file = \"PATH\" }), not a literal",
                ));
            }
        }
        match &self.auth {
            AuthSpec::OauthPassword { token_url, .. } | AuthSpec::OauthClient { token_url, .. } => {
                check_url("token_url", token_url, &["http://", "https://"])?;
            }
            AuthSpec::Azure { tenant, .. } => {
                if retp_client::login::azure_token_url(tenant).is_err() {
                    return Err(Problem::new(
                        "tenant",
                        "`tenant` must be an Azure AD tenant: a GUID or a domain",
                    ));
                }
            }
            AuthSpec::Cognito { region, .. } => {
                let valid = !region.is_empty()
                    && region
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
                if !valid {
                    return Err(Problem::new(
                        "region",
                        "`region` must be an AWS region such as `us-east-2`",
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// The text of a `server.toml` for this entry.
    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str(
            "# A retp server. No secret belongs in this file: a secret is a reference, such as { env = \"NAME\" }.\n",
        );
        out.push_str("schema = 1\n");
        if let Some(description) = &self.description {
            let _ = writeln!(out, "description = {}", quote(description));
        }
        if !self.aliases.is_empty() {
            let list: Vec<String> = self.aliases.iter().map(|alias| quote(alias)).collect();
            let _ = writeln!(out, "alias = [{}]", list.join(", "));
        }
        let server = &self.server;
        out.push_str("\n[server]\n");
        if let Some(url) = &server.etp_url {
            let _ = writeln!(out, "etp_url = {}", quote(url));
        }
        if let Some(url) = &server.rest_url {
            let _ = writeln!(out, "rest_url = {}", quote(url));
        }
        let _ = writeln!(out, "transport = {}", quote(server.transport.as_str()));
        if let Some(partition) = &server.partition {
            let _ = writeln!(out, "partition = {}", quote(partition));
        }
        let tls = match server.tls {
            Tls::Verify => "verify",
            Tls::Skip => "skip",
        };
        let _ = writeln!(out, "tls = {}", quote(tls));
        let handshake = match server.handshake {
            Handshake::Native => "native",
            Handshake::Browser => "browser",
        };
        let _ = writeln!(out, "handshake = {}", quote(handshake));
        let _ = writeln!(out, "protected = {}", server.protected);
        if let Some(seconds) = server.keepalive {
            let _ = writeln!(out, "keepalive = {seconds}");
        }
        if let Some(count) = server.at_once {
            let _ = writeln!(out, "at_once = {count}");
        }
        if !server.types.is_empty() {
            let list: Vec<String> = server.types.iter().map(|name| quote(name)).collect();
            let _ = writeln!(out, "types = [{}]", list.join(", "));
        }
        if let Some(offer) = &server.offer {
            let _ = writeln!(out, "offer = {}", quote(offer));
        }
        out.push_str("\n[auth]\n");
        let _ = writeln!(out, "method = {}", quote(self.auth.method()));
        let (plain, refs) = self.auth.parts();
        for (key, value) in plain {
            let _ = writeln!(out, "{key} = {}", quote(&value));
        }
        for (key, reference) in refs {
            let _ = writeln!(out, "{key} = {}", reference.to_toml());
        }
        if !self.headers.is_empty() {
            out.push_str("\n[headers]\n");
            for header in &self.headers {
                let value = match &header.value {
                    HeaderValue::Literal(text) => quote(text),
                    HeaderValue::Secret(reference) => reference.to_toml(),
                };
                let _ = writeln!(out, "{} = {value}", quote(&header.name));
            }
        }
        if !self.defaults.is_empty() {
            out.push_str("\n[defaults]\n");
            for (key, values) in [
                ("legal_tags", &self.defaults.legal_tags),
                ("owners", &self.defaults.owners),
                ("viewers", &self.defaults.viewers),
            ] {
                if !values.is_empty() {
                    let list: Vec<String> = values.iter().map(|v| quote(v)).collect();
                    let _ = writeln!(out, "{key} = [{}]", list.join(", "));
                }
            }
        }
        out
    }
}

fn server_spec(file: FileServer) -> Result<ServerSpec, Problem> {
    let transport = match file.transport.as_deref() {
        Some(text) => Transport::parse(text)
            .ok_or_else(|| Problem::new("transport", "`transport` takes `etp` or `rest`"))?,
        None if file.etp_url.is_some() => Transport::Etp,
        None => Transport::Rest,
    };
    let tls = match file.tls.as_deref() {
        None | Some("verify") => Tls::Verify,
        Some("skip") => Tls::Skip,
        Some(_) => return Err(Problem::new("tls", "`tls` takes `verify` or `skip`")),
    };
    let handshake = match file.handshake.as_deref() {
        None | Some("native") => Handshake::Native,
        Some("browser") => Handshake::Browser,
        Some(_) => {
            return Err(Problem::new(
                "handshake",
                "`handshake` takes `native` or `browser`",
            ));
        }
    };
    Ok(ServerSpec {
        etp_url: file.etp_url,
        rest_url: file.rest_url,
        transport,
        partition: file.partition,
        tls,
        handshake,
        protected: file.protected.unwrap_or(false),
        keepalive: file.keepalive,
        at_once: file.at_once,
        types: file.types,
        offer: file.offer,
    })
}

// ---- building from a command line -----------------------------------------

/// What `retp server add` gathers: loose text, turned into an [`Entry`] by the
/// same rules a `server.toml` is read with.
#[derive(Debug, Clone, Default)]
pub struct Draft {
    /// A server URL; its scheme says whether it is the ETP or the REST one.
    pub url: String,
    /// The URL of the other transport, when the server has both.
    pub other_url: Option<String>,
    /// `etp` or `rest`; the first URL's by default.
    pub transport: Option<String>,
    /// An OSDU data partition.
    pub partition: Option<String>,
    /// Do not check the certificate.
    pub insecure: bool,
    /// Connect as a web page can.
    pub as_browser: bool,
    /// Ask for a confirmation before writing to this server.
    pub protected: bool,
    /// A line about the server.
    pub description: Option<String>,
    /// Other names.
    pub aliases: Vec<String>,
    /// The method; `none` when no auth field is given.
    pub method: Option<String>,
    /// `tenant`.
    pub tenant: Option<String>,
    /// `client_id`.
    pub client_id: Option<String>,
    /// `scope`.
    pub scope: Option<String>,
    /// `token_url`.
    pub token_url: Option<String>,
    /// `region`.
    pub region: Option<String>,
    /// `user`.
    pub user: Option<String>,
    /// A reference (`env:NAME`, `file:PATH`) to the token.
    pub token_from: Option<String>,
    /// A reference to the password.
    pub password_from: Option<String>,
    /// A reference to the client's secret.
    pub client_secret_from: Option<String>,
    /// Extra headers with a literal value (name, value); refused for a name that looks secret.
    pub headers: Vec<(String, String)>,
    /// Extra headers read from a reference (name, `env:NAME` or `file:PATH`).
    pub headers_from: Vec<(String, String)>,
    /// Legal tags of a dataspace this server creates.
    pub legal_tags: Vec<String>,
    /// Owners of a dataspace this server creates.
    pub owners: Vec<String>,
    /// Viewers of a dataspace this server creates.
    pub viewers: Vec<String>,
}

fn kind_of(url: &str) -> Option<Transport> {
    if url.starts_with("ws://") || url.starts_with("wss://") {
        Some(Transport::Etp)
    } else if url.starts_with("http://") || url.starts_with("https://") {
        Some(Transport::Rest)
    } else {
        None
    }
}

impl Draft {
    /// The entry this draft describes, or the first rule it breaks.
    pub fn into_entry(self) -> Result<Entry, CatalogError> {
        let fail = |key: &str, why: &str| CatalogError::entry(Some(key), None, why);
        let mut etp_url = None;
        let mut rest_url = None;
        let mut first = None;
        for url in std::iter::once(&self.url).chain(self.other_url.iter()) {
            let kind = kind_of(url).ok_or_else(|| {
                fail(
                    "url",
                    "a server URL starts with ws://, wss://, http:// or https:// (the text given is not shown)",
                )
            })?;
            first.get_or_insert(kind);
            let slot = match kind {
                Transport::Etp => &mut etp_url,
                Transport::Rest => &mut rest_url,
            };
            if slot.is_some() {
                return Err(fail("url", "two URLs of the same transport were given"));
            }
            *slot = Some(url.clone());
        }
        let transport = match self.transport.as_deref() {
            Some(text) => Transport::parse(text)
                .ok_or_else(|| fail("transport", "`transport` takes `etp` or `rest`"))?,
            None => first.unwrap_or(Transport::Etp),
        };
        let reference = |text: &Option<String>, key: &str| -> Result<Option<SecretRef>, CatalogError> {
            text.as_deref()
                .map(|text| {
                    SecretRef::parse(text).map_err(|error| match error {
                        CatalogError::Reference { why } => CatalogError::entry(Some(key), None, why),
                        other => other,
                    })
                })
                .transpose()
        };
        let any_auth_field = [
            &self.tenant,
            &self.client_id,
            &self.scope,
            &self.token_url,
            &self.region,
            &self.user,
            &self.token_from,
            &self.password_from,
            &self.client_secret_from,
        ]
        .iter()
        .any(|field| field.is_some());
        let method = match (&self.method, any_auth_field) {
            (Some(method), _) => method.clone(),
            (None, false) => "none".to_string(),
            (None, true) => {
                return Err(fail(
                    "method",
                    "give the login method: --auth none|basic|bearer|oauth-password|oauth-client|azure|cognito",
                ));
            }
        };
        let fields = AuthFields {
            method,
            tenant: self.tenant.clone(),
            client_id: self.client_id.clone(),
            scope: self.scope.clone(),
            token_url: self.token_url.clone(),
            region: self.region.clone(),
            user: self.user.clone(),
            token: reference(&self.token_from, "token")?,
            password: reference(&self.password_from, "password")?,
            client_secret: reference(&self.client_secret_from, "client_secret")?,
        };
        let server = ServerSpec {
            etp_url,
            rest_url,
            transport,
            partition: self.partition,
            tls: if self.insecure { Tls::Skip } else { Tls::Verify },
            handshake: if self.as_browser {
                Handshake::Browser
            } else {
                Handshake::Native
            },
            protected: self.protected,
            keepalive: None,
            at_once: None,
            types: Vec::new(),
            offer: None,
        };
        let mut headers: Vec<Header> = self
            .headers
            .into_iter()
            .map(|(name, text)| Header {
                name,
                value: HeaderValue::Literal(text),
            })
            .collect();
        for (name, text) in self.headers_from {
            let value = SecretRef::parse(&text).map_err(|error| match error {
                CatalogError::Reference { why } => CatalogError::entry(Some(&name), None, why),
                other => other,
            })?;
            headers.push(Header {
                name,
                value: HeaderValue::Secret(value),
            });
        }
        let entry = Entry {
            headers,
            defaults: WriteDefaults {
                legal_tags: self.legal_tags,
                owners: self.owners,
                viewers: self.viewers,
            },
            description: self.description,
            aliases: self.aliases,
            server,
            auth: fields.build().map_err(|p| p.located(None))?,
        };
        entry.validate().map_err(|p| p.located(None))?;
        Ok(entry)
    }
}
