//! From an entry to what the client connects with.
//!
//! **One door.** The secrets an entry points at are read, put in the `Choice`
//! of `retp-client`, and `Choice::login` decides the login, as it does for the
//! command line. The catalogue never decides the order of the methods; it only
//! checks that the method an entry declares is the one `Choice::login` gives.
//! The ambient `ETP_*` variables are not consulted (`|_| None`): the `ETP_TOKEN`
//! set for one server would otherwise go to another.

use std::time::Duration;

use retp_client::login::{Choice, Login};
use retp_client::native::{CertificateCheck, Endpoint};
use retp_client::{ExtraHeaders, HeaderRefusal, plain_text_warning};
use retp_proto::credentials::Credentials;
use retp_proto::probe::Profile;

use crate::entry::{AuthSpec, Entry, Handshake, HeaderValue, Tls, Transport};
use crate::error::CatalogError;
use crate::secret::{RefStatus, Secret, SecretRef, SecretSource};

/// What the command line decided, which wins over the entry: the URL it ended
/// up with (the catalogue's own expansion put it there), the transport, the
/// two flags. Its headers do not win, they are **added**: one
/// the entry already defines is refused, by name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Overrides {
    /// The URL to connect to. It must be one of the entry's.
    pub url: Option<String>,
    /// REST rather than ETP.
    pub rest: Option<bool>,
    /// Headers to send in addition to the entry's.
    pub headers: ExtraHeaders,
    /// Do not check the certificate.
    pub insecure: bool,
    /// Connect as a web page can.
    pub as_browser: bool,
}

/// An entry with its secrets read, ready to connect. No `Debug` of the values.
pub struct Resolved {
    name: String,
    url: String,
    rest: bool,
    certificates: CertificateCheck,
    browser: bool,
    protected: bool,
    login: Option<Login>,
    headers: ExtraHeaders,
    held: Vec<Secret>,
}

impl std::fmt::Debug for Resolved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Resolved")
            .field("name", &self.name)
            .field("url", &self.url)
            .field("rest", &self.rest)
            .field("login", &self.login.as_ref().map(|_| "…"))
            .finish_non_exhaustive()
    }
}

/// An endpoint, and what to tell the person about it.
pub struct Opened {
    /// The server, with the credentials obtained.
    pub endpoint: Endpoint,
    /// Lines for stderr, already worded: `warning: …` and `credentials: …`.
    pub notes: Vec<String>,
    /// How long the credentials last, from now, when the authorization server
    /// said (`expires_in`): the number the `credentials: … for N s` line
    /// words, given back as a number so that whoever renews them can plan it.
    /// `None` when there was no login, or the answer carried none.
    pub expires_in: Option<Duration>,
}

/// The one place a secret's value leaves its `Secret`: where the login is built.
fn reveal(secret: &Secret) -> String {
    secret.expose().to_string()
}

fn consistent(declared: &str, login: &Option<Login>) -> bool {
    matches!(
        (declared, login),
        ("none", None)
            | ("basic", Some(Login::Basic { .. }))
            | ("bearer", Some(Login::Bearer(_)))
            | ("oauth-password", Some(Login::Password { .. }))
            | ("oauth-client", Some(Login::ClientCredentials { .. }))
            | (
                "azure",
                Some(Login::ClientCredentials { .. } | Login::Password { .. })
            )
            | ("cognito", Some(Login::Cognito { .. }))
    )
}

fn inferred(login: &Option<Login>) -> &'static str {
    match login {
        None => "none",
        Some(Login::Basic { .. }) => "basic",
        Some(Login::Bearer(_)) => "bearer",
        Some(Login::Password { .. }) => "a password grant",
        Some(Login::ClientCredentials { .. }) => "client credentials",
        Some(Login::Cognito { .. }) => "cognito",
    }
}

impl Entry {
    /// Read the secrets and build the login.
    ///
    /// `name` is the server's name, for the messages. `overrides` is what the
    /// command line settled; a URL that is not one of the entry's is refused,
    /// since the credentials go to the entry's own URLs only.
    pub fn resolve(
        &self,
        name: &str,
        secrets: &dyn SecretSource,
        overrides: &Overrides,
    ) -> Result<Resolved, CatalogError> {
        let server = &self.server;
        let rest = overrides.rest.unwrap_or(server.transport == Transport::Rest);
        let wanted = if rest { &server.rest_url } else { &server.etp_url };
        let url = match (&overrides.url, wanted) {
            (Some(given), _) => {
                let known = [&server.etp_url, &server.rest_url]
                    .into_iter()
                    .flatten()
                    .any(|own| own == given);
                if !known {
                    return Err(CatalogError::ForeignUrl {
                        entry: name.to_string(),
                    });
                }
                given.clone()
            }
            (None, Some(own)) => own.clone(),
            (None, None) => {
                return Err(CatalogError::NoUrl {
                    entry: name.to_string(),
                    wanted: if rest { "REST" } else { "ETP" }.to_string(),
                });
            }
        };
        tracing::debug!(server = %name, method = %self.auth.method(), "catalog: resolving `{name}` (method {})", self.auth.method());

        let mut held: Vec<Secret> = Vec::new();
        let mut read = |field: &str, reference: &SecretRef| -> Result<String, CatalogError> {
            let secret = secrets.read(reference).map_err(|error| CatalogError::Secret {
                entry: name.to_string(),
                field: field.to_string(),
                error,
            })?;
            let value = reveal(&secret);
            held.push(secret);
            Ok(value)
        };
        let mut choice = Choice::default();
        match &self.auth {
            AuthSpec::None => {}
            AuthSpec::Basic { user, password } => {
                choice.user = Some(user.clone());
                choice.password = Some(read("password", password)?);
            }
            AuthSpec::Bearer { token } => {
                choice.token = Some(read("token", token)?);
            }
            AuthSpec::OauthPassword {
                token_url,
                user,
                password,
                client_id,
                client_secret,
                scope,
            } => {
                choice.token_url = Some(token_url.clone());
                choice.user = Some(user.clone());
                choice.password = Some(read("password", password)?);
                choice.client_id = client_id.clone();
                if let Some(reference) = client_secret {
                    choice.client_secret = Some(read("client_secret", reference)?);
                }
                choice.oauth_scope = scope.clone();
            }
            AuthSpec::OauthClient {
                token_url,
                client_id,
                client_secret,
                scope,
            } => {
                choice.token_url = Some(token_url.clone());
                choice.client_id = Some(client_id.clone());
                choice.client_secret = Some(read("client_secret", client_secret)?);
                choice.oauth_scope = scope.clone();
            }
            AuthSpec::Azure {
                tenant,
                client_id,
                scope,
                client_secret,
                user,
                password,
            } => {
                choice.azure_tenant = Some(tenant.clone());
                choice.client_id = Some(client_id.clone());
                choice.oauth_scope = scope.clone();
                choice.user = user.clone();
                if let Some(reference) = client_secret {
                    choice.client_secret = Some(read("client_secret", reference)?);
                }
                if let Some(reference) = password {
                    choice.password = Some(read("password", reference)?);
                }
            }
            AuthSpec::Cognito {
                region,
                client_id,
                user,
                password,
                client_secret,
            } => {
                choice.cognito_region = Some(region.clone());
                choice.client_id = Some(client_id.clone());
                choice.user = Some(user.clone());
                choice.password = Some(read("password", password)?);
                if let Some(reference) = client_secret {
                    choice.client_secret = Some(read("client_secret", reference)?);
                }
            }
        }
        // The entry's headers, then the command line's: one list, each header
        // checked by the client's own door. A value read from a reference is
        // a secret whatever the header is called.
        let unsendable = |why: HeaderRefusal| CatalogError::refused(format!("server `{name}`: {why}"));
        let mut headers = ExtraHeaders::new();
        for header in &self.headers {
            headers = match &header.value {
                HeaderValue::Literal(text) => headers.with(header.name.as_str(), text.as_str()),
                HeaderValue::Secret(reference) => {
                    let value = read(&format!("header {}", header.name), reference)?;
                    headers.with_secret(header.name.as_str(), value)
                }
            }
            .map_err(unsendable)?;
        }
        let headers = headers.and(&overrides.headers).map_err(unsendable)?;

        let redact = |text: &str| {
            held.iter()
                .fold(text.to_string(), |text, secret| secret.redact_in(&text))
        };
        let login = choice.login(|_| None).map_err(|error| CatalogError::Login {
            entry: name.to_string(),
            message: redact(&error.to_string()),
        })?;
        let declared = self.auth.method();
        if !consistent(declared, &login) {
            return Err(CatalogError::Inconsistent {
                entry: name.to_string(),
                declared: declared.to_string(),
                inferred: inferred(&login).to_string(),
            });
        }
        let resolved = Resolved {
            name: name.to_string(),
            url,
            rest,
            certificates: if overrides.insecure || server.tls == Tls::Skip {
                CertificateCheck::Skip
            } else {
                CertificateCheck::Verify
            },
            browser: overrides.as_browser || server.handshake == Handshake::Browser,
            protected: server.protected,
            login,
            headers,
            held,
        };
        resolved.sendable().map_err(unsendable)?;
        Ok(resolved)
    }
}

impl Resolved {
    /// The server's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The URL to connect to.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Whether the transport is REST.
    pub fn rest(&self) -> bool {
        self.rest
    }

    /// Whether writes to this server ask for a confirmation.
    pub fn protected(&self) -> bool {
        self.protected
    }

    /// The login, when the entry has one. `retp-conformance` obtains it itself,
    /// to report a refusal as a finding.
    pub fn login(&self) -> Option<&Login> {
        self.login.as_ref()
    }

    /// The extra headers of the entry, their references read, and the ones
    /// the command line added. The endpoint carries them
    /// ([`Resolved::endpoint`]).
    pub fn headers(&self) -> &ExtraHeaders {
        &self.headers
    }

    /// What a connection would refuse, said as soon as the entry is resolved:
    /// a header that carries a secret when the session is opened as a front
    /// end opens it - its attributes go in the URL. REST always sends
    /// headers.
    fn sendable(&self) -> Result<(), HeaderRefusal> {
        if self.rest {
            return Ok(());
        }
        self.endpoint(None).sendable()
    }

    /// `text` with every literal occurrence of this entry's secrets removed: a
    /// last-resort guard for what a server's answer may repeat, not a defence.
    pub fn redact(&self, text: &str) -> String {
        self.held
            .iter()
            .fold(text.to_string(), |text, secret| secret.redact_in(&text))
    }

    /// The endpoint with the credentials given: no network.
    pub fn endpoint(&self, credentials: Option<Credentials>) -> Endpoint {
        Endpoint {
            url: self.url.clone(),
            headers: self.headers.clone(),
            credentials,
            certificates: self.certificates,
            handshake: if self.browser {
                Profile::Browser
            } else {
                Profile::Native
            },
        }
    }

    /// Get the credentials (one request to the authorization server when the
    /// method needs one) and build the endpoint, with the warnings the command
    /// line prints for a plain-text or unchecked connection. The entry's extra
    /// headers are on the endpoint, which sends them with every request.
    pub fn obtain_endpoint(&self) -> Result<Opened, CatalogError> {
        let mut notes = Vec::new();
        for warning in [
            plain_text_warning(&self.url),
            self.certificates.warning(&self.url),
        ]
        .into_iter()
        .flatten()
        {
            notes.push(format!("warning: {warning}"));
        }
        let mut expires_in = None;
        let credentials = match &self.login {
            Some(login) => {
                tracing::debug!(server = %self.name, "catalog: logging in to `{}`", self.name);
                let obtained = login.obtain().map_err(|error| CatalogError::Login {
                    entry: self.name.clone(),
                    message: self.redact(&error.to_string()),
                })?;
                let lasts: Option<Duration> = obtained.expires_in;
                expires_in = lasts;
                notes.push(match lasts {
                    Some(lasts) => format!(
                        "credentials: {}, for {} s",
                        obtained.credentials.scheme(),
                        lasts.as_secs()
                    ),
                    None => format!("credentials: {}", obtained.credentials.scheme()),
                });
                Some(obtained.credentials)
            }
            None => None,
        };
        Ok(Opened {
            endpoint: self.endpoint(credentials),
            notes,
            expires_in,
        })
    }
}

/// What `server show` prints: references and whether they resolve, never a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Description {
    /// The name.
    pub name: String,
    /// The aliases.
    pub aliases: Vec<String>,
    /// The description line.
    pub description: Option<String>,
    /// The ETP URL.
    pub etp_url: Option<String>,
    /// The REST URL.
    pub rest_url: Option<String>,
    /// The default transport.
    pub transport: Transport,
    /// Whether the certificate is checked.
    pub tls: Tls,
    /// The handshake.
    pub handshake: Handshake,
    /// Whether writes ask for a confirmation.
    pub protected: bool,
    /// The login method.
    pub method: &'static str,
    /// The non-secret fields of the method.
    pub fields: Vec<(&'static str, String)>,
    /// The secret references and their status.
    pub references: Vec<ReferenceLine>,
    /// The extra headers: name and, for a reference, its status; a literal's value is not shown.
    pub headers: Vec<(String, String)>,
    /// What a write needs by default, as `KEY=VALUE`.
    pub defaults: Vec<(&'static str, String)>,
}

/// One secret reference of an entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceLine {
    /// The field that points at it (`client_secret`…).
    pub field: String,
    /// The reference, as the command line writes it.
    pub reference: String,
    /// Whether it resolves.
    pub status: RefStatus,
}

impl Entry {
    /// Describe the entry. The references are inspected, not read into a value that is kept.
    pub fn describe(&self, name: &str, secrets: &dyn SecretSource) -> Description {
        let (fields, refs) = self.auth.parts();
        let mut references: Vec<ReferenceLine> = refs
            .into_iter()
            .map(|(field, reference)| ReferenceLine {
                field: field.to_string(),
                reference: reference.to_text(),
                status: secrets.inspect(reference),
            })
            .collect();
        let mut headers = Vec::new();
        for header in &self.headers {
            match &header.value {
                HeaderValue::Literal(_) => {
                    headers.push((header.name.clone(), "literal (not shown)".to_string()))
                }
                HeaderValue::Secret(reference) => {
                    let status = secrets.inspect(reference);
                    headers.push((
                        header.name.clone(),
                        format!("{} ({})", reference.to_text(), status.detail),
                    ));
                    references.push(ReferenceLine {
                        field: format!("header {}", header.name),
                        reference: reference.to_text(),
                        status,
                    });
                }
            }
        }
        Description {
            name: name.to_string(),
            aliases: self.aliases.clone(),
            description: self.description.clone(),
            etp_url: self.server.etp_url.clone(),
            rest_url: self.server.rest_url.clone(),
            transport: self.server.transport,
            tls: self.server.tls,
            handshake: self.server.handshake,
            protected: self.server.protected,
            method: self.auth.method(),
            fields,
            references,
            headers,
            defaults: self.defaults.custom_entries(),
        }
    }
}
