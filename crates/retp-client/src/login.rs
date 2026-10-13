//! Getting credentials before connecting (§4.1.2, p.94).
//!
//! ETP leaves *how* a client obtains its token to the authorization server,
//! and servers in the field send clients to three kinds of places: nowhere (a
//! token handed over, or HTTP Basic), an OAuth 2.0 token endpoint with client
//! credentials - the grant §4.1.1 §1.e makes every authorization server
//! support, and the one Azure AD gives - and AWS Cognito, which
//! OSDU on AWS uses and which speaks its own API rather than OAuth's.
//!
//! The exchanges go over verified TLS, always: `--insecure` is for an ETP
//! server whose certificate is wrong, never for the service that hands out
//! the tokens. What can be checked without a network - the header an OAuth
//! client authenticates with, Cognito's `SECRET_HASH`, how each answer is
//! read - is a plain function, tested against the RFCs' own examples.

use std::fmt;
use std::time::Duration;

use base64::Engine;
use retp_proto::credentials::{Credentials, CredentialsRefusal};
use serde_json::Value as Json;

use crate::native::{CertificateCheck, http_agent};

/// How the client proves itself. `Debug` is not derived: every variant but
/// the first holds a secret.
#[derive(Clone)]
pub enum Login {
    /// A token already in hand.
    Bearer(String),
    /// HTTP Basic (RFC 7617) - no longer recommended by ETP v1.2 (§4.1.1),
    /// and what some servers still take.
    Basic {
        /// The user-id.
        user: String,
        /// The password.
        password: String,
    },
    /// OAuth 2.0 client credentials (RFC 6749 §4.4) at `token_url`, the
    /// client authenticated by `client_secret_basic` - the method §4.1.1
    /// §1.d makes every authorization server support.
    ClientCredentials {
        /// The token endpoint; [`azure_token_url`] gives Azure AD's.
        token_url: String,
        /// The client's id.
        client_id: String,
        /// The client's secret.
        client_secret: String,
        /// The scope to ask for, if the server needs one.
        scope: Option<String>,
    },
    /// OAuth 2.0 resource owner password credentials (RFC 6749 §4.3) at
    /// `token_url`: a user and a password exchanged for a token - what
    /// servers whose users log in by name hand out (a vendor's `/token`
    /// route, an RDDMS REST API). The client authenticates by `client_secret_basic`
    /// when it has a secret, names itself in the form when it has only an
    /// id (RFC 6749 §3.2.1, a public client), and not at all otherwise.
    Password {
        /// The token endpoint.
        token_url: String,
        /// The user.
        user: String,
        /// The password.
        password: String,
        /// The client's id, when the server wants one.
        client_id: Option<String>,
        /// The client's secret, when it has one.
        client_secret: Option<String>,
        /// The scope to ask for, if the server needs one.
        scope: Option<String>,
    },
    /// AWS Cognito's `USER_PASSWORD_AUTH` flow; the access token it returns.
    Cognito {
        /// The AWS region of the user pool (`us-east-2`…).
        region: String,
        /// The app client's id.
        client_id: String,
        /// The app client's secret, if it has one.
        client_secret: Option<String>,
        /// The user.
        user: String,
        /// The password.
        password: String,
    },
}

/// Credentials, and how long they last when the server said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Obtained {
    /// What to send.
    pub credentials: Credentials,
    /// `expires_in`, when the answer carried it.
    pub expires_in: Option<Duration>,
}

/// Why no credentials came out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoginError {
    /// What was given cannot make credentials.
    Invalid(String),
    /// The authorization server could not be reached, or answered something
    /// that is not an answer.
    Http(String),
    /// It answered, and said no.
    Refused {
        /// Which service.
        server: String,
        /// The HTTP status.
        status: u16,
        /// Its error code (`invalid_client`, `NotAuthorizedException`…).
        error: String,
        /// Its explanation.
        description: String,
    },
    /// Cognito wants a step this client does not take - a new password, a
    /// second factor. Named, since passing over it would look like a refusal
    /// of the password.
    Challenge {
        /// `NEW_PASSWORD_REQUIRED`, `SOFTWARE_TOKEN_MFA`…
        name: String,
    },
}

impl fmt::Display for LoginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoginError::Invalid(why) => write!(f, "{why}"),
            LoginError::Http(why) => write!(f, "{why}"),
            LoginError::Refused {
                server,
                status,
                error,
                description,
            } => {
                write!(f, "{server} refused (HTTP {status}): {error}")?;
                if !description.is_empty() {
                    write!(f, " - {description}")?;
                }
                Ok(())
            }
            LoginError::Challenge { name } => write!(
                f,
                "Cognito asks for `{name}` before it gives a token, which this client does not answer; \
                 settle it once in another client, then come back"
            ),
        }
    }
}

impl std::error::Error for LoginError {}

impl From<CredentialsRefusal> for LoginError {
    fn from(why: CredentialsRefusal) -> Self {
        LoginError::Invalid(why.0)
    }
}

/// Azure AD's token endpoint (v2.0) for `tenant` - a GUID or a domain; the
/// tenant is part of the host path, so anything else is refused.
pub fn azure_token_url(tenant: &str) -> Result<String, LoginError> {
    if tenant.is_empty()
        || !tenant
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.'))
    {
        return Err(LoginError::Invalid(format!(
            "`{tenant}` is not an Azure AD tenant (a GUID or a domain)"
        )));
    }
    Ok(format!(
        "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    ))
}

impl Login {
    /// The credentials this login gives: at once for a token or Basic, after
    /// one request to the authorization server otherwise.
    pub fn obtain(&self) -> Result<Obtained, LoginError> {
        match self {
            Login::Bearer(token) => Ok(Obtained {
                credentials: Credentials::bearer(token)?,
                expires_in: None,
            }),
            Login::Basic { user, password } => Ok(Obtained {
                credentials: Credentials::basic(user, password)?,
                expires_in: None,
            }),
            Login::ClientCredentials {
                token_url,
                client_id,
                client_secret,
                scope,
            } => {
                let mut form = vec![("grant_type", "client_credentials".to_string())];
                if let Some(scope) = scope {
                    form.push(("scope", scope.clone()));
                }
                token_request(token_url, form, Some((client_id, client_secret)))
            }
            Login::Password {
                token_url,
                user,
                password,
                client_id,
                client_secret,
                scope,
            } => {
                let form = password_grant_form(
                    user,
                    password,
                    client_id.as_deref(),
                    client_secret.is_some(),
                    scope.as_deref(),
                );
                let client = match (client_id, client_secret) {
                    (Some(id), Some(secret)) => Some((id, secret)),
                    _ => None,
                };
                token_request(token_url, form, client)
            }
            Login::Cognito {
                region,
                client_id,
                client_secret,
                user,
                password,
            } => {
                if region.is_empty()
                    || !region
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
                {
                    return Err(LoginError::Invalid(format!("`{region}` is not an AWS region")));
                }
                let url = format!("https://cognito-idp.{region}.amazonaws.com/");
                let mut parameters = serde_json::Map::new();
                parameters.insert("USERNAME".into(), user.clone().into());
                parameters.insert("PASSWORD".into(), password.clone().into());
                if let Some(secret) = client_secret {
                    parameters.insert(
                        "SECRET_HASH".into(),
                        cognito_secret_hash(user, client_id, secret).into(),
                    );
                }
                let body = serde_json::json!({
                    "AuthFlow": "USER_PASSWORD_AUTH",
                    "ClientId": client_id,
                    "AuthParameters": parameters,
                });
                let response = http_agent(CertificateCheck::Verify)
                    .post(&url)
                    .header("content-type", "application/x-amz-json-1.1")
                    .header("x-amz-target", "AWSCognitoIdentityProviderService.InitiateAuth")
                    .send(body.to_string())
                    .map_err(|why| LoginError::Http(format!("POST {url}: {why}")))?;
                let (status, body) = read(response, &url)?;
                read_cognito_answer(status, &body)
            }
        }
    }
}

/// One request to a token endpoint, the client authenticated the two ways
/// RFC 6749 §2.3.1 names: `client_secret_basic` first - the one §4.1.1 §1.d
/// makes every authorization server support -, and when the server answers
/// `invalid_client`, once more with the id and secret in the form
/// (`client_secret_post`). The second is not a guess: a server that does not
/// form-decode the Basic credentials, as RFC 6749 Appendix B asks, refuses a
/// secret with a character the encoding changes, and takes it in the form -
/// one vendor's.
fn token_request(
    token_url: &str,
    form: Vec<(&'static str, String)>,
    client: Option<(&String, &String)>,
) -> Result<Obtained, LoginError> {
    let send =
        |form: &[(&'static str, String)], header: Option<String>| -> Result<(u16, Vec<u8>), LoginError> {
            let mut request = http_agent(CertificateCheck::Verify)
                .post(token_url)
                .header("accept", "application/json");
            if let Some(header) = header {
                request = request.header("authorization", header);
            }
            let response = request
                .send_form(form.iter().map(|(k, v)| (*k, v.as_str())))
                .map_err(|why| LoginError::Http(format!("POST {token_url}: {why}")))?;
            read(response, token_url)
        };
    let Some((id, secret)) = client else {
        let (status, body) = send(&form, None)?;
        return read_oauth_answer(token_url, status, &body);
    };
    let (status, body) = send(&form, Some(client_secret_basic(id, secret)?.header().to_string()))?;
    let first = read_oauth_answer(token_url, status, &body);
    match &first {
        Err(LoginError::Refused { error, .. }) if error == "invalid_client" => {
            let (status, body) = send(&client_secret_post(form, id, secret), None)?;
            read_oauth_answer(token_url, status, &body)
        }
        _ => first,
    }
}

/// The form with the client's id and secret in it - `client_secret_post`
/// (RFC 6749 §2.3.1), for a server that does not take them in the header.
pub fn client_secret_post(
    mut form: Vec<(&'static str, String)>,
    client_id: &str,
    client_secret: &str,
) -> Vec<(&'static str, String)> {
    form.retain(|(key, _)| *key != "client_id");
    form.push(("client_id", client_id.to_string()));
    form.push(("client_secret", client_secret.to_string()));
    form
}

fn read(mut response: ureq::http::Response<ureq::Body>, url: &str) -> Result<(u16, Vec<u8>), LoginError> {
    let status = response.status().as_u16();
    let body = response
        .body_mut()
        .read_to_vec()
        .map_err(|why| LoginError::Http(format!("{url}: reading the answer: {why}")))?;
    Ok((status, body))
}

/// `application/x-www-form-urlencoded`, as RFC 6749 Appendix B has it:
/// unreserved characters kept, a space as `+`, the rest `%XX`.
fn form_encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'*' => out.push(byte as char),
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// How an OAuth client authenticates at the token endpoint by
/// `client_secret_basic` (RFC 6749 §2.3.1): id and secret form-encoded, then
/// HTTP Basic.
pub fn client_secret_basic(client_id: &str, client_secret: &str) -> Result<Credentials, CredentialsRefusal> {
    Credentials::basic(&form_encode(client_id), &form_encode(client_secret))
}

/// The form of a password grant (RFC 6749 §4.3.2): `grant_type=password`,
/// the user and the password, the scope when one is asked for - and the
/// client's id when the client has no secret to authenticate with in the
/// header (§3.2.1: a public client names itself in the request).
pub fn password_grant_form(
    user: &str,
    password: &str,
    client_id: Option<&str>,
    has_secret: bool,
    scope: Option<&str>,
) -> Vec<(&'static str, String)> {
    let mut form = vec![
        ("grant_type", "password".to_string()),
        ("username", user.to_string()),
        ("password", password.to_string()),
    ];
    if let Some(scope) = scope {
        form.push(("scope", scope.to_string()));
    }
    if let (Some(id), false) = (client_id, has_secret) {
        form.push(("client_id", id.to_string()));
    }
    form
}

/// What a command line asks of the login, every field optional - the faces
/// (`energyml etp`, `retp-conformance`) parse their options into it, and
/// [`Choice::login`] decides, so that both log in the same way.
#[derive(Clone, Default)]
pub struct Choice {
    /// `--token`.
    pub token: Option<String>,
    /// `--user`.
    pub user: Option<String>,
    /// `--password`.
    pub password: Option<String>,
    /// `--token-url`.
    pub token_url: Option<String>,
    /// `--azure-tenant`.
    pub azure_tenant: Option<String>,
    /// `--cognito-region`.
    pub cognito_region: Option<String>,
    /// `--client-id`.
    pub client_id: Option<String>,
    /// `--client-secret`.
    pub client_secret: Option<String>,
    /// `--oauth-scope`.
    pub oauth_scope: Option<String>,
}

impl Choice {
    /// Which login these options ask for, the secrets not given read from
    /// `env` - `ETP_TOKEN`, `ETP_USER`, `ETP_PASSWORD`, `ETP_CLIENT_SECRET` -
    /// which keeps them off a command line and out of a shell's history.
    ///
    /// In this order: a token given; Cognito (`cognito_region`, a user and a
    /// password); a token endpoint (`token_url`, or Azure AD's for
    /// `azure_tenant`) - the **password grant** when a user is given, client
    /// credentials otherwise; HTTP Basic for a user alone; `ETP_TOKEN`; and
    /// no login at all.
    pub fn login(&self, env: impl Fn(&str) -> Option<String>) -> Result<Option<Login>, LoginError> {
        let given = |value: &Option<String>, name: &str| {
            value
                .clone()
                .filter(|v| !v.is_empty())
                .or_else(|| env(name).filter(|v| !v.is_empty()))
        };
        if let Some(token) = self.token.clone().filter(|t| !t.is_empty()) {
            return Ok(Some(Login::Bearer(token)));
        }
        let user = given(&self.user, "ETP_USER");
        let password = || {
            given(&self.password, "ETP_PASSWORD").ok_or_else(|| {
                LoginError::Invalid("a password is needed: --password, or ETP_PASSWORD".into())
            })
        };
        let client_secret = given(&self.client_secret, "ETP_CLIENT_SECRET");
        if let Some(region) = &self.cognito_region {
            return Ok(Some(Login::Cognito {
                region: region.clone(),
                client_id: self
                    .client_id
                    .clone()
                    .ok_or_else(|| LoginError::Invalid("--client-id is needed".into()))?,
                client_secret,
                user: user.ok_or_else(|| {
                    LoginError::Invalid("a user is needed with --cognito-region: --user, or ETP_USER".into())
                })?,
                password: password()?,
            }));
        }
        let token_url = match (&self.token_url, &self.azure_tenant) {
            (Some(url), _) => Some(url.clone()),
            (None, Some(tenant)) => Some(azure_token_url(tenant)?),
            (None, None) => None,
        };
        if let Some(token_url) = token_url {
            if let Some(user) = user {
                return Ok(Some(Login::Password {
                    token_url,
                    user,
                    password: password()?,
                    client_id: self.client_id.clone(),
                    client_secret,
                    scope: self.oauth_scope.clone(),
                }));
            }
            return Ok(Some(Login::ClientCredentials {
                token_url,
                client_id: self
                    .client_id
                    .clone()
                    .ok_or_else(|| LoginError::Invalid("--client-id is needed".into()))?,
                client_secret: client_secret.ok_or_else(|| {
                    LoginError::Invalid(
                        "a client secret is needed: --client-secret, or ETP_CLIENT_SECRET".into(),
                    )
                })?,
                scope: self.oauth_scope.clone(),
            }));
        }
        if let Some(user) = user {
            return Ok(Some(Login::Basic {
                user,
                password: password()?,
            }));
        }
        Ok(given(&None, "ETP_TOKEN").map(Login::Bearer))
    }
}

/// Cognito's `SECRET_HASH`: `Base64(HMAC_SHA256(secret, user + client_id))`,
/// asked for when the app client has a secret.
pub fn cognito_secret_hash(user: &str, client_id: &str, client_secret: &str) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, client_secret.as_bytes());
    let tag = ring::hmac::sign(&key, format!("{user}{client_id}").as_bytes());
    base64::engine::general_purpose::STANDARD.encode(tag.as_ref())
}

fn json(body: &[u8], server: &str, status: u16) -> Result<serde_json::Map<String, Json>, LoginError> {
    match serde_json::from_slice::<Json>(body) {
        Ok(Json::Object(object)) => Ok(object),
        _ => Err(LoginError::Http(format!(
            "{server} answered HTTP {status} with something that is not a JSON object ({} bytes)",
            body.len()
        ))),
    }
}

fn text(object: &serde_json::Map<String, Json>, name: &str) -> Option<String> {
    object.get(name).and_then(Json::as_str).map(str::to_string)
}

fn seconds(value: Option<&Json>) -> Option<Duration> {
    value.and_then(Json::as_u64).map(Duration::from_secs)
}

/// Read a token endpoint's answer (RFC 6749 §5.1 and §5.2).
///
/// A token whose `token_type` is not `Bearer` is refused: it would need a
/// scheme this client does not speak, and sending it as a bearer token is
/// sending the wrong thing.
pub fn read_oauth_answer(server: &str, status: u16, body: &[u8]) -> Result<Obtained, LoginError> {
    let object = json(body, server, status)?;
    if status != 200 {
        return Err(LoginError::Refused {
            server: server.to_string(),
            status,
            error: text(&object, "error").unwrap_or_else(|| "(no error code)".into()),
            description: text(&object, "error_description").unwrap_or_default(),
        });
    }
    let token = text(&object, "access_token")
        .ok_or_else(|| LoginError::Http(format!("{server} answered 200 without an `access_token`")))?;
    let kind = text(&object, "token_type").unwrap_or_default();
    if !kind.eq_ignore_ascii_case("bearer") {
        return Err(LoginError::Invalid(format!(
            "{server} gave a token of type `{kind}`; ETP takes bearer tokens (§4.1.1 §1.b)"
        )));
    }
    Ok(Obtained {
        credentials: Credentials::bearer(&token)?,
        expires_in: seconds(object.get("expires_in")),
    })
}

/// Read Cognito's answer to `InitiateAuth`: an `AuthenticationResult`, a
/// `ChallengeName`, or an error (`__type`, `message`).
pub fn read_cognito_answer(status: u16, body: &[u8]) -> Result<Obtained, LoginError> {
    let object = json(body, "Cognito", status)?;
    if status != 200 {
        let error = text(&object, "__type").unwrap_or_else(|| "(no error type)".into());
        return Err(LoginError::Refused {
            server: "Cognito".into(),
            status,
            error: error.rsplit('#').next().unwrap_or(&error).to_string(),
            description: text(&object, "message")
                .or_else(|| text(&object, "Message"))
                .unwrap_or_default(),
        });
    }
    if let Some(name) = text(&object, "ChallengeName") {
        return Err(LoginError::Challenge { name });
    }
    let result = object
        .get("AuthenticationResult")
        .and_then(Json::as_object)
        .ok_or_else(|| LoginError::Http("Cognito answered 200 without an `AuthenticationResult`".into()))?;
    let token = text(result, "AccessToken")
        .ok_or_else(|| LoginError::Http("Cognito's `AuthenticationResult` has no `AccessToken`".into()))?;
    Ok(Obtained {
        credentials: Credentials::bearer(&token)?,
        expires_in: seconds(result.get("ExpiresIn")),
    })
}
