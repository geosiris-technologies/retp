//! Each method: the entry, then the `Choice`, then the `Login` the client
//! deduces - and the method declared is the one deduced. The ambient `ETP_*`
//! variables are never consulted.

use retp_catalog::{
    AuthSpec, CatalogError, Entry, Handshake, OsSecrets, Overrides, SecretRef, ServerSpec, Tls,
};
use retp_client::ServerProfile;
use retp_client::login::{Login, azure_token_url};
use retp_client::native::CertificateCheck;
use retp_proto::probe::Profile;

use crate::common::{CANARY, MemorySecrets};

const URL: &str = "wss://h.example.com/etp";
const TOKEN_URL: &str = "https://t.example.com/token";
const TENANT: &str = "00000000-0000-0000-0000-000000000000";

fn env(name: &str) -> SecretRef {
    SecretRef::Env(name.to_string())
}

fn secrets() -> MemorySecrets {
    MemorySecrets::of(&[("env:P", "pw"), ("env:S", "client-secret"), ("env:T", "tok")])
}

fn resolve(auth: AuthSpec) -> retp_catalog::Resolved {
    Entry::new(ServerSpec::etp(URL), auth)
        .resolve("test", &secrets(), &Overrides::default())
        .expect("resolved")
}

#[test]
fn none_asks_for_nothing() {
    assert!(resolve(AuthSpec::None).login().is_none());
}

#[test]
fn basic_is_http_basic() {
    let resolved = resolve(AuthSpec::Basic {
        user: "u".into(),
        password: env("P"),
    });
    assert!(
        matches!(resolved.login(), Some(Login::Basic { user, password }) if *user == "u" && *password == "pw")
    );
    let obtained = resolved
        .login()
        .expect("a login")
        .obtain()
        .expect("no network for Basic");
    assert_eq!(obtained.credentials.header(), "Basic dTpwdw==");
}

#[test]
fn bearer_is_a_token_in_hand() {
    let resolved = resolve(AuthSpec::Bearer { token: env("T") });
    let obtained = resolved
        .login()
        .expect("a login")
        .obtain()
        .expect("no network for a token");
    assert_eq!(obtained.credentials.header(), "Bearer tok");
}

#[test]
fn oauth_password_is_the_password_grant() {
    let resolved = resolve(AuthSpec::OauthPassword {
        token_url: TOKEN_URL.into(),
        user: "u".into(),
        password: env("P"),
        client_id: Some("cid".into()),
        client_secret: Some(env("S")),
        scope: Some("openid".into()),
    });
    match resolved.login() {
        Some(Login::Password {
            token_url,
            user,
            password,
            client_id,
            client_secret,
            scope,
        }) => {
            assert_eq!(
                (token_url.as_str(), user.as_str(), password.as_str()),
                (TOKEN_URL, "u", "pw")
            );
            assert_eq!(client_id.as_deref(), Some("cid"));
            assert_eq!(client_secret.as_deref(), Some("client-secret"));
            assert_eq!(scope.as_deref(), Some("openid"));
        }
        _ => panic!("not a password grant"),
    }
}

#[test]
fn oauth_client_is_client_credentials() {
    let resolved = resolve(AuthSpec::OauthClient {
        token_url: TOKEN_URL.into(),
        client_id: "cid".into(),
        client_secret: env("S"),
        scope: None,
    });
    assert!(matches!(
        resolved.login(),
        Some(Login::ClientCredentials { token_url, client_id, client_secret, scope: None })
            if *token_url == TOKEN_URL && *client_id == "cid" && *client_secret == "client-secret"
    ));
}

#[test]
fn azure_is_client_credentials_at_the_tenants_endpoint() {
    let resolved = resolve(AuthSpec::Azure {
        tenant: TENANT.into(),
        client_id: "cid".into(),
        scope: Some("api://cid/.default".into()),
        client_secret: Some(env("S")),
        user: None,
        password: None,
    });
    let endpoint = azure_token_url(TENANT).expect("a tenant");
    assert!(matches!(
        resolved.login(),
        Some(Login::ClientCredentials { token_url, scope: Some(scope), .. })
            if *token_url == endpoint && *scope == "api://cid/.default"
    ));
}

#[test]
fn azure_with_a_user_is_the_password_grant_at_the_tenants_endpoint() {
    let resolved = resolve(AuthSpec::Azure {
        tenant: TENANT.into(),
        client_id: "cid".into(),
        scope: None,
        client_secret: None,
        user: Some("u".into()),
        password: Some(env("P")),
    });
    let endpoint = azure_token_url(TENANT).expect("a tenant");
    assert!(matches!(
        resolved.login(),
        Some(Login::Password { token_url, user, password, .. })
            if *token_url == endpoint && *user == "u" && *password == "pw"
    ));
}

#[test]
fn cognito_is_the_user_pool_flow() {
    let resolved = resolve(AuthSpec::Cognito {
        region: "us-east-2".into(),
        client_id: "cid".into(),
        user: "u".into(),
        password: env("P"),
        client_secret: Some(env("S")),
    });
    assert!(matches!(
        resolved.login(),
        Some(Login::Cognito { region, client_id, client_secret: Some(secret), user, password })
            if *region == "us-east-2" && *client_id == "cid" && *secret == "client-secret" && *user == "u" && *password == "pw"
    ));
}

#[test]
fn the_ambient_variables_are_not_consulted() {
    // The process environment of a test is shared by every test; the variables are those of a closure.
    let ambient = OsSecrets::with_env(|name| match name {
        "ETP_TOKEN" => Some("ambient-token".to_string()),
        "ETP_USER" => Some("ambient-user".to_string()),
        "ETP_PASSWORD" => Some("ambient-password".to_string()),
        "ETP_CLIENT_SECRET" => Some("ambient-secret".to_string()),
        "OWN" => Some("own-token".to_string()),
        _ => None,
    });
    let none = Entry::new(ServerSpec::etp(URL), AuthSpec::None)
        .resolve("none", &ambient, &Overrides::default())
        .expect("resolved");
    assert!(
        none.login().is_none(),
        "ETP_TOKEN set for another server was taken"
    );

    let own = Entry::new(ServerSpec::etp(URL), AuthSpec::Bearer { token: env("OWN") })
        .resolve("own", &ambient, &Overrides::default())
        .expect("resolved");
    let header = own
        .login()
        .expect("a login")
        .obtain()
        .expect("a token")
        .credentials;
    assert_eq!(header.header(), "Bearer own-token");

    // An entry that wants the ambient variable says so, by name.
    let wants = Entry::new(
        ServerSpec::etp(URL),
        AuthSpec::Bearer {
            token: env("ETP_TOKEN"),
        },
    )
    .resolve("wants", &ambient, &Overrides::default())
    .expect("resolved");
    let header = wants
        .login()
        .expect("a login")
        .obtain()
        .expect("a token")
        .credentials;
    assert_eq!(header.header(), "Bearer ambient-token");
}

#[test]
fn a_reference_that_gives_nothing_names_the_server_the_field_and_the_reference() {
    let entry = Entry::new(
        ServerSpec::etp(URL),
        AuthSpec::Basic {
            user: "u".into(),
            password: env("NOT_THERE"),
        },
    );
    let error = entry
        .resolve("azure", &MemorySecrets::of(&[]), &Overrides::default())
        .expect_err("no value");
    match &error {
        CatalogError::Secret { entry, field, .. } => {
            assert_eq!((entry.as_str(), field.as_str()), ("azure", "password"))
        }
        other => panic!("{other}"),
    }
    assert!(error.to_string().contains("NOT_THERE"));
}

#[test]
fn credentials_go_to_the_entrys_own_urls_only() {
    let mut server = ServerSpec::etp(URL);
    server.rest_url = Some("https://h.example.com/rest".into());
    let entry = Entry::new(server, AuthSpec::Bearer { token: env("T") });
    let go = |url: &str| {
        entry.resolve(
            "test",
            &secrets(),
            &Overrides {
                url: Some(url.into()),
                ..Overrides::default()
            },
        )
    };
    assert!(go(URL).is_ok());
    assert!(go("https://h.example.com/rest").is_ok());
    let error = go("wss://attacker.example.com/etp").expect_err("refused");
    assert!(matches!(error, CatalogError::ForeignUrl { .. }), "{error}");
}

#[test]
fn the_command_line_settles_partition_certificate_and_handshake() {
    let mut server = ServerSpec::etp(URL);
    server.partition = Some("from-entry".into());
    server.tls = Tls::Skip;
    let entry = Entry::new(server.clone(), AuthSpec::None);

    let from_entry = entry
        .resolve("t", &secrets(), &Overrides::default())
        .expect("resolved");
    let endpoint = from_entry.endpoint(None);
    assert!(
        matches!(&endpoint.profile, ServerProfile::Rddms { data_partition } if data_partition == "from-entry")
    );
    assert_eq!(endpoint.certificates, CertificateCheck::Skip);
    assert!(matches!(endpoint.handshake, Profile::Native));
    assert_eq!(endpoint.url, URL);

    let overridden = entry
        .resolve(
            "t",
            &secrets(),
            &Overrides {
                partition: Some("from-line".into()),
                as_browser: true,
                ..Overrides::default()
            },
        )
        .expect("resolved");
    let endpoint = overridden.endpoint(None);
    assert!(
        matches!(&endpoint.profile, ServerProfile::Rddms { data_partition } if data_partition == "from-line")
    );
    assert!(matches!(endpoint.handshake, Profile::Browser));

    server.tls = Tls::Verify;
    server.handshake = Handshake::Native;
    let strict = Entry::new(server, AuthSpec::None)
        .resolve(
            "t",
            &secrets(),
            &Overrides {
                insecure: true,
                ..Overrides::default()
            },
        )
        .expect("resolved");
    assert_eq!(
        strict.endpoint(None).certificates,
        CertificateCheck::Skip,
        "--insecure on the line"
    );
}

#[test]
fn what_resolution_returns_shows_no_value_in_debug() {
    let resolved = resolve(AuthSpec::Basic {
        user: "u".into(),
        password: env("P"),
    });
    assert!(!format!("{resolved:?}").contains("pw"));
    let canary = MemorySecrets::of(&[("env:P", CANARY)]);
    let resolved = Entry::new(
        ServerSpec::etp(URL),
        AuthSpec::Basic {
            user: "u".into(),
            password: env("P"),
        },
    )
    .resolve("t", &canary, &Overrides::default())
    .expect("resolved");
    assert!(!format!("{resolved:?}").contains(CANARY));
    assert_eq!(
        resolved.redact(&format!("echo {CANARY} echo")),
        "echo [redacted] echo"
    );
}

#[test]
fn extra_headers_are_read_but_not_sent_until_the_client_can() {
    use retp_catalog::{Header, HeaderValue};
    let mut entry = Entry::new(ServerSpec::etp(URL), AuthSpec::None);
    entry.headers = vec![Header {
        name: "x-api-key".into(),
        value: HeaderValue::Secret(env("T")),
    }];
    let resolved = entry
        .resolve("t", &secrets(), &Overrides::default())
        .expect("resolved");
    assert_eq!(resolved.headers(), [("x-api-key".to_string(), "tok".to_string())]);
    // Connecting without them would be a quiet departure from the definition.
    let error = resolved.obtain_endpoint().err().expect("refused by name");
    assert!(error.to_string().contains("extra headers"), "{error}");
}
