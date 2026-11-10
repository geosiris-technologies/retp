//! `--save-as`: an entry written from a connection that worked. The value of a
//! secret is never saved: a variable's name is, and a value that came from the
//! command line is not, with the advice to define a variable of its own.

use retp_catalog::{AuthSpec, Catalog, CatalogError, Connection, Reserved, SecretRef, Transport, observe};
use retp_client::login::Choice;

use crate::common::{CANARY, Sandbox, files_containing};

fn connection() -> Connection {
    Connection {
        url: "wss://h.example.com/etp".into(),
        partition: Some("opendes".into()),
        insecure: true,
        ..Connection::default()
    }
}

fn from_env(name: &str) -> Option<String> {
    (name == "ETP_PASSWORD" || name == "ETP_TOKEN" || name == "ETP_CLIENT_SECRET").then(|| CANARY.to_string())
}

fn nothing(_: &str) -> Option<String> {
    None
}

#[test]
fn a_secret_from_an_ambient_variable_becomes_a_reference_to_that_variable() {
    let choice = Choice {
        user: Some("u".into()),
        ..Choice::default()
    };
    let observed = observe("azure-test", &connection(), &choice, &from_env).expect("observed");
    assert_eq!(
        observed.entry.auth,
        AuthSpec::Basic {
            user: "u".into(),
            password: SecretRef::Env("ETP_PASSWORD".into())
        }
    );
    assert!(
        observed
            .advice
            .iter()
            .any(|a| a.contains("ETP_PASSWORD") && a.contains("shared"))
    );
    assert_eq!(observed.entry.server.partition.as_deref(), Some("opendes"));
    assert_eq!(observed.entry.server.tls, retp_catalog::Tls::Skip);
    assert!(!observed.entry.to_toml().contains(CANARY));
    assert!(observed.advice.iter().all(|a| !a.contains(CANARY)));
}

#[test]
fn a_secret_from_the_command_line_is_not_saved_and_the_entry_points_at_a_variable_of_its_own() {
    let choice = Choice {
        user: Some("u".into()),
        password: Some(CANARY.into()),
        ..Choice::default()
    };
    let observed = observe("azure-test", &connection(), &choice, &nothing).expect("observed");
    assert_eq!(
        observed.entry.auth,
        AuthSpec::Basic {
            user: "u".into(),
            password: SecretRef::Env("RETP_AZURE_TEST_PASSWORD".into())
        }
    );
    let advice = observed.advice.join("\n");
    assert!(
        advice.contains("NOT saved")
            && advice.contains("shell history")
            && advice.contains("RETP_AZURE_TEST_PASSWORD")
    );
    assert!(!advice.contains(CANARY));
    assert!(!observed.entry.to_toml().contains(CANARY));
}

#[test]
fn the_headers_of_the_line_are_saved_and_a_secret_one_as_a_reference() {
    use retp_catalog::{Header, HeaderValue};
    use retp_client::ExtraHeaders;

    let connection = Connection {
        headers: ExtraHeaders::from_pairs([("x-tenant", "acme"), ("X-Api-Key", CANARY)])
            .expect("headers a command line may give"),
        ..connection()
    };
    let observed = observe("azure-test", &connection, &Choice::default(), &nothing).expect("observed");
    assert_eq!(
        observed.entry.headers,
        [
            // Not a secret by its name: saved as `server add --header` saves it.
            Header {
                name: "x-tenant".into(),
                value: HeaderValue::Literal("acme".into())
            },
            // A secret by its name: the entry points at a variable of its own.
            Header {
                name: "X-Api-Key".into(),
                value: HeaderValue::Secret(SecretRef::Env("RETP_AZURE_TEST_HEADER_X_API_KEY".into()))
            },
        ]
    );
    let advice = observed.advice.join("\n");
    assert!(
        advice.contains("NOT saved")
            && advice.contains("--header X-Api-Key")
            && advice.contains("RETP_AZURE_TEST_HEADER_X_API_KEY"),
        "{advice}"
    );
    let shown = format!(
        "{advice} {} {observed:?} {connection:?}",
        observed.entry.to_toml()
    );
    assert!(!shown.contains(CANARY), "{shown}");

    // What was saved is an entry the catalogue reads back - a file keeps no
    // order between its headers.
    let read = retp_catalog::Entry::parse(&observed.entry.to_toml()).expect("valid");
    assert_eq!(read.headers.len(), 2);
    assert!(
        observed
            .entry
            .headers
            .iter()
            .all(|header| read.headers.contains(header)),
        "{:?}",
        read.headers
    );
}

#[test]
fn every_method_is_deduced_from_the_login_the_options_gave() {
    let case = |choice: Choice, env: &dyn Fn(&str) -> Option<String>| {
        observe("srv", &connection(), &choice, env)
            .expect("observed")
            .entry
            .auth
    };

    assert_eq!(case(Choice::default(), &nothing), AuthSpec::None);
    assert_eq!(
        case(Choice::default(), &from_env),
        AuthSpec::Bearer {
            token: SecretRef::Env("ETP_TOKEN".into())
        }
    );
    assert!(matches!(
        case(
            Choice { token_url: Some("https://t.example.com/token".into()), client_id: Some("c".into()), client_secret: Some(CANARY.into()), oauth_scope: Some("s".into()), ..Choice::default() },
            &nothing
        ),
        AuthSpec::OauthClient { client_secret: SecretRef::Env(name), scope: Some(_), .. } if name == "RETP_SRV_CLIENT_SECRET"
    ));
    assert!(matches!(
        case(
            Choice {
                azure_tenant: Some("00000000-0000-0000-0000-000000000000".into()),
                client_id: Some("c".into()),
                client_secret: Some(CANARY.into()),
                ..Choice::default()
            },
            &nothing
        ),
        AuthSpec::Azure {
            client_secret: Some(_),
            user: None,
            ..
        }
    ));
    assert!(matches!(
        case(
            Choice {
                token_url: Some("https://t.example.com/token".into()),
                user: Some("u".into()),
                password: Some(CANARY.into()),
                ..Choice::default()
            },
            &nothing
        ),
        AuthSpec::OauthPassword {
            client_secret: None,
            ..
        }
    ));
    assert!(matches!(
        case(
            Choice {
                cognito_region: Some("us-east-2".into()),
                client_id: Some("c".into()),
                user: Some("u".into()),
                password: Some(CANARY.into()),
                ..Choice::default()
            },
            &nothing
        ),
        AuthSpec::Cognito {
            client_secret: None,
            ..
        }
    ));
}

#[test]
fn rest_is_kept_as_the_transport() {
    let mut rest = connection();
    rest.url = "https://h.example.com/rest".into();
    rest.rest = true;
    let observed = observe("srv", &rest, &Choice::default(), &nothing).expect("observed");
    assert_eq!(observed.entry.server.transport, Transport::Rest);
    assert_eq!(
        observed.entry.server.rest_url.as_deref(),
        Some("https://h.example.com/rest")
    );
    assert!(observed.entry.server.etp_url.is_none());
}

#[test]
fn nothing_is_written_for_a_name_that_is_taken_and_no_file_holds_the_value() {
    let sandbox = Sandbox::new("save");
    let mut catalog = sandbox.catalog();
    let reserved = Reserved::new(["shell".to_string()]);
    let choice = Choice {
        user: Some("u".into()),
        password: Some(CANARY.into()),
        ..Choice::default()
    };
    let observed = observe("azure-test", &connection(), &choice, &nothing).expect("observed");

    // The check the command makes before it runs anything.
    catalog
        .check_new("azure-test", &[], false, &reserved)
        .expect("free");
    catalog
        .add("azure-test", observed.entry.clone(), false, &reserved)
        .expect("saved");
    assert!(matches!(
        catalog.check_new("azure-test", &[], false, &reserved),
        Err(CatalogError::Exists { .. })
    ));
    assert!(
        catalog
            .add("azure-test", observed.entry.clone(), false, &reserved)
            .is_err()
    );
    assert!(
        catalog
            .add("azure-test", observed.entry.clone(), true, &reserved)
            .is_ok(),
        "--force replaces"
    );
    assert!(matches!(
        catalog.check_new("shell", &[], false, &reserved),
        Err(CatalogError::Reserved { .. })
    ));

    assert_eq!(
        files_containing(&sandbox.root, CANARY),
        Vec::<std::path::PathBuf>::new()
    );
    let reopened = Catalog::open(&sandbox.location()).expect("opened");
    assert_eq!(reopened.find("azure-test").expect("saved").entry, &observed.entry);
}
