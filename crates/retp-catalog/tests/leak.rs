//! The central test: a secret that is made up (the canary) goes through every
//! operation of the catalogue, and is found nowhere but where it is meant to
//! go - the login.
//!
//! The operations: `server add` (a draft for each method, and `--save-as`),
//! `show`, `list`, `test`, a login refused by a server that repeats what it
//! was sent, a connection refused, a reference that does not resolve, the
//! `Debug` of every public type, and the log at its most verbose (what
//! `--verbose` shows, and more). What is looked at: every string a person
//! would see, every log line, the bytes of every file under the catalogue
//! folder and the configuration file.
//!
//! **A test that cannot fail proves nothing**, so the test also shows that it
//! can see: the canary is found in the secret file it was written to, in a
//! string that leaks on purpose, in the credentials the login builds, and in
//! the raw error of a server that echoes it. And it is broken on purpose, by
//! hand, before it is trusted. Each of these makes this test red:
//!
//! - `impl Debug for Secret` printing `self.expose()` (secret.rs);
//! - `render::show` or `Entry::describe` putting a value in a status detail;
//! - `Resolved::obtain_endpoint` or `Catalog::run_test` using the error text
//!   without `redact` (resolve.rs, check.rs);
//! - `Catalog::record_test` or `Entry::to_toml` writing a secret (check.rs, entry.rs);
//! - a `tracing::debug!` whose text or fields name a value instead of a reference
//!   (secret.rs, resolve.rs).

use retp_catalog::{
    Catalog, Connection, Draft, Network, OsSecrets, Overrides, Reserved, Secret, SecretRef, observe, render,
};
use retp_client::login::{Choice, Login};
use retp_client::native::Endpoint;

use crate::common::{CANARY, FakeToken, Reply, Sandbox, capture_log, files_containing, logged};

struct Quiet;

impl Network for Quiet {
    fn well_known(&self, _: &Endpoint) -> Result<String, String> {
        Ok("Fake 1.2".into())
    }

    fn session(&self, _: Endpoint, _: bool) -> Result<String, String> {
        Ok("opened".into())
    }
}

/// Whether `text` holds the canary: the one definition of a leak in this file.
fn leaks(text: &str) -> bool {
    text.contains(CANARY)
}

fn draft(url: &str, method: &str) -> Draft {
    Draft {
        url: url.into(),
        method: Some(method.into()),
        ..Draft::default()
    }
}

#[test]
fn the_canary_goes_to_the_login_and_nowhere_else() {
    capture_log();
    let sandbox = Sandbox::new("leak");
    let mut catalog = sandbox.catalog();
    let reserved = Reserved::new(["shell".to_string()]);

    // Where the canary is kept: a variable (a closure's, not the process's), and a file.
    let file = sandbox.secret_file("api.secret", &format!("{CANARY}\n"));
    let file_ref = format!("file:{}", file.to_str().expect("utf-8"));
    let secrets = OsSecrets::with_env(|name| (name == "RETP_TEST_SECRET").then(|| CANARY.to_string()));

    // Token endpoints: one that refuses and repeats what it was sent, one that is not there.
    let echoing = FakeToken::start(Reply::EchoBody, 8);
    let echoing_password = FakeToken::start(Reply::EchoBody, 4);
    let closed = FakeToken::closed();
    let secret = "env:RETP_TEST_SECRET";

    let drafts: Vec<(&str, Draft)> = vec![
        ("none-srv", draft("wss://h.example.com/etp", "none")),
        (
            "basic-srv",
            Draft {
                user: Some("u".into()),
                password_from: Some(secret.into()),
                ..draft("wss://h.example.com/etp", "basic")
            },
        ),
        (
            "bearer-srv",
            Draft {
                token_from: Some(file_ref.clone()),
                ..draft("wss://h.example.com/etp", "bearer")
            },
        ),
        (
            "pw-srv",
            Draft {
                token_url: Some(echoing_password.url.clone()),
                user: Some("u".into()),
                client_id: Some("cid".into()),
                password_from: Some(secret.into()),
                ..draft("wss://h.example.com/etp", "oauth-password")
            },
        ),
        (
            "client-srv",
            Draft {
                token_url: Some(echoing.url.clone()),
                client_id: Some("cid".into()),
                client_secret_from: Some(secret.into()),
                ..draft("wss://h.example.com/etp", "oauth-client")
            },
        ),
        (
            "closed-srv",
            Draft {
                token_url: Some(closed.clone()),
                client_id: Some("cid".into()),
                client_secret_from: Some(secret.into()),
                ..draft("wss://h.example.com/etp", "oauth-client")
            },
        ),
        (
            "azure-srv",
            Draft {
                tenant: Some("00000000-0000-0000-0000-000000000000".into()),
                client_id: Some("cid".into()),
                scope: Some("api://cid/.default".into()),
                client_secret_from: Some(secret.into()),
                ..draft("wss://h.example.com/etp", "azure")
            },
        ),
        (
            "cognito-srv",
            Draft {
                region: Some("us-east-2".into()),
                client_id: Some("cid".into()),
                user: Some("u".into()),
                password_from: Some(secret.into()),
                ..draft("wss://h.example.com/etp", "cognito")
            },
        ),
        (
            "headers-srv",
            Draft {
                headers_from: vec![("x-api-key".into(), secret.into())],
                ..draft("wss://h.example.com/etp", "none")
            },
        ),
        (
            "missing-srv",
            Draft {
                user: Some("u".into()),
                password_from: Some("env:RETP_NOT_DEFINED".into()),
                ..draft("wss://h.example.com/etp", "basic")
            },
        ),
    ];
    let names: Vec<&str> = drafts.iter().map(|(name, _)| *name).collect();
    for (name, draft) in drafts {
        let entry = draft.into_entry().unwrap_or_else(|e| panic!("{name}: {e}"));
        catalog.add(name, entry, false, &reserved).expect(name);
    }

    // `--save-as`, from a password that came from the ambient variable and one from the line.
    let ambient = |name: &str| (name == "ETP_PASSWORD").then(|| CANARY.to_string());
    let from_env = observe(
        "saved-env",
        &Connection {
            url: "wss://h.example.com/etp".into(),
            ..Connection::default()
        },
        &Choice {
            user: Some("u".into()),
            ..Choice::default()
        },
        &ambient,
    )
    .expect("observed");
    let from_line = observe(
        "saved-line",
        &Connection {
            url: "wss://h.example.com/etp".into(),
            ..Connection::default()
        },
        &Choice {
            user: Some("u".into()),
            password: Some(CANARY.into()),
            ..Choice::default()
        },
        &|_| None,
    )
    .expect("observed");
    let mut seen: Vec<(String, String)> = Vec::new();
    for observed in [&from_env, &from_line] {
        seen.push(("save-as advice".into(), observed.advice.join("\n")));
        seen.push(("save-as entry".into(), observed.entry.to_toml()));
        seen.push(("save-as debug".into(), format!("{observed:?}")));
    }
    catalog
        .add("saved-env", from_env.entry.clone(), false, &reserved)
        .expect("saved");
    catalog
        .add("saved-line", from_line.entry.clone(), false, &reserved)
        .expect("saved");

    // What a person sees.
    seen.push(("list".into(), render::list(&catalog.listed(&secrets))));
    for name in names.iter().chain(["saved-env", "saved-line"].iter()) {
        let found = catalog.find(name).expect(name);
        let description = found.entry.describe(found.name, &secrets);
        seen.push((format!("show {name}"), render::show(&description)));
        seen.push((format!("debug description {name}"), format!("{description:?}")));
        seen.push((format!("debug entry {name}"), format!("{:#?}", found.entry)));

        // Azure AD and Cognito are real services: their login is never attempted here.
        let offline = matches!(*name, "azure-srv" | "cognito-srv");
        match found.entry.resolve(found.name, &secrets, &Overrides::default()) {
            Ok(resolved) => {
                seen.push((format!("debug resolved {name}"), format!("{resolved:?}")));
                if offline {
                    // Nothing more to do with it.
                } else {
                    match resolved.obtain_endpoint() {
                        Ok(opened) => seen.push((format!("notes {name}"), opened.notes.join("\n"))),
                        Err(error) => {
                            seen.push((format!("refused {name}"), error.to_string()));
                            seen.push((format!("debug refused {name}"), format!("{error:?}")));
                        }
                    }
                }
            }
            Err(error) => {
                seen.push((format!("resolve {name}"), error.to_string()));
                seen.push((format!("debug resolve {name}"), format!("{error:?}")));
            }
        }

        if offline {
            continue;
        }
        let report = catalog.run_test(name, &secrets, &Quiet);
        seen.push((format!("test {name}"), render::test(&report)));
        seen.push((format!("debug test {name}"), format!("{report:?}")));
        catalog.record_test(&report).expect("recorded");
    }
    seen.push((
        "debug secret".into(),
        format!(
            "{:?} {:#?}",
            Secret::new(CANARY.into()),
            Some(Secret::new(CANARY.into()))
        ),
    ));
    seen.push((
        "debug reference".into(),
        format!("{:?}", SecretRef::parse(&file_ref).expect("a reference")),
    ));

    // The assertions.
    for (what, text) in &seen {
        assert!(!leaks(text), "the canary is in `{what}`:\n{text}");
    }
    let log = logged();
    assert!(!leaks(&log), "the canary is in the log:\n{log}");
    assert!(
        log.contains("RETP_TEST_SECRET"),
        "the log captured nothing, so its silence would prove nothing:\n{log}"
    );
    assert_eq!(
        files_containing(&sandbox.servers(), CANARY),
        Vec::<std::path::PathBuf>::new(),
        "a file of the catalogue holds the canary"
    );
    let config = std::fs::read(sandbox.location().config_file()).expect("the configuration file");
    assert!(
        !config.windows(CANARY.len()).any(|w| w == CANARY.as_bytes()),
        "the configuration file holds the canary"
    );
    assert!(
        sandbox.servers().join("basic-srv").join("server.toml").is_file()
            && sandbox
                .servers()
                .join("basic-srv")
                .join("state")
                .join("last.json")
                .is_file(),
        "the files that were to be scanned exist"
    );

    // The test can see.
    assert_eq!(
        files_containing(&sandbox.secrets(), CANARY),
        vec![file.clone()],
        "the scan finds the canary where it is"
    );
    assert!(leaks(&format!("a string that leaks {CANARY} on purpose")));
    assert!(!leaks("a string that does not"));
    let found = catalog.find("bearer-srv").expect("found");
    let resolved = found
        .entry
        .resolve("bearer-srv", &secrets, &Overrides::default())
        .expect("resolved");
    let credentials = resolved
        .login()
        .expect("a login")
        .obtain()
        .expect("a token in hand")
        .credentials;
    assert!(
        credentials.header().contains(CANARY),
        "the canary does reach the login"
    );
    let found = catalog.find("basic-srv").expect("found");
    let resolved = found
        .entry
        .resolve("basic-srv", &secrets, &Overrides::default())
        .expect("resolved");
    assert!(matches!(resolved.login(), Some(Login::Basic { password, .. }) if *password == CANARY));

    // What a server that repeats what it is sent does: the client's own error holds the canary,
    // and the catalogue's does not. The redaction is a last-resort guard, and this shows it working.
    let found = catalog.find("client-srv").expect("found");
    let resolved = found
        .entry
        .resolve("client-srv", &secrets, &Overrides::default())
        .expect("resolved");
    let raw = resolved
        .login()
        .expect("a login")
        .obtain()
        .expect_err("refused")
        .to_string();
    assert!(
        leaks(&raw),
        "the fake server did not echo the secret, so the redaction was not exercised: {raw}"
    );
    let kept = resolved.obtain_endpoint().err().expect("refused").to_string();
    assert!(!leaks(&kept) && kept.contains("[redacted]"), "{kept}");
}

#[test]
fn a_literal_secret_in_a_hand_written_file_is_refused_and_does_not_reach_the_catalogue() {
    let sandbox = Sandbox::new("leak-hand-written");
    let mut catalog = sandbox.catalog();
    catalog
        .add(
            "ok",
            Draft {
                url: "wss://h.example.com/etp".into(),
                ..Draft::default()
            }
            .into_entry()
            .expect("entry"),
            false,
            &Reserved::default(),
        )
        .expect("added");
    let dir = sandbox.servers().join("hand");
    std::fs::create_dir_all(&dir).expect("folder");
    std::fs::write(
        dir.join("server.toml"),
        format!("schema = 1\n[server]\netp_url = \"wss://h.example.com/etp\"\n[auth]\nmethod = \"basic\"\nuser = \"u\"\npassword = \"{CANARY}\"\n"),
    )
    .expect("file");
    let reopened = Catalog::open(&sandbox.location()).expect("opened");
    let error = reopened.find("hand").expect_err("unusable").to_string();
    assert!(!leaks(&error), "{error}");
    let listed = render::list(&reopened.listed(&OsSecrets::with_env(|_| None)));
    assert!(!leaks(&listed), "{listed}");
}
