//! What `server.toml` may and may not hold. Each refusal is a rule of the
//! design (a secret is a reference, never a value), written as a rule on
//! texts made here; none reads a file.

use retp_catalog::{
    AuthSpec, Draft, Entry, Handshake, Header, HeaderValue, SecretRef, ServerSpec, Tls, Transport,
    WriteDefaults,
};

use crate::common::CANARY;

const SERVER: &str = r#"
[server]
etp_url = "wss://adme.example.com/api/reservoir-ddms-etp/v2/"
rest_url = "https://adme.example.com/api/reservoir-ddms/v2"
transport = "etp"
partition = "opendes"
tls = "verify"
handshake = "native"
protected = false
"#;

fn file(auth: &str) -> String {
    format!("schema = 1\ndescription = \"a test server\"\nalias = [\"az\"]\n{SERVER}\n{auth}")
}

fn refusal(text: &str) -> String {
    Entry::parse(text).expect_err("the file is refused").to_string()
}

fn line_of(text: &str, needle: &str) -> usize {
    text.lines()
        .position(|l| l.contains(needle))
        .expect("the needle is in the text")
        + 1
}

#[test]
fn a_valid_file_is_read() {
    let text = file(
        "[auth]\nmethod = \"azure\"\ntenant = \"00000000-0000-0000-0000-000000000000\"\nclient_id = \"11111111\"\nscope = \"api://x/.default\"\nclient_secret = { env = \"RETP_AZURE_TEST_SECRET\" }\n",
    );
    let entry = Entry::parse(&text).expect("valid");
    assert_eq!(entry.aliases, ["az"]);
    assert_eq!(entry.server.partition.as_deref(), Some("opendes"));
    match entry.auth {
        AuthSpec::Azure { client_secret, .. } => {
            assert_eq!(
                client_secret,
                Some(SecretRef::Env("RETP_AZURE_TEST_SECRET".into()))
            );
        }
        other => panic!("not azure: {other:?}"),
    }
}

#[test]
fn a_literal_secret_is_refused_naming_the_key_and_the_line_and_not_the_value() {
    for key in ["password", "token", "client_secret", "secret"] {
        for quote in ['"', '\''] {
            let text = file(&format!(
                "[auth]\nmethod = \"basic\"\nuser = \"u\"\n{key} = {quote}{CANARY}{quote}\n"
            ));
            let message = refusal(&text);
            assert!(message.contains(&format!("`{key}`")), "{key}: {message}");
            let line = line_of(&text, &format!("{key} = "));
            assert!(message.contains(&format!("line {line}")), "{key}: {message}");
            assert!(!message.contains(CANARY), "the value is repeated: {message}");
        }
    }
}

#[test]
fn a_dotted_literal_and_a_literal_in_an_inline_table_are_refused_without_the_value() {
    let dotted = format!("schema = 1\nauth.password = \"{CANARY}\"\n{SERVER}");
    let message = refusal(&dotted);
    assert!(
        message.contains("password") && !message.contains(CANARY),
        "{message}"
    );

    let inline = file(&format!(
        "auth = {{ method = \"bearer\", token = \"{CANARY}\" }}\n"
    ));
    let message = refusal(&inline);
    assert!(!message.contains(CANARY), "the value is repeated: {message}");
}

#[test]
fn a_url_with_credentials_is_refused_naming_the_key() {
    for key in ["etp_url", "rest_url"] {
        let scheme = if key == "etp_url" { "wss" } else { "https" };
        let text = file("[auth]\nmethod = \"none\"\n").replace(
            &format!("{key} = "),
            &format!("{key} = \"{scheme}://user:{CANARY}@h.example.com/\"\n#"),
        );
        let message = refusal(&text);
        assert!(message.contains(&format!("`{key}`")), "{message}");
        assert!(!message.contains(CANARY), "{message}");
    }
}

#[test]
fn an_unknown_key_is_refused_by_name_and_line_and_not_by_value() {
    let text = file(&format!(
        "[auth]\nmethod = \"basic\"\nuser = \"u\"\npasword = \"{CANARY}\"\n"
    ));
    let message = refusal(&text);
    assert!(message.contains("pasword"), "{message}");
    assert!(!message.contains(CANARY), "{message}");
}

#[test]
fn a_wrong_type_or_value_never_quotes_the_source() {
    let wrong_type = file("[auth]\nmethod = \"none\"\n")
        .replace("protected = false", &format!("keepalive = \"{CANARY}\""));
    assert!(!refusal(&wrong_type).contains(CANARY));

    let wrong_tls =
        file("[auth]\nmethod = \"none\"\n").replace("tls = \"verify\"", &format!("tls = \"{CANARY}\""));
    assert!(!refusal(&wrong_tls).contains(CANARY));

    let wrong_method = file(&format!("[auth]\nmethod = \"{CANARY}\"\n"));
    assert!(!refusal(&wrong_method).contains(CANARY));
}

#[test]
fn the_sources_not_available_yet_are_refused_by_name() {
    for source in [
        "keyring = \"retp/azure/secret\"",
        "cmd = [\"pass\", \"show\", \"azure\"]",
        "prompt = true",
    ] {
        let text = file(&format!(
            "[auth]\nmethod = \"oauth-client\"\ntoken_url = \"https://t.example.com/token\"\nclient_id = \"i\"\nclient_secret = {{ {source} }}\n"
        ));
        let message = refusal(&text);
        let name = source.split(' ').next().expect("a name");
        assert!(
            message.contains(name) && message.contains("not available yet"),
            "{message}"
        );
        assert!(message.contains("client_secret"), "{message}");
    }
}

#[test]
fn a_field_the_method_does_not_use_and_a_missing_field_are_refused() {
    let unused =
        file("[auth]\nmethod = \"basic\"\nuser = \"u\"\ntenant = \"t\"\npassword = { env = \"P\" }\n");
    assert!(refusal(&unused).contains("`tenant` is not used by the method `basic`"));

    let missing = file("[auth]\nmethod = \"basic\"\nuser = \"u\"\n");
    assert!(refusal(&missing).contains("needs `password`"));

    let pkce = file("[auth]\nmethod = \"oauth-code-pkce\"\n");
    assert!(refusal(&pkce).contains("not available yet"));

    let bad_tenant = file(
        "[auth]\nmethod = \"azure\"\ntenant = \"a/b\"\nclient_id = \"i\"\nclient_secret = { env = \"S\" }\n",
    );
    assert!(refusal(&bad_tenant).contains("tenant"));
}

#[test]
fn the_schema_is_required() {
    let text = file("[auth]\nmethod = \"none\"\n").replace("schema = 1", "schema = 2");
    assert!(refusal(&text).contains("schema"));
    let text = file("[auth]\nmethod = \"none\"\n").replace("schema = 1\n", "");
    assert!(refusal(&text).contains("schema"));
}

#[test]
fn a_header_that_looks_secret_takes_a_reference_and_not_a_literal() {
    let literal = file("[auth]\nmethod = \"none\"\n[headers]\nx-api-key = \"abc\"\n");
    let message = refusal(&literal);
    assert!(
        message.contains("x-api-key") && message.contains("reference"),
        "{message}"
    );

    let reference =
        file("[auth]\nmethod = \"none\"\n[headers]\nx-api-key = { env = \"KEY\" }\nx-trace = \"on\"\n");
    let entry = Entry::parse(&reference).expect("valid");
    assert_eq!(entry.headers.len(), 2);
}

fn full(auth: AuthSpec) -> Entry {
    let mut server = ServerSpec::etp("wss://h.example.com/etp");
    server.rest_url = Some("https://h.example.com/rest".into());
    server.partition = Some("opendes".into());
    server.tls = Tls::Skip;
    server.handshake = Handshake::Browser;
    server.protected = true;
    server.keepalive = Some(15);
    server.at_once = Some(2);
    server.types = vec!["resqml20.*".into()];
    let mut entry = Entry::new(server, auth);
    entry.description = Some("a \"quoted\" description".into());
    entry.aliases = vec!["az".into(), "azure.test".into()];
    entry.headers = vec![
        Header {
            name: "x-api-key".into(),
            value: HeaderValue::Secret(SecretRef::File("~/keys/api".into())),
        },
        Header {
            name: "x-trace".into(),
            value: HeaderValue::Literal("on".into()),
        },
    ];
    entry.defaults = WriteDefaults {
        legal_tags: vec!["tag-1".into(), "tag-2".into()],
        owners: vec!["data.default.owners@opendes.example.com".into()],
        viewers: vec![],
    };
    entry
}

#[test]
fn what_is_written_is_read_back_for_every_method() {
    let env = |name: &str| SecretRef::Env(name.to_string());
    let methods = [
        AuthSpec::None,
        AuthSpec::Basic {
            user: "u".into(),
            password: env("P"),
        },
        AuthSpec::Bearer { token: env("T") },
        AuthSpec::OauthPassword {
            token_url: "https://t.example.com/token".into(),
            user: "u".into(),
            password: env("P"),
            client_id: Some("c".into()),
            client_secret: Some(env("S")),
            scope: Some("openid".into()),
        },
        AuthSpec::OauthClient {
            token_url: "https://t.example.com/token".into(),
            client_id: "c".into(),
            client_secret: env("S"),
            scope: None,
        },
        AuthSpec::Azure {
            tenant: "00000000-0000-0000-0000-000000000000".into(),
            client_id: "c".into(),
            scope: Some("api://c/.default".into()),
            client_secret: Some(env("S")),
            user: None,
            password: None,
        },
        AuthSpec::Azure {
            tenant: "contoso.onmicrosoft.com".into(),
            client_id: "c".into(),
            scope: None,
            client_secret: None,
            user: Some("u".into()),
            password: Some(SecretRef::File("/keys/p".into())),
        },
        AuthSpec::Cognito {
            region: "us-east-2".into(),
            client_id: "c".into(),
            user: "u".into(),
            password: env("P"),
            client_secret: Some(env("S")),
        },
    ];
    for auth in methods {
        let entry = full(auth);
        let text = entry.to_toml();
        let back = Entry::parse(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert_eq!(back, entry, "{text}");
        assert_eq!(back.server.transport, Transport::Etp);
    }
}

#[test]
fn a_draft_goes_through_the_same_rules() {
    let draft = |mutate: &dyn Fn(&mut Draft)| {
        let mut draft = Draft {
            url: "wss://h.example.com/etp".into(),
            method: Some("basic".into()),
            user: Some("u".into()),
            password_from: Some("env:P".into()),
            ..Draft::default()
        };
        mutate(&mut draft);
        draft.into_entry()
    };
    assert!(draft(&|_| {}).is_ok());

    // The text that was meant to be a reference may be the secret itself: it is not repeated.
    let error = draft(&|d| d.password_from = Some(CANARY.into()))
        .expect_err("refused")
        .to_string();
    assert!(!error.contains(CANARY), "{error}");
    let error = draft(&|d| d.password_from = Some(format!("env:{CANARY} x")))
        .expect_err("refused")
        .to_string();
    assert!(!error.contains(CANARY), "{error}");
    for source in ["keyring:a/b", "cmd:pass show x", "prompt"] {
        let error = draft(&|d| d.password_from = Some(source.into()))
            .expect_err("refused")
            .to_string();
        assert!(error.contains("not available yet"), "{source}: {error}");
    }

    let error = draft(&|d| d.url = format!("wss://u:{CANARY}@h.example.com/"))
        .expect_err("refused")
        .to_string();
    assert!(!error.contains(CANARY), "{error}");
    let error = draft(&|d| d.url = CANARY.into())
        .expect_err("refused")
        .to_string();
    assert!(!error.contains(CANARY), "{error}");
    assert!(
        draft(&|d| d.tenant = Some("t".into())).is_err(),
        "a field the method does not use"
    );
    let error = draft(&|d| d.method = None).expect_err("refused").to_string();
    assert!(
        error.contains("--auth"),
        "a login field without a method: {error}"
    );
}
