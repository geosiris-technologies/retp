//! Where a secret is read from: `env:` and `file:`, and the sources refused by
//! name. Rules about the sources, on files and variables made here.

use retp_catalog::{CatalogError, OsSecrets, Secret, SecretError, SecretRef, SecretSource};

use crate::common::{CANARY, Sandbox};

fn env_of(pairs: &'static [(&'static str, &'static str)]) -> OsSecrets {
    OsSecrets::with_env(move |name| pairs.iter().find(|(k, _)| *k == name).map(|(_, v)| v.to_string()))
}

#[test]
fn a_variable_that_is_missing_or_empty_is_an_error_naming_it() {
    let source = env_of(&[("EMPTY", ""), ("SET", "value")]);
    for name in ["NOPE", "EMPTY"] {
        let error = source.read(&SecretRef::Env(name.into())).expect_err("no value");
        assert_eq!(
            error,
            SecretError::EnvMissing {
                variable: name.into()
            }
        );
        assert!(error.to_string().contains(name));
    }
    let secret = source.read(&SecretRef::Env("SET".into())).expect("a value");
    assert_eq!(secret.expose(), "value");
}

#[test]
fn a_file_loses_one_final_line_ending_and_not_two() {
    let sandbox = Sandbox::new("newline");
    let source = OsSecrets::with_env(|_| None);
    for (contents, expected) in [
        ("s3cr3t\n", "s3cr3t"),
        ("s3cr3t\r\n", "s3cr3t"),
        ("s3cr3t\n\n", "s3cr3t\n"),
        ("s3cr3t", "s3cr3t"),
    ] {
        let path = sandbox.secret_file("k", contents);
        let secret = source
            .read(&SecretRef::File(path.to_str().expect("utf-8").into()))
            .expect("read");
        assert_eq!(secret.expose(), expected, "{contents:?}");
    }
}

#[test]
fn a_missing_empty_or_non_utf8_file_is_an_error_naming_the_path() {
    let sandbox = Sandbox::new("bad-files");
    let source = OsSecrets::with_env(|_| None);
    let missing = sandbox.secrets().join("absent");
    let reference = SecretRef::File(missing.to_str().expect("utf-8").into());
    assert!(matches!(
        source.read(&reference),
        Err(SecretError::FileMissing { .. })
    ));
    assert!(!source.inspect(&reference).resolves);

    let empty = sandbox.secret_file("empty", "\n");
    let reference = SecretRef::File(empty.to_str().expect("utf-8").into());
    assert!(matches!(source.read(&reference), Err(SecretError::Empty { .. })));

    let binary = sandbox.secrets().join("binary");
    std::fs::write(&binary, [0xff, 0xfe, 0x41]).expect("written");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o600)).expect("mode");
    }
    let reference = SecretRef::File(binary.to_str().expect("utf-8").into());
    assert!(matches!(
        source.read(&reference),
        Err(SecretError::FileUnreadable { .. })
    ));
}

#[test]
fn the_home_folder_is_expanded_from_a_tilde_and_from_home() {
    let sandbox = Sandbox::new("home");
    sandbox.secret_file("k", "from-home\n");
    let home = sandbox.root.to_str().expect("utf-8").to_string();
    let source = OsSecrets::with_env(move |name| (name == "HOME").then(|| home.clone()));
    for spelling in ["~/secrets/k", "${HOME}/secrets/k"] {
        let secret = source.read(&SecretRef::File(spelling.into())).expect("read");
        assert_eq!(secret.expose(), "from-home", "{spelling}");
    }
}

#[cfg(unix)]
#[test]
fn a_file_others_can_read_is_refused_with_its_mode() {
    use std::os::unix::fs::PermissionsExt;
    let sandbox = Sandbox::new("mode");
    let path = sandbox.secret_file("k", "value\n");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("mode");
    let source = OsSecrets::with_env(|_| None);
    let reference = SecretRef::File(path.to_str().expect("utf-8").into());
    let error = source.read(&reference).expect_err("refused");
    assert_eq!(
        error,
        SecretError::FileTooOpen {
            path: path.to_str().expect("utf-8").into(),
            mode: "0644".into()
        }
    );
    let status = source.inspect(&reference);
    assert!(!status.resolves && status.detail.contains("0644") && status.detail.contains("0600"));
}

#[test]
fn a_reference_is_written_and_read_the_same_on_the_command_line() {
    for text in [
        "env:RETP_AZURE_TEST_SECRET",
        "file:~/keys/azure.secret",
        "file:C:\\keys\\k",
    ] {
        let parsed = SecretRef::parse(text).expect("a reference");
        assert_eq!(parsed.to_text(), text);
    }
}

#[test]
fn what_is_not_a_reference_is_refused_without_being_repeated() {
    let with_colon = format!("{CANARY}:x");
    let bad_name = format!("env:{CANARY}!");
    for text in [
        CANARY,
        with_colon.as_str(),
        bad_name.as_str(),
        "env:",
        "file:",
        "env:1abc",
    ] {
        let error = SecretRef::parse(text).expect_err("refused");
        assert!(matches!(error, CatalogError::Reference { .. }));
        assert!(!error.to_string().contains(CANARY), "{error}");
    }
    for text in ["keyring:retp/azure/secret", "cmd:pass show azure", "prompt"] {
        let error = SecretRef::parse(text).expect_err("refused").to_string();
        assert!(error.contains("not available yet"), "{text}: {error}");
    }
}

#[test]
fn a_secret_shows_nothing() {
    let secret = Secret::new(CANARY.to_string());
    assert!(!format!("{secret:?}").contains(CANARY));
    assert!(!format!("{:#?}", Some(&secret)).contains(CANARY));
    assert!(!secret.is_empty());
}

/// The value leaves its `Secret` at one place. This counts the calls in the
/// sources of the crate (the sources of this repository, compiled in), so that
/// a second site is a test failing and not a review remembering.
#[test]
fn the_value_leaves_a_secret_at_one_site() {
    let sources = [
        include_str!("../src/catalog.rs"),
        include_str!("../src/check.rs"),
        include_str!("../src/entry.rs"),
        include_str!("../src/error.rs"),
        include_str!("../src/expand.rs"),
        include_str!("../src/lib.rs"),
        include_str!("../src/observe.rs"),
        include_str!("../src/render.rs"),
        include_str!("../src/resolve.rs"),
        include_str!("../src/util.rs"),
    ];
    let calls: usize = sources.iter().map(|text| text.matches(".expose()").count()).sum();
    assert_eq!(
        calls, 1,
        "`Secret::expose` is called at one site (resolve.rs, `reveal`)"
    );
}
