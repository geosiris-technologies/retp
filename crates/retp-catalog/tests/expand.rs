//! `retp azure`, `retp @azure`, `retp --server azure`: the rewrite of the
//! command line before the parser sees it. The commands are those of a small
//! grammar written here; the real one is read from the parser by the command
//! line, and a test there compares the two.

use std::ffi::OsString;

use retp_catalog::{
    Accepts, AuthSpec, Catalog, CatalogError, Entry, Expanded, Grammar, Handshake, Reserved, ServerSpec, Tls,
    WriteDefaults, expand,
};

use crate::common::{CANARY, Sandbox};

const ETP: &str = "wss://adme.example.com/api/etp/v2/";
const REST: &str = "https://adme.example.com/api/rest/v2";

fn grammar() -> Grammar {
    let commands = [
        "probe",
        "connect",
        "dataspaces",
        "resources",
        "get",
        "shell",
        "create-dataspace",
        "delete-dataspace",
        "push",
        "server",
        "catalog",
        "help",
        "mcp",
    ];
    Grammar {
        subcommands: true,
        commands: Reserved::new(commands.map(String::from)),
        without_server: ["server", "catalog", "help"].map(String::from).to_vec(),
        writing: ["create-dataspace", "delete-dataspace", "push"]
            .map(String::from)
            .to_vec(),
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

fn flat() -> Grammar {
    Grammar {
        subcommands: false,
        commands: Reserved::default(),
        without_server: Vec::new(),
        writing: Vec::new(),
        default_command: String::new(),
        create_command: String::new(),
        accepts: Accepts {
            as_browser: false,
            keepalive: false,
            at_once: false,
            types: true,
            offer: false,
        },
        excludes_insecure: vec!["--tls-fallback".into()],
    }
}

fn azure() -> Entry {
    let mut server = ServerSpec::etp(ETP);
    server.rest_url = Some(REST.into());
    server.partition = Some("opendes".into());
    server.tls = Tls::Skip;
    let mut entry = Entry::new(server, AuthSpec::None);
    entry.aliases = vec!["az".into()];
    entry
}

fn catalog_with(sandbox: &Sandbox, servers: &[(&str, Entry)]) -> Result<Catalog, CatalogError> {
    let mut catalog = sandbox.catalog();
    let reserved = Reserved::default();
    for (name, entry) in servers {
        catalog.add(name, entry.clone(), false, &reserved).expect("added");
    }
    Ok(catalog)
}

fn words(list: &[&str]) -> Vec<OsString> {
    list.iter().map(OsString::from).collect()
}

fn strings(expanded: &Expanded) -> Vec<String> {
    expanded
        .argv
        .iter()
        .map(|a| a.to_string_lossy().to_string())
        .collect()
}

fn run(list: &[&str], catalog: &Result<Catalog, CatalogError>) -> Result<Vec<String>, CatalogError> {
    expand(&words(list), catalog, &grammar()).map(|e| strings(&e))
}

fn count(args: &[String], option: &str) -> usize {
    args.iter().filter(|a| *a == option).count()
}

fn value_of<'a>(args: &'a [String], option: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == option)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

#[test]
fn a_bare_server_name_opens_the_shell_on_it() {
    let sandbox = Sandbox::new("bare");
    let catalog = catalog_with(&sandbox, &[("azure", azure())]);
    let expanded = expand(&words(&["retp", "azure"]), &catalog, &grammar()).expect("expanded");
    assert_eq!(expanded.entry.as_deref(), Some("azure"));
    assert!(!expanded.via_default);
    assert_eq!(
        strings(&expanded),
        [
            "retp",
            "shell",
            ETP,
            "--catalog-entry",
            "azure",
            "--partition",
            "opendes",
            "--insecure"
        ]
    );
}

#[test]
fn every_way_to_name_a_server_gives_the_same_line() {
    let sandbox = Sandbox::new("equivalent");
    let catalog = catalog_with(&sandbox, &[("azure", azure())]);
    let expected = run(&["retp", "azure", "dataspaces"], &catalog).expect("expanded");
    assert_eq!(
        expected[..5],
        ["retp", "dataspaces", ETP, "--catalog-entry", "azure"]
    );
    for line in [
        &["retp", "@azure", "dataspaces"][..],
        &["retp", "--server", "azure", "dataspaces"],
        &["retp", "-s", "azure", "dataspaces"],
        &["retp", "dataspaces", "--server", "azure"],
        &["retp", "dataspaces", "--server=azure"],
        &["retp", "dataspaces", "-sazure"],
        &["retp", "--serveur", "azure", "dataspaces"],
        &["retp", "--server", "az", "dataspaces"],
        &["retp", "az", "dataspaces"],
        &["retp", "@az", "dataspaces"],
        &["retp", "--server", "@azure", "dataspaces"],
    ] {
        assert_eq!(run(line, &catalog).expect("expanded"), expected, "{line:?}");
    }
}

#[test]
fn the_command_line_wins_over_the_entry() {
    let sandbox = Sandbox::new("line-wins");
    let catalog = catalog_with(&sandbox, &[("azure", azure())]);
    let line = run(
        &["retp", "azure", "dataspaces", "--partition", "mine", "--insecure"],
        &catalog,
    )
    .expect("expanded");
    assert_eq!(value_of(&line, "--partition"), Some("mine"));
    assert_eq!(count(&line, "--partition"), 1);
    assert_eq!(count(&line, "--insecure"), 1);
    let line = run(&["retp", "azure", "dataspaces", "--partition=mine"], &catalog).expect("expanded");
    assert_eq!(count(&line, "--partition"), 0, "the `=` form is the line's too");
}

#[test]
fn the_url_comes_before_the_commands_own_positionals() {
    let sandbox = Sandbox::new("positional");
    let catalog = catalog_with(&sandbox, &[("azure", azure())]);
    let line = run(
        &["retp", "azure", "get", "eml:///dataspace('demo')", "--out", "dir"],
        &catalog,
    )
    .expect("expanded");
    assert_eq!(line[1], "get");
    assert_eq!(line[2], ETP);
    let uri = line
        .iter()
        .position(|a| a == "eml:///dataspace('demo')")
        .expect("kept");
    assert!(uri > 4);
    assert_eq!(line.last().map(String::as_str), Some("dir"));
}

#[test]
fn a_built_in_command_wins_over_a_server_of_the_same_name_and_at_sign_wins_back() {
    let sandbox = Sandbox::new("priority");
    // A server called `probe` can only come from a hand-made folder: `server add` refuses the name.
    let mut catalog = sandbox.catalog();
    catalog
        .add("azure", azure(), false, &Reserved::default())
        .expect("added");
    let dir = sandbox.servers().join("probe");
    std::fs::create_dir_all(&dir).expect("folder");
    std::fs::write(dir.join("server.toml"), azure().to_toml()).expect("file");
    let catalog = Catalog::open(&sandbox.location());

    let plain = expand(
        &words(&["retp", "probe", "wss://x.example.com/etp"]),
        &catalog,
        &grammar(),
    )
    .expect("expanded");
    assert!(plain.entry.is_none(), "the command ran, not the server");
    assert_eq!(strings(&plain), ["retp", "probe", "wss://x.example.com/etp"]);

    let named = expand(&words(&["retp", "@probe", "dataspaces"]), &catalog, &grammar()).expect("expanded");
    assert_eq!(named.entry.as_deref(), Some("probe"));
}

#[test]
fn an_unknown_word_lists_the_commands_and_servers_that_look_like_it() {
    let sandbox = Sandbox::new("unknown");
    let catalog = catalog_with(&sandbox, &[("azure", azure())]);
    match run(&["retp", "dataspces"], &catalog).expect_err("unknown") {
        CatalogError::UnknownWord { word, near } => {
            assert_eq!(word, "dataspces");
            assert!(near.contains(&"dataspaces".to_string()), "{near:?}");
        }
        other => panic!("{other}"),
    }
    match run(&["retp", "azur"], &catalog).expect_err("unknown") {
        CatalogError::UnknownWord { near, .. } => assert!(near.contains(&"azure".to_string()), "{near:?}"),
        other => panic!("{other}"),
    }
    assert!(
        run(&["retp", "azure", "dataspces"], &catalog).is_err(),
        "a server then a mistyped command"
    );
}

#[test]
fn login_options_and_urls_are_refused_with_a_catalogue_server() {
    let sandbox = Sandbox::new("mix");
    let catalog = catalog_with(&sandbox, &[("azure", azure())]);
    for option in [
        "--token",
        "--user",
        "--password",
        "--token-url",
        "--azure-tenant",
        "--cognito-region",
        "--client-id",
        "--client-secret",
        "--oauth-scope",
    ] {
        for line in [
            vec!["retp", "azure", "dataspaces", option, CANARY],
            vec![
                "retp",
                "--server",
                "azure",
                "dataspaces",
                &format!("{option}={CANARY}"),
            ],
        ] {
            let error = run(&line, &catalog).expect_err(option).to_string();
            assert!(error.contains(option), "{error}");
            assert!(!error.contains(CANARY), "the value is repeated: {error}");
        }
    }
    let error = run(
        &["retp", "azure", "dataspaces", "wss://other.example.com/etp"],
        &catalog,
    )
    .expect_err("two places");
    assert!(error.to_string().contains("URL"));
    assert!(run(&["retp", "--server"], &catalog).is_err());
    assert!(
        run(
            &["retp", "--server", "azure", "--server", "az", "dataspaces"],
            &catalog
        )
        .is_err()
    );
}

#[test]
fn rest_is_the_entrys_rest_url_and_is_refused_when_it_has_none() {
    let sandbox = Sandbox::new("rest");
    let mut only_rest = azure();
    only_rest.server.transport = retp_catalog::Transport::Rest;
    only_rest.aliases.clear();
    let etp_only = Entry::new(ServerSpec::etp(ETP), AuthSpec::None);
    let catalog = catalog_with(
        &sandbox,
        &[("azure", azure()), ("restful", only_rest), ("plain", etp_only)],
    );

    let line = run(&["retp", "azure", "dataspaces", "--rest"], &catalog).expect("expanded");
    assert_eq!(line[2], REST);
    assert_eq!(count(&line, "--rest"), 1);

    let line = run(&["retp", "restful", "dataspaces"], &catalog).expect("expanded");
    assert_eq!(line[2], REST);
    assert_eq!(count(&line, "--rest"), 1);
    assert_eq!(
        count(&line, "--as-browser") + count(&line, "--keepalive"),
        0,
        "REST refuses them"
    );

    let error = run(&["retp", "plain", "dataspaces", "--rest"], &catalog).expect_err("no REST URL");
    assert!(matches!(error, CatalogError::NoUrl { .. }), "{error}");
}

#[test]
fn the_options_an_entry_carries_are_added_unless_the_line_gives_its_own() {
    let sandbox = Sandbox::new("options");
    let mut busy = Entry::new(ServerSpec::etp(ETP), AuthSpec::None);
    busy.server.handshake = Handshake::Browser;
    busy.server.keepalive = Some(15);
    busy.server.at_once = Some(2);
    busy.server.types = vec!["resqml20.*".into()];
    let catalog = catalog_with(&sandbox, &[("busy", busy)]);

    let line = run(&["retp", "busy", "dataspaces"], &catalog).expect("expanded");
    assert_eq!(count(&line, "--as-browser"), 1);
    assert_eq!(value_of(&line, "--keepalive"), Some("15"));
    assert_eq!(value_of(&line, "--at-once"), Some("2"));
    assert_eq!(value_of(&line, "--type"), Some("resqml20.*"));

    let line = run(
        &[
            "retp",
            "busy",
            "dataspaces",
            "--keepalive",
            "0",
            "--offer",
            "o.json",
        ],
        &catalog,
    )
    .expect("expanded");
    assert_eq!(value_of(&line, "--keepalive"), Some("0"));
    assert_eq!(count(&line, "--keepalive"), 1);
    assert_eq!(count(&line, "--type"), 0, "an offer file replaces the types");
}

#[test]
fn the_default_server_is_used_for_reads_and_refused_for_writes() {
    let sandbox = Sandbox::new("default");
    let mut catalog = sandbox.catalog();
    catalog
        .add("azure", azure(), false, &Reserved::default())
        .expect("added");
    catalog.set_default("azure").expect("default");
    let catalog = Ok(catalog);

    let expanded = expand(&words(&["retp", "dataspaces"]), &catalog, &grammar()).expect("expanded");
    assert!(expanded.via_default);
    assert_eq!(expanded.entry.as_deref(), Some("azure"));

    for write in [
        &["retp", "push", "a.epc", "demo"][..],
        &["retp", "create-dataspace", "demo"],
        &["retp", "delete-dataspace", "demo"],
    ] {
        let error = run(write, &catalog)
            .expect_err("a write never goes to the default")
            .to_string();
        assert!(
            error.contains("default server") && error.contains(write[1]),
            "{error}"
        );
    }
    // Named, or with a URL, or not about a server: the default is not involved.
    assert!(run(&["retp", "push", "--server", "azure", "a.epc", "demo"], &catalog).is_ok());
    let with_url = expand(
        &words(&["retp", "dataspaces", "wss://x.example.com/etp"]),
        &catalog,
        &grammar(),
    )
    .expect("expanded");
    assert!(with_url.entry.is_none() && !with_url.via_default);
    for line in [
        &["retp", "server", "list"][..],
        &["retp", "help"],
        &["retp", "--version"],
        &["retp"],
    ] {
        let expanded = expand(&words(line), &catalog, &grammar()).expect("expanded");
        assert!(expanded.entry.is_none(), "{line:?}");
    }
}

#[test]
fn a_dataspace_created_on_a_server_gets_its_write_defaults_unless_the_line_says_otherwise() {
    let sandbox = Sandbox::new("defaults");
    let mut entry = azure();
    entry.defaults = WriteDefaults {
        legal_tags: vec!["tag-1".into(), "tag-2".into()],
        owners: vec!["owners@example.com".into()],
        viewers: vec![],
    };
    let catalog = catalog_with(&sandbox, &[("azure", entry)]);

    let line = run(&["retp", "azure", "create-dataspace", "demo"], &catalog).expect("expanded");
    let customs: Vec<&str> = line
        .iter()
        .enumerate()
        .filter(|(_, a)| *a == "--custom")
        .map(|(i, _)| line[i + 1].as_str())
        .collect();
    assert_eq!(customs, ["legaltags=tag-1,tag-2", "owners=owners@example.com"]);

    let line = run(
        &[
            "retp",
            "azure",
            "create-dataspace",
            "demo",
            "--custom",
            "owners=mine",
        ],
        &catalog,
    )
    .expect("expanded");
    assert!(line.iter().filter(|a| a.starts_with("owners=")).count() == 1);
    assert!(line.contains(&"legaltags=tag-1,tag-2".to_string()));

    let line = run(&["retp", "azure", "dataspaces"], &catalog).expect("expanded");
    assert_eq!(count(&line, "--custom"), 0, "only the command that creates");
}

#[test]
fn without_a_catalogue_a_line_that_does_not_use_it_is_left_alone() {
    let none: Result<Catalog, CatalogError> = Err(CatalogError::NotInitialised {
        config_file: "config.toml".into(),
    });
    let line = run(&["retp", "dataspaces", "wss://x.example.com/etp"], &none).expect("untouched");
    assert_eq!(line, ["retp", "dataspaces", "wss://x.example.com/etp"]);
    let error = run(&["retp", "--server", "azure", "dataspaces"], &none).expect_err("needs the catalogue");
    assert!(matches!(error, CatalogError::NotInitialised { .. }));
    assert!(run(&["retp", "azure"], &none).is_err());
}

#[test]
fn a_program_with_one_command_gets_the_url_as_its_first_positional() {
    let sandbox = Sandbox::new("flat");
    let mut entry = azure();
    entry.server.handshake = Handshake::Browser;
    entry.server.keepalive = Some(30);
    let catalog = catalog_with(&sandbox, &[("azure", entry)]);
    let go = |list: &[&str]| expand(&words(list), &catalog, &flat()).map(|e| strings(&e));

    let expected = go(&["retp-conformance", "--server", "azure", "--write"]).expect("expanded");
    assert_eq!(
        expected[..4],
        ["retp-conformance", ETP, "--catalog-entry", "azure"]
    );
    assert_eq!(
        count(&expected, "--as-browser") + count(&expected, "--keepalive"),
        0,
        "it does not accept them"
    );
    assert_eq!(expected.last().map(String::as_str), Some("--write"));
    assert_eq!(
        go(&["retp-conformance", "@azure", "--write"]).expect("expanded"),
        expected
    );
    assert_eq!(
        go(&["retp-conformance", "-s", "azure", "--write"]).expect("expanded"),
        expected
    );

    // `--tls-fallback` checks the certificate first: the entry's `skip` does not turn it off.
    let line = go(&["retp-conformance", "--server", "azure", "--tls-fallback"]).expect("expanded");
    assert_eq!(count(&line, "--insecure"), 0);
    // No server named: the line is the program's own.
    let line = go(&["retp-conformance", "wss://x.example.com/etp", "--write"]).expect("untouched");
    assert_eq!(line, ["retp-conformance", "wss://x.example.com/etp", "--write"]);
    assert!(go(&["retp-conformance", "--server", "azure", "wss://x.example.com/etp"]).is_err());
}
