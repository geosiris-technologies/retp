//! Names, aliases and the folder they become: a name is a folder name, so
//! `retp server add ../x` is the first test. Collisions with the commands of
//! `retp` are refused at the add.

use retp_catalog::{AuthSpec, Catalog, CatalogError, Entry, Location, Reserved, ServerSpec};

use crate::common::{MemorySecrets, Sandbox};

fn entry() -> Entry {
    Entry::new(ServerSpec::etp("wss://h.example.com/etp"), AuthSpec::None)
}

fn with_aliases(aliases: &[&str]) -> Entry {
    let mut entry = entry();
    entry.aliases = aliases.iter().map(|a| a.to_string()).collect();
    entry
}

fn commands() -> Reserved {
    Reserved::new(
        [
            "probe", "connect", "shell", "quit", "exit", "help", "server", "catalog",
        ]
        .map(String::from),
    )
}

fn folder_names(path: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(path)
        .expect("a folder")
        .flatten()
        .map(|item| item.file_name().to_string_lossy().to_string())
        .collect();
    names.sort();
    names
}

#[test]
fn a_name_that_is_not_a_safe_folder_name_is_refused_and_nothing_is_created() {
    let sandbox = Sandbox::new("names");
    let mut catalog = sandbox.catalog();
    let before = folder_names(&sandbox.root);
    let too_long = "a".repeat(64);
    for name in [
        "",
        "../x",
        "..",
        "a/b",
        "a\\b",
        "A",
        "Azure",
        "CON",
        "con",
        "nul.txt",
        "com1",
        "a..b",
        ".hidden",
        "-x",
        "name.",
        "@x",
        "é",
        "a b",
        too_long.as_str(),
    ] {
        let error = catalog.add(name, entry(), false, &commands()).expect_err(name);
        assert!(matches!(error, CatalogError::Name { .. }), "{name:?}: {error}");
    }
    assert_eq!(
        folder_names(&sandbox.root),
        before,
        "a refused name created something outside the catalogue"
    );
    assert_eq!(folder_names(&sandbox.servers()), [".gitignore"]);
}

#[test]
fn the_names_a_server_may_have() {
    let sandbox = Sandbox::new("good-names");
    let mut catalog = sandbox.catalog();
    let longest = "a".repeat(63);
    for name in ["azure-test", "a", "0x", "a.b_c-d", longest.as_str()] {
        catalog.add(name, entry(), false, &commands()).expect(name);
        assert!(sandbox.servers().join(name).join("server.toml").is_file());
    }
}

#[test]
fn a_command_of_retp_cannot_be_a_name_or_an_alias() {
    let sandbox = Sandbox::new("reserved");
    let mut catalog = sandbox.catalog();
    for word in ["probe", "connect", "shell", "quit", "help", "server"] {
        let error = catalog.add(word, entry(), false, &commands()).expect_err(word);
        assert_eq!(error, CatalogError::Reserved { name: word.into() });
        let error = catalog
            .add("fine", with_aliases(&[word]), false, &commands())
            .expect_err(word);
        assert_eq!(error, CatalogError::Reserved { name: word.into() });
    }
    assert!(!sandbox.servers().join("fine").exists());
}

#[test]
fn names_and_aliases_share_one_namespace() {
    let sandbox = Sandbox::new("collisions");
    let mut catalog = sandbox.catalog();
    catalog
        .add("azure", with_aliases(&["az"]), false, &commands())
        .expect("added");

    let error = catalog
        .add("az", entry(), false, &commands())
        .expect_err("an alias is taken");
    assert_eq!(
        error,
        CatalogError::Taken {
            word: "az".into(),
            by: "azure".into()
        }
    );
    let error = catalog
        .add("other", with_aliases(&["azure"]), false, &commands())
        .expect_err("a name is taken");
    assert_eq!(
        error,
        CatalogError::Taken {
            word: "azure".into(),
            by: "azure".into()
        }
    );
    let error = catalog
        .add("other", with_aliases(&["az"]), false, &commands())
        .expect_err("an alias is taken");
    assert_eq!(
        error,
        CatalogError::Taken {
            word: "az".into(),
            by: "azure".into()
        }
    );

    let error = catalog
        .add("azure", entry(), false, &commands())
        .expect_err("exists");
    assert_eq!(error, CatalogError::Exists { name: "azure".into() });
    catalog
        .add("azure", with_aliases(&["az", "az2"]), true, &commands())
        .expect("replaced, and it may keep its own alias");
    assert_eq!(catalog.find("az2").expect("found").name, "azure");
}

#[test]
fn a_server_is_found_by_name_or_alias_and_an_unknown_word_lists_near_ones() {
    let sandbox = Sandbox::new("find");
    let mut catalog = sandbox.catalog();
    catalog
        .add("azure", with_aliases(&["az"]), false, &commands())
        .expect("added");
    catalog
        .add("lab-test", entry(), false, &commands())
        .expect("added");
    assert_eq!(catalog.find("az").expect("alias").name, "azure");
    assert_eq!(catalog.find("azure").expect("name").name, "azure");
    match catalog.find("azur").expect_err("unknown") {
        CatalogError::Unknown { word, near } => {
            assert_eq!(word, "azur");
            assert!(near.contains(&"azure".to_string()), "{near:?}");
            assert!(!near.contains(&"lab-test".to_string()), "{near:?}");
        }
        other => panic!("{other}"),
    }
}

#[test]
fn what_was_added_is_there_when_the_catalogue_is_opened_again() {
    let sandbox = Sandbox::new("reopen");
    {
        let mut catalog = sandbox.catalog();
        catalog
            .add("azure", with_aliases(&["az"]), false, &commands())
            .expect("added");
    }
    let catalog = Catalog::open(&sandbox.location()).expect("opened");
    assert_eq!(catalog.find("az").expect("found").entry, &with_aliases(&["az"]));
    let rows = catalog.listed(&MemorySecrets::of(&[]));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "azure");
    assert_eq!(rows[0].url, "wss://h.example.com/etp");
}

#[test]
fn removing_a_server_removes_its_folder_and_clears_the_default() {
    let sandbox = Sandbox::new("remove");
    let mut catalog = sandbox.catalog();
    catalog
        .add("azure", with_aliases(&["az"]), false, &commands())
        .expect("added");
    catalog.add("other", entry(), false, &commands()).expect("added");
    catalog.set_default("az").expect("default");
    assert_eq!(catalog.default_entry().expect("a default").name, "azure");
    let state = catalog.state_dir("azure").expect("state");
    assert!(state.is_dir());

    catalog.remove("az").expect("removed");
    assert!(!sandbox.servers().join("azure").exists());
    assert!(sandbox.servers().join("other").is_dir());
    assert!(catalog.default_entry().is_none());
    assert!(catalog.find("az").is_err());
    let reopened = Catalog::open(&sandbox.location()).expect("opened");
    assert!(reopened.default_entry().is_none());
}

#[test]
fn the_default_server_is_kept_in_the_configuration() {
    let sandbox = Sandbox::new("default");
    let mut catalog = sandbox.catalog();
    catalog
        .add("azure", with_aliases(&["az"]), false, &commands())
        .expect("added");
    assert!(catalog.default_entry().is_none());
    assert!(catalog.set_default("nope").is_err());
    catalog.set_default("az").expect("default");
    let reopened = Catalog::open(&sandbox.location()).expect("opened");
    assert_eq!(reopened.default_entry().expect("a default").name, "azure");
}

#[test]
fn a_server_whose_file_is_wrong_does_not_stop_the_others() {
    let sandbox = Sandbox::new("broken");
    let mut catalog = sandbox.catalog();
    catalog.add("good", entry(), false, &commands()).expect("added");
    let dir = sandbox.servers().join("bad");
    std::fs::create_dir_all(&dir).expect("folder");
    std::fs::write(
        dir.join("server.toml"),
        "schema = 1\n[auth]\nmethod = \"basic\"\npassword = \"not-a-reference\"\n",
    )
    .expect("file");
    let catalog = Catalog::open(&sandbox.location()).expect("opened");
    assert!(catalog.find("good").is_ok());
    let error = catalog.find("bad").expect_err("unusable");
    assert!(matches!(error, CatalogError::Broken { .. }), "{error}");
    assert!(!error.to_string().contains("not-a-reference"));
    let rows = catalog.listed(&MemorySecrets::of(&[]));
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|r| r.name == "bad" && r.problem.is_some()));
}

#[test]
fn the_location_comes_from_the_environment_then_the_os() {
    let sandbox = Sandbox::new("locate");
    let config = sandbox.root.join("elsewhere.toml");
    let catalog_dir = sandbox.root.join("folder");
    let (c, d) = (
        config.to_str().expect("utf-8").to_string(),
        catalog_dir.to_str().expect("utf-8").to_string(),
    );
    let location = Catalog::locate_with(&|name| match name {
        "RETP_CONFIG" => Some(c.clone()),
        "RETP_CATALOG" => Some(d.clone()),
        _ => None,
    })
    .expect("located");
    assert_eq!(location.config_file(), config);
    assert_eq!(location.catalog_dir(), Some(catalog_dir.as_path()));

    // With no variable the OS decides; a machine with no home folder has no answer.
    if let Ok(os) = Catalog::locate_with(&|_| None) {
        assert!(os.config_file().ends_with("config.toml"));
        assert!(os.catalog_dir().is_none());
    }
}

#[test]
fn without_a_configuration_nothing_is_created_and_the_error_says_how_to_start() {
    let sandbox = Sandbox::new("uninitialised");
    let location = sandbox.location();
    match Catalog::open(&location) {
        Err(CatalogError::NotInitialised { config_file }) => assert_eq!(config_file, location.config_file()),
        other => panic!("{:?}", other.map(|_| ())),
    }
    assert!(!sandbox.servers().exists());
    assert!(
        CatalogError::NotInitialised {
            config_file: location.config_file().to_path_buf()
        }
        .to_string()
        .contains("retp catalog init")
    );

    // A folder named by the environment is enough, with no configuration file.
    std::fs::create_dir_all(sandbox.servers()).expect("folder");
    let by_env = Location::new(location.config_file().to_path_buf(), Some(sandbox.servers()));
    assert!(Catalog::open(&by_env).is_ok());
}

#[test]
fn init_refuses_to_overwrite_a_configuration_and_keeps_local_state_out_of_git() {
    let sandbox = Sandbox::new("init");
    let _catalog = sandbox.catalog();
    let again = Catalog::init(&sandbox.location(), &sandbox.root.join("other"), false);
    assert!(matches!(again, Err(CatalogError::Config { .. })));
    assert!(!sandbox.root.join("other").exists());
    let ignore = std::fs::read_to_string(sandbox.servers().join(".gitignore")).expect("a .gitignore");
    assert!(ignore.contains("*/state/"));
    assert!(Catalog::init(&sandbox.location(), &sandbox.root.join("other"), true).is_ok());
}
