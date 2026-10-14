//! **This bridge only bridges the ETP client.**
//!
//! Geometry, export, translation, analysis and the rest of the EnergyML
//! workshops belong to `energyml-rust`'s wheel (or to nobody yet): a dependency
//! from here would bring them into this wheel, its compile time and its public
//! surface. A comment does not hold that boundary, so it is read in the
//! `Cargo.toml`: a dependency added by distraction goes red here.

use std::path::Path;

fn manifest() -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
        .expect("the crate has a Cargo.toml")
}

/// The name on the left of every dependency line, in every dependency table.
fn dependencies(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut inside = false;
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            inside = line.contains("dependencies");
            continue;
        }
        if !inside || line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some((name, _)) = line.split_once('=') {
            out.push(name.trim().to_string());
        }
    }
    out
}

#[test]
fn the_bridge_depends_only_on_the_client_and_what_it_reads_objects_with() {
    let allowed = [
        "retp-client",
        "retp-proto",
        "retp-types",
        "energyml-etp-store",
        "energyml-core",
        "energyml-schemas",
        "energyml-storage",
        "pyo3",
        "numpy",
    ];
    let unexpected: Vec<String> = dependencies(&manifest())
        .into_iter()
        .filter(|name| !allowed.contains(&name.as_str()))
        .collect();
    assert!(
        unexpected.is_empty(),
        "new dependencies: {unexpected:?} - if they belong here, say so in this list and in DESIGN.md"
    );
}

#[test]
fn the_bridge_depends_on_no_energyml_workshop() {
    let found: Vec<String> = dependencies(&manifest())
        .into_iter()
        .filter(|name| {
            [
                "geometry",
                "export",
                "translate",
                "analysis",
                "build",
                "sheet",
                "section",
                "cache",
                "ui",
            ]
            .iter()
            .any(|word| name.contains(word))
        })
        .collect();
    assert!(found.is_empty(), "workshops stay out of this wheel: {found:?}");
}
