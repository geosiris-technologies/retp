//! The capability names we use, held against the three official enumerations.
//!
//! Two independent sources of one fact. The official side is the Avro
//! enumerations `EndpointCapabilityKind`, `ProtocolCapabilityKind` and
//! `DataObjectCapabilityKind` (chapter 23 of the ETP v1.2 specification),
//! generated from the `.avsc` of the Energistics `etpv1` repository. Ours are
//! the hand-written `rc/etp-1.2/capabilities.json` and the public `DEFINITIONS`
//! table and `names` constants of `retp_proto::capabilities`. A name we use that the enumeration
//! does not hold would travel on the wire as a capability no peer knows.
//!
//! The generated enums expose no list of their symbols, so they are enumerated
//! through the Avro codec itself: ordinal 0, 1, 2 … is decoded until the codec
//! refuses, and each value's `Debug` name is the symbol.

use std::collections::BTreeSet;
use std::path::PathBuf;

use retp_avro::{Avro, Reader};
use retp_proto::capabilities::{DEFINITIONS, Scope};
use retp_types::datatypes::{DataObjectCapabilityKind, EndpointCapabilityKind, ProtocolCapabilityKind};
use serde_json::Value;

/// The symbols of an official enumeration, in ordinal order.
fn symbols<T: Avro + std::fmt::Debug>() -> Vec<String> {
    let mut out = Vec::new();
    for ordinal in 0u8..64 {
        // Avro zig-zag encoding of a small non-negative long: twice the value.
        let bytes = [ordinal * 2];
        match T::read(&mut Reader::new(&bytes)) {
            Ok(value) => out.push(format!("{value:?}")),
            Err(_) => break,
        }
    }
    out
}

/// The names in `ours` that `official` does not hold.
fn unknown_to(official: &[String], ours: &BTreeSet<String>) -> Vec<String> {
    ours.iter()
        .filter(|name| !official.contains(name))
        .cloned()
        .collect()
}

/// The names in `official` that `ours` lacks.
fn missing_from(official: &[String], ours: &BTreeSet<String>) -> Vec<String> {
    official
        .iter()
        .filter(|name| !ours.contains(*name))
        .cloned()
        .collect()
}

fn declared() -> Value {
    // `ETP_SCHEMAS` names the schema root; the default is where it lives today.
    let root = std::env::var_os("ETP_SCHEMAS")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../rc/etp-1.2"));
    let path = root.join("capabilities.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|why| panic!("{}: {why}", path.display()));
    serde_json::from_str(&text).expect("capabilities.json is JSON")
}

/// The `(name, scopes)` pairs of the public `DEFINITIONS` table of `retp-proto`.
fn used_in_definitions() -> Vec<(String, Vec<Scope>)> {
    DEFINITIONS
        .iter()
        .map(|d| (d.name.to_string(), d.scopes.to_vec()))
        .collect()
}

fn scope_key(scope: Scope) -> &'static str {
    match scope {
        Scope::Endpoint => "endpoint",
        Scope::Protocol => "protocol",
        Scope::DataObject => "dataObject",
    }
}

fn official(key: &str) -> Vec<String> {
    match key {
        "endpoint" => symbols::<EndpointCapabilityKind>(),
        "protocol" => symbols::<ProtocolCapabilityKind>(),
        "dataObject" => symbols::<DataObjectCapabilityKind>(),
        other => panic!("unknown kind {other}"),
    }
}

#[test]
fn the_official_enumerations_are_read_whole() {
    // Counts of the specification's tables (ETP v1.2, 23.3 to 23.5), so a codec
    // that stopped early could not make the other tests pass on an empty list.
    assert_eq!(official("endpoint").len(), 17);
    assert_eq!(official("protocol").len(), 13);
    assert_eq!(official("dataObject").len(), 8);
    assert!(official("dataObject").contains(&"MaxContainedDataObjectCount".to_string()));
}

#[test]
fn declared_kinds_are_the_official_enumerations() {
    let table = declared();
    for key in ["endpoint", "protocol", "dataObject"] {
        let ours: BTreeSet<String> = table["kinds"][key]
            .as_array()
            .expect("kinds list")
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        let official = official(key);
        assert_eq!(
            unknown_to(&official, &ours),
            Vec::<String>::new(),
            "{key}: names capabilities.json uses that the official enum lacks"
        );
        assert_eq!(
            missing_from(&official, &ours),
            Vec::<String>::new(),
            "{key}: official symbols capabilities.json lacks"
        );
    }
}

#[test]
fn described_capabilities_are_official_in_each_scope() {
    let table = declared();
    for key in ["endpoint", "protocol", "dataObject"] {
        let ours: BTreeSet<String> = table["capabilities"]
            .as_array()
            .expect("capabilities list")
            .iter()
            .filter(|c| c["scopes"].as_array().unwrap().iter().any(|s| s == key))
            .map(|c| c["name"].as_str().unwrap().to_string())
            .collect();
        assert!(!ours.is_empty(), "{key}: nothing compared");
        assert_eq!(unknown_to(&official(key), &ours), Vec::<String>::new(), "{key}");
    }
}

#[test]
fn definitions_are_official_in_each_scope() {
    let used = used_in_definitions();
    assert!(used.len() > 30, "DEFINITIONS holds {} entries", used.len());
    for key in ["endpoint", "protocol", "dataObject"] {
        let ours: BTreeSet<String> = used
            .iter()
            .filter(|(_, scopes)| scopes.iter().any(|s| scope_key(*s) == key))
            .map(|(name, _)| name.clone())
            .collect();
        assert!(!ours.is_empty(), "{key}: nothing compared");
        assert_eq!(
            unknown_to(&official(key), &ours),
            Vec::<String>::new(),
            "{key}: DEFINITIONS names the official enum lacks"
        );
        assert_eq!(
            missing_from(&official(key), &ours),
            Vec::<String>::new(),
            "{key}: official symbols DEFINITIONS lacks"
        );
    }
}

/// The public `names` constants are official in at least one of the three
/// enumerations. The module exposes no list, so the constants are named here:
/// one that disappears stops this file compiling, and one that is added is to be
/// added below.
#[test]
fn name_constants_are_official() {
    use retp_proto::capabilities::names as n;
    let constants: BTreeSet<String> = [
        n::MAX_DATA_ARRAY_SIZE,
        n::MAX_DATA_OBJECT_SIZE,
        n::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE,
        n::MAX_WEBSOCKET_FRAME_PAYLOAD_SIZE,
        n::SUPPORTS_MESSAGE_HEADER_EXTENSIONS,
        n::SUPPORTS_ALTERNATE_REQUEST_URIS,
        n::RESPONSE_TIMEOUT_PERIOD,
        n::MULTIPART_MESSAGE_TIMEOUT_PERIOD,
        n::REQUEST_SESSION_TIMEOUT_PERIOD,
        n::SESSION_ESTABLISHMENT_TIMEOUT_PERIOD,
        n::MAX_CONCURRENT_MULTIPART,
        n::CHANGE_RETENTION_PERIOD,
        n::CHANGE_PROPAGATION_PERIOD,
        n::AUTHORIZATION_DETAILS,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    assert_eq!(constants.len(), 14, "a constant listed twice, or one dropped");
    let mut every: Vec<String> = Vec::new();
    for key in ["endpoint", "protocol", "dataObject"] {
        every.extend(official(key));
    }
    assert_eq!(
        unknown_to(&every, &constants),
        Vec::<String>::new(),
        "constants naming no official capability"
    );
}

/// The comparison can fail: a fabricated name is caught, a dropped one too.
#[test]
fn a_wrong_name_is_caught() {
    let official = official("endpoint");
    let mut ours: BTreeSet<String> = official.iter().cloned().collect();
    assert!(unknown_to(&official, &ours).is_empty());
    assert!(missing_from(&official, &ours).is_empty());

    ours.insert("MaxPartsize".to_string());
    assert_eq!(unknown_to(&official, &ours), vec!["MaxPartsize".to_string()]);

    ours.remove("MaxPartsize");
    ours.remove("MaxPartSize");
    assert_eq!(missing_from(&official, &ours), vec!["MaxPartSize".to_string()]);
}
