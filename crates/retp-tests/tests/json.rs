//! The Avro JSON encoding of the ETP types, against an independent witness.
//!
//! ETP v1.2 §3.8.1 (p.76) allows JSON "for internal testing and debugging only"
//! and warns that Avro's JSON conventions "are not commonly supported by Avro
//! libraries or commonly used JSON libraries" -- so the one thing worth proving
//! is that *we* agree with someone who follows them. The 133 files in
//! `fixtures/json/` were written by `fastavro.json_writer` from the same
//! `.avsc` the generator read (`fixtures/make_json.py`), from the same values as
//! the binary fixtures `crossaudit.rs` decodes. Three independent facts are
//! confronted for every message:
//!
//! 1. our `to_json` gives the JSON fastavro wrote;
//! 2. our `from_json` reads it into the value our `from_avro` reads out of
//!    fastavro's *binary* encoding of the same value;
//! 3. our own text round-trips.
//!
//! The five messages with no field are not witnessed: fastavro 1.12 raises on a
//! record without a field. Their JSON is `{}` by the specification, and says so
//! below.
//!
//! The rest is hand-built values whose expected JSON is written out from the
//! specification's rules (Avro 1.x, "JSON Encoding"), not read from anywhere.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::PathBuf;

use retp_avro::{Avro, Error, EtpMessage};
use retp_types::datatypes::data_array_types::{DataArray, DataArrayIdentifier, PutDataArraysType};
use retp_types::datatypes::object::{ActiveStatusKind, ContextInfo, ContextScopeKind, RelationshipKind};
use retp_types::datatypes::{
    AnyArray, AnyArrayItem, ArrayOfDouble, DataValue, DataValueItem, SupportedProtocol, Uuid, Version,
};
use retp_types::dispatch::MESSAGE_COUNT;
use retp_types::protocol::core::OpenSession;
use retp_types::protocol::data_array::PutDataArrays;
use retp_types::protocol::discovery::GetResources;
use retp_types::protocol::store::{Chunk, GetDataObjects};
use serde_json::{Value, json};

#[path = "json/messages.rs"]
mod messages;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

// ---------------------------------------------------------------------------
// Every message against fastavro
// ---------------------------------------------------------------------------

/// What happened to one message.
enum Outcome {
    Checked,
    /// No JSON fixture: the record has no field, and fastavro cannot write one.
    NoWitness,
}

fn witnessed<T>(name: &str) -> Result<Outcome, String>
where
    T: Avro + EtpMessage + PartialEq + Debug,
{
    let stem = format!("{:02}-{:02}-{name}", T::PROTOCOL, T::MESSAGE_TYPE);
    let json_path = fixtures_dir().join("json").join(format!("{stem}.json"));
    let text = match std::fs::read_to_string(&json_path) {
        Ok(text) => text,
        Err(_) => return Ok(Outcome::NoWitness),
    };
    let binary = std::fs::read(fixtures_dir().join(format!("{stem}.avro")))
        .map_err(|why| format!("{stem}: reading the binary fixture: {why}"))?;

    let from_binary = T::from_avro(&binary).map_err(|why| format!("{stem}: binary: {why}"))?;
    let from_json = T::from_json_str(&text).map_err(|why| format!("{stem}: from_json: {why}"))?;
    if from_json != from_binary {
        return Err(format!(
            "{stem}: fastavro's JSON and fastavro's binary read as different values\n  json:   {from_json:?}\n  binary: {from_binary:?}"
        ));
    }

    let witness: Value = serde_json::from_str(&text).map_err(|why| format!("{stem}: {why}"))?;
    let ours = from_json.to_json();
    if ours != witness {
        return Err(format!(
            "{stem}: to_json differs from fastavro's JSON\n  ours:     {ours}\n  fastavro: {witness}"
        ));
    }

    let again = T::from_json_str(&from_json.to_json_string())
        .map_err(|why| format!("{stem}: reading our own text: {why}"))?;
    if again != from_json {
        return Err(format!("{stem}: our text did not read back as itself"));
    }
    Ok(Outcome::Checked)
}

macro_rules! witness_all {
    ($(($name:ident, $ty:ty),)*) => {
        fn witness_every_message() -> Vec<(&'static str, Result<Outcome, String>)> {
            vec![$((stringify!($name), witnessed::<$ty>(stringify!($name))),)*]
        }
    };
}
messages::every_message!(witness_all);

#[test]
fn every_message_agrees_with_fastavros_json() {
    let results = witness_every_message();
    assert_eq!(
        results.len(),
        MESSAGE_COUNT,
        "the list of messages and the dispatch table disagree about how many ETP has"
    );

    let failures: Vec<&str> = results
        .iter()
        .filter_map(|(_, result)| result.as_ref().err().map(String::as_str))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} messages disagree with fastavro:\n{}",
        failures.len(),
        results.len(),
        failures.join("\n")
    );

    let unwitnessed: Vec<&str> = results
        .iter()
        .filter(|(_, result)| matches!(result, Ok(Outcome::NoWitness)))
        .map(|(name, _)| *name)
        .collect();
    let mut expected = vec![
        "WMLS_GetVersion",
        "StartStreaming",
        "StopStreaming",
        "Acknowledge",
        "ReplacePartsByRangeResponse",
    ];
    let mut got = unwitnessed.clone();
    expected.sort_unstable();
    got.sort_unstable();
    assert_eq!(
        got, expected,
        "only the records without a field may lack a fastavro witness"
    );
    let checked = results.len() - unwitnessed.len();
    assert_eq!(checked, 133, "every other message has a JSON fixture");
}

/// A record without a field is `{}` -- Avro 1.x, "JSON Encoding": a record is an
/// object with one entry per field. This is the five messages fastavro cannot
/// write; the specification can.
#[test]
fn a_record_with_no_field_is_an_empty_object() {
    use retp_types::protocol::channel_streaming::StartStreaming;
    use retp_types::protocol::core::Acknowledge;

    assert_eq!(Acknowledge::default().to_json(), json!({}));
    assert_eq!(Acknowledge::from_json_str("{}"), Ok(Acknowledge::default()));
    assert_eq!(StartStreaming::default().to_json_string(), "{}");
    // A key on a record that has no field is not nothing.
    assert!(matches!(
        Acknowledge::from_json_str(r#"{"x": 1}"#),
        Err(Error::JsonUnknownField { .. })
    ));
}

// ---------------------------------------------------------------------------
// Real messages, expected JSON written out from the specification
// ---------------------------------------------------------------------------

fn context() -> ContextInfo {
    ContextInfo {
        uri: "eml:///dataspace('demo')".into(),
        depth: 1,
        data_object_types: vec!["resqml20.obj_TriangulatedSetRepresentation".into()],
        navigable_edges: RelationshipKind::Both,
        include_secondary_targets: true,
        include_secondary_sources: false,
    }
}

/// `["null", T]` is `null` or `{"<branch>": value}`; the branch of a primitive
/// is its type name and the branch of an enum is its **full name**.
#[test]
fn nullable_unions_are_null_or_a_named_branch() {
    let without = GetResources {
        context: context(),
        scope: ContextScopeKind::SourcesOrSelf,
        count_objects: true,
        store_last_write_filter: None,
        active_status_filter: None,
        include_edges: false,
    };
    let with = GetResources {
        store_last_write_filter: Some(1_700_000_000_000_000),
        active_status_filter: Some(ActiveStatusKind::Inactive),
        include_edges: true,
        ..without.clone()
    };

    let context = json!({
        "uri": "eml:///dataspace('demo')",
        "depth": 1,
        "dataObjectTypes": ["resqml20.obj_TriangulatedSetRepresentation"],
        "navigableEdges": "Both",
        "includeSecondaryTargets": true,
        "includeSecondarySources": false,
    });
    assert_eq!(
        without.to_json(),
        json!({
            "context": context,
            "scope": "sourcesOrSelf",
            "countObjects": true,
            "storeLastWriteFilter": null,
            "activeStatusFilter": null,
            "includeEdges": false,
        })
    );
    assert_eq!(
        with.to_json(),
        json!({
            "context": context,
            "scope": "sourcesOrSelf",
            "countObjects": true,
            "storeLastWriteFilter": {"long": 1_700_000_000_000_000_i64},
            "activeStatusFilter": {"Energistics.Etp.v12.Datatypes.Object.ActiveStatusKind": "Inactive"},
            "includeEdges": true,
        })
    );
    for message in [&without, &with] {
        assert_eq!(&GetResources::from_json(&message.to_json()).unwrap(), message);
        assert_eq!(
            &GetResources::from_json_str(&message.to_json_string()).unwrap(),
            message
        );
    }
}

/// `bytes` is a string of code points 0-255 -- so 0xC8 is `"È"`, which is
/// two bytes of UTF-8 in the text, and 0x41 is `"A"`.
#[test]
fn bytes_and_fixed_are_strings_of_code_points() {
    let chunk = Chunk {
        blob_id: Uuid([0, 1, 0x7f, 0x80, 0xfe, 0xff, 0x41, 0x42, 9, 8, 7, 6, 5, 4, 3, 2]),
        data: vec![0x00, 0xc8, 0xff, 0x41, 0x7f, 0x80],
        r#final: true,
    };
    assert_eq!(
        chunk.to_json(),
        json!({
            "blobId": "\u{0}\u{1}\u{7f}\u{80}\u{fe}\u{ff}AB\u{9}\u{8}\u{7}\u{6}\u{5}\u{4}\u{3}\u{2}",
            "data": "\u{0}\u{c8}\u{ff}A\u{7f}\u{80}",
            "final": true,
        })
    );
    assert_eq!(Chunk::from_json(&chunk.to_json()).unwrap(), chunk);
    // Through the text, where the high code points are escaped or encoded.
    assert_eq!(Chunk::from_json_str(&chunk.to_json_string()).unwrap(), chunk);

    // 0x100 is not a byte, and a `fixed` of the wrong size is not a Uuid.
    let too_high = json!({"blobId": "\u{0}".repeat(16), "data": "\u{100}", "final": false});
    assert!(matches!(
        Chunk::from_json(&too_high),
        Err(Error::JsonBadValue { .. })
    ));
    let short = json!({"blobId": "abc", "data": "", "final": false});
    match Chunk::from_json(&short) {
        Err(Error::JsonBadValue { at, why }) => {
            assert_eq!(at, "Energistics.Etp.v12.Datatypes.Uuid");
            assert!(why.contains("16"), "{why}");
        }
        other => panic!("expected a named refusal, got {other:?}"),
    }
}

/// `PutDataArrays`: a map of records, whose array is a union of *records*, so
/// its branch is a full name -- and whose items are doubles, each a number.
#[test]
fn arrays_travel_under_their_full_name() {
    let message = PutDataArrays {
        data_arrays: BTreeMap::from([(
            "0".to_string(),
            PutDataArraysType {
                uid: DataArrayIdentifier {
                    uri: "eml:///dataspace('demo')/resqml20.obj_PointSet(1)".into(),
                    path_in_resource: "/RESQML/pts".into(),
                },
                array: DataArray {
                    dimensions: vec![2, 3],
                    data: AnyArray {
                        item: AnyArrayItem::ArrayOfDouble(ArrayOfDouble {
                            values: vec![0.5, -1.25, 1e300, 0.1, 3.0, 1e-7],
                        }),
                    },
                },
                custom_data: BTreeMap::new(),
            },
        )]),
    };
    let json = message.to_json();
    let item = &json["dataArrays"]["0"]["array"]["data"]["item"];
    assert_eq!(
        item,
        &json!({"Energistics.Etp.v12.Datatypes.ArrayOfDouble": {"values": [0.5, -1.25, 1e300, 0.1, 3.0, 1e-7]}})
    );
    assert_eq!(json["dataArrays"]["0"]["array"]["dimensions"], json!([2, 3]));
    assert_eq!(
        PutDataArrays::from_json_str(&message.to_json_string()).unwrap(),
        message
    );
}

/// `OpenSession`: a map of unions, a `fixed`, and fields with defaults.
#[test]
fn a_session_with_capabilities_round_trips() {
    let message = OpenSession {
        application_name: "demo".into(),
        application_version: "1.0".into(),
        server_instance_id: Uuid([7; 16]),
        supported_protocols: vec![SupportedProtocol {
            protocol: 4,
            protocol_version: Version {
                major: 1,
                minor: 2,
                revision: 0,
                patch: 0,
            },
            role: "store".into(),
            protocol_capabilities: BTreeMap::from([(
                "MaxDataObjectSize".to_string(),
                DataValue {
                    item: DataValueItem::Long(1 << 40),
                },
            )]),
        }],
        supported_data_objects: vec![],
        supported_compression: String::new(),
        supported_formats: vec!["xml".into()],
        current_date_time: 1,
        earliest_retained_change_time: 2,
        session_id: Uuid([9; 16]),
        endpoint_capabilities: BTreeMap::from([
            (
                "MaxWebSocketFramePayloadSize".to_string(),
                DataValue {
                    item: DataValueItem::Int(-5),
                },
            ),
            (
                "Name".to_string(),
                DataValue {
                    item: DataValueItem::String("x".into()),
                },
            ),
            (
                "Nothing".to_string(),
                DataValue {
                    item: DataValueItem::Null,
                },
            ),
            (
                "Ratio".to_string(),
                DataValue {
                    item: DataValueItem::Float(0.1),
                },
            ),
            (
                "Flag".to_string(),
                DataValue {
                    item: DataValueItem::Boolean(true),
                },
            ),
        ]),
    };
    let json = message.to_json();
    assert_eq!(
        json["endpointCapabilities"],
        json!({
            "Flag": {"item": {"boolean": true}},
            "MaxWebSocketFramePayloadSize": {"item": {"int": -5}},
            "Name": {"item": {"string": "x"}},
            "Nothing": {"item": null},
            "Ratio": {"item": {"float": 0.1}},
        })
    );
    assert_eq!(
        json["supportedProtocols"][0]["protocolCapabilities"],
        json!({"MaxDataObjectSize": {"item": {"long": 1_099_511_627_776_i64}}})
    );
    assert_eq!(
        OpenSession::from_json_str(&message.to_json_string()).unwrap(),
        message
    );
}

// ---------------------------------------------------------------------------
// Defaults and refusals
// ---------------------------------------------------------------------------

/// A field the schema gives a default takes it when absent; a field it does
/// not is a named refusal -- never the Avro zero, which `Default` would give.
#[test]
fn an_absent_field_takes_its_default_or_is_refused_by_name() {
    // `ContextInfo`: `dataObjectTypes` defaults to `[]`, the booleans to
    // `false`; `uri`, `depth` and `navigableEdges` have no default.
    let minimal = json!({"uri": "u", "depth": 3, "navigableEdges": "Primary"});
    let read = ContextInfo::from_json(&minimal).unwrap();
    assert_eq!(read.data_object_types, Vec::<String>::new());
    assert!(!read.include_secondary_targets && !read.include_secondary_sources);
    assert_eq!(read.depth, 3);

    for missing in ["uri", "depth", "navigableEdges"] {
        let mut broken = minimal.clone();
        broken.as_object_mut().unwrap().remove(missing);
        match ContextInfo::from_json(&broken) {
            Err(Error::JsonMissingField { in_type, field }) => {
                assert_eq!(in_type, "Energistics.Etp.v12.Datatypes.Object.ContextInfo");
                assert_eq!(field, missing);
            }
            other => panic!("`{missing}` absent: expected a named refusal, got {other:?}"),
        }
    }

    // The default is the schema's, not the Avro zero: `format` is `"xml"`.
    let request = GetDataObjects::from_json(&json!({"uris": {"0": "eml:///"}})).unwrap();
    assert_eq!(request.format, "xml");
    assert_eq!(
        request.uris,
        BTreeMap::from([("0".to_string(), "eml:///".to_string())])
    );

    // An absent nullable is still absent: Avro has no implicit `null`.
    let no_filters = json!({
        "context": minimal, "scope": "self", "includeEdges": false,
    });
    assert!(matches!(
        GetResources::from_json(&no_filters),
        Err(Error::JsonMissingField {
            field: "storeLastWriteFilter",
            ..
        })
    ));
}

#[test]
fn what_the_schema_does_not_declare_is_refused_by_name() {
    let base = json!({"uri": "u", "depth": 3, "navigableEdges": "Primary"});

    let mut extra = base.clone();
    extra["colour"] = json!("red");
    assert!(matches!(
        ContextInfo::from_json(&extra),
        Err(Error::JsonUnknownField { field, .. }) if field == "colour"
    ));

    let mut symbol = base.clone();
    symbol["navigableEdges"] = json!("Neither");
    assert!(matches!(
        ContextInfo::from_json(&symbol),
        Err(Error::JsonNoSuchSymbol { symbol, .. }) if symbol == "Neither"
    ));

    let mut kind = base.clone();
    kind["depth"] = json!("3");
    assert!(matches!(
        ContextInfo::from_json(&kind),
        Err(Error::JsonType {
            expected: "integer",
            found: "string",
            ..
        })
    ));
    kind["depth"] = json!(3.0);
    assert!(
        matches!(ContextInfo::from_json(&kind), Err(Error::JsonType { .. })),
        "3.0 is not an int"
    );
    kind["depth"] = json!(3_000_000_000_i64);
    assert!(
        matches!(ContextInfo::from_json(&kind), Err(Error::JsonBadValue { .. })),
        "out of int range"
    );

    assert!(matches!(
        ContextInfo::from_json(&json!([1, 2])),
        Err(Error::JsonType {
            expected: "object",
            found: "array",
            ..
        })
    ));
    assert!(matches!(
        ContextInfo::from_json_str("{not json"),
        Err(Error::JsonSyntax(_))
    ));
}

#[test]
fn a_union_value_must_name_a_branch_the_union_has() {
    let value = |item: Value| json!({"item": item});
    // The right branch, under the wrong name: `long` where the union has `int`
    // and `long` both -- and a bare number, which is not a union value at all.
    assert_eq!(
        DataValue::from_json(&value(json!({"long": 5}))).unwrap().item,
        DataValueItem::Long(5)
    );
    assert!(matches!(
        DataValue::from_json(&value(json!({"short": 5}))),
        Err(Error::JsonBadUnion { .. })
    ));
    assert!(matches!(
        DataValue::from_json(&value(json!(5))),
        Err(Error::JsonBadUnion { .. })
    ));
    assert!(matches!(
        DataValue::from_json(&value(json!({"int": 1, "long": 2}))),
        Err(Error::JsonBadUnion { .. })
    ));
    // A union with no `null` branch refuses the JSON `null`.
    assert!(matches!(
        AnyArray::from_json(&json!({"item": null})),
        Err(Error::JsonBadUnion { .. })
    ));
    // A branch of the right name and the wrong content.
    assert!(matches!(
        DataValue::from_json(&value(json!({"int": "x"}))),
        Err(Error::JsonType { .. })
    ));
    // `null` is the null branch -- and a nullable `long` refuses `{"int": 1}`.
    assert_eq!(
        DataValue::from_json(&value(json!(null))).unwrap().item,
        DataValueItem::Null
    );
    let mut resources = GetResources::default().to_json();
    resources["storeLastWriteFilter"] = json!({"int": 1});
    assert!(matches!(
        GetResources::from_json(&resources),
        Err(Error::JsonBadUnion { .. })
    ));
}

/// JSON has no `NaN` (§3.8.1 2.a). It is written as the string the Avro
/// specification uses for it, and read back; it is never a `null`.
#[test]
fn non_finite_numbers_are_strings_and_come_back() {
    let item = |item: DataValueItem| DataValue { item };
    for (value, text) in [(f64::INFINITY, "Infinity"), (f64::NEG_INFINITY, "-Infinity")] {
        let json = item(DataValueItem::Double(value)).to_json();
        assert_eq!(json, json!({"item": {"double": text}}));
        assert_eq!(
            DataValue::from_json(&json).unwrap().item,
            DataValueItem::Double(value)
        );
    }
    let json = item(DataValueItem::Double(f64::NAN)).to_json();
    assert_eq!(json, json!({"item": {"double": "NaN"}}));
    match DataValue::from_json(&json).unwrap().item {
        DataValueItem::Double(d) => assert!(d.is_nan()),
        other => panic!("{other:?}"),
    }
    let json = item(DataValueItem::Float(f32::NAN)).to_json();
    assert_eq!(json, json!({"item": {"float": "NaN"}}));
    // A string that is not one of the three is not a number.
    assert!(matches!(
        DataValue::from_json(&json!({"item": {"double": "one"}})),
        Err(Error::JsonBadValue { .. })
    ));
}

/// A `float` is written in the shortest decimal that is the same `f32`, not as
/// the `f64` it widens to; a `double` keeps every digit.
#[test]
fn floats_are_written_shortest_and_doubles_exactly() {
    let float = |f: f32| DataValue {
        item: DataValueItem::Float(f),
    };
    assert_eq!(float(0.1).to_json().to_string(), r#"{"item":{"float":0.1}}"#);
    for f in [
        0.1_f32,
        1.0 / 3.0,
        16_777_217.0,
        f32::MIN_POSITIVE,
        f32::MAX,
        -2.5e-10,
    ] {
        assert_eq!(
            DataValue::from_json(&float(f).to_json()).unwrap().item,
            DataValueItem::Float(f)
        );
    }
    for d in [
        0.1_f64,
        1.0 / 3.0,
        f64::MAX,
        f64::MIN_POSITIVE,
        5e-324,
        9_007_199_254_740_993.0,
    ] {
        let message = DataValue {
            item: DataValueItem::Double(d),
        };
        let text = message.to_json_string();
        assert_eq!(
            DataValue::from_json_str(&text).unwrap().item,
            DataValueItem::Double(d),
            "{text}"
        );
    }
    let long = |l: i64| DataValue {
        item: DataValueItem::Long(l),
    };
    for l in [i64::MIN, i64::MAX, 0, -1] {
        assert_eq!(
            DataValue::from_json_str(&long(l).to_json_string()).unwrap(),
            long(l)
        );
    }
}
