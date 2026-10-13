//! Every branch of every ETP union, against `fastavro`.
//!
//! `crossaudit.rs` samples: it decodes 138 messages whose values a seeded
//! filler draws, which reaches eleven union branches and misses the rest. This
//! file walks **all thirty**, so a branch index that is off by one cannot hide
//! in the branch nobody happened to draw.
//!
//! It exists because of a specific hazard, which `etpproto-python` met first
//! (`tests/test_array_types.py`): a union branch is chosen by the **writer**,
//! and in Python `fastavro` chooses it by guessing which branch the value
//! validates against. Handed a plain dict, an `ArrayOfDouble` goes out as an
//! `ArrayOfFloat` - same shape, half the precision, no warning. Our fixtures
//! pin the branch with `fastavro`'s `-type` tag, and check the byte that came
//! out before writing the file.
//!
//! Nothing in Rust can make that mistake: a branch here is a variant, and the
//! index is not a guess. What this file proves is that the variant order
//! matches the schema's branch order - which is the one thing the generator
//! could get wrong and no round trip of our own would notice.
//!
//! Fixtures: `python tests/fixtures/unions.py`.

use std::path::PathBuf;

use retp_avro::Avro;
use retp_types::datatypes::channel_data::PassDirection;
use retp_types::datatypes::{AnyArray, AnyArrayItem, DataValue, DataValueItem, IndexValue, IndexValueItem};

fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/unions")
        .join(format!("{name}.avro"));
    std::fs::read(&path).unwrap_or_else(|why| panic!("reading {}: {why}", path.display()))
}

/// The index describes what the generator could and could not produce.
fn index() -> Vec<serde_json::Value> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/unions/index.json");
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|why| panic!("reading {}: {why}", path.display()));
    serde_json::from_str(&text).expect("unions/index.json")
}

/// A fixture name, and what its decoded branch has to look like.
type Expectation = (&'static str, fn(&AnyArrayItem) -> bool);

/// Seven branches, and the two that differ only in width are among them.
#[test]
fn every_any_array_branch_decodes_to_its_own_variant() {
    let cases: Vec<Expectation> = vec![
        (
            "AnyArray-00-ArrayOfBoolean",
            |i| matches!(i, AnyArrayItem::ArrayOfBoolean(a) if a.values == [true, false, true]),
        ),
        (
            "AnyArray-01-ArrayOfInt",
            |i| matches!(i, AnyArrayItem::ArrayOfInt(a) if a.values == [i32::MIN, 0, i32::MAX]),
        ),
        (
            "AnyArray-02-ArrayOfLong",
            |i| matches!(i, AnyArrayItem::ArrayOfLong(a) if a.values == [i64::MIN, i64::MAX]),
        ),
        (
            "AnyArray-03-ArrayOfFloat",
            |i| matches!(i, AnyArrayItem::ArrayOfFloat(a) if a.values == [1.5f32, -2.5]),
        ),
        (
            "AnyArray-04-ArrayOfDouble",
            |i| matches!(i, AnyArrayItem::ArrayOfDouble(a) if a.values == [1.5f64, -2.5, 1e300]),
        ),
        (
            "AnyArray-05-ArrayOfString",
            |i| matches!(i, AnyArrayItem::ArrayOfString(a) if a.values == ["", "élan", "x"]),
        ),
        (
            "AnyArray-06-bytes",
            |i| matches!(i, AnyArrayItem::Bytes(b) if b == &[0x00, 0x01, 0xfe, 0xff]),
        ),
    ];

    for (name, expected) in cases {
        let bytes = fixture(name);
        let decoded = AnyArray::from_avro(&bytes).unwrap_or_else(|why| panic!("{name}: {why}"));
        assert!(
            expected(&decoded.item),
            "{name} decoded to the wrong branch: {:?}",
            decoded.item
        );
        // And back out again, because the branch index has to survive writing
        // as well as reading.
        assert_eq!(decoded.to_avro(), bytes, "{name} re-encoded differently");
    }
}

/// The pair that motivates the whole file: two branches with the same shape and
/// different widths. Decoding one as the other loses half the mantissa and
/// nothing says so.
#[test]
fn float_and_double_arrays_are_not_confused() {
    let floats = AnyArray::from_avro(&fixture("AnyArray-03-ArrayOfFloat")).unwrap();
    let doubles = AnyArray::from_avro(&fixture("AnyArray-04-ArrayOfDouble")).unwrap();

    assert!(matches!(floats.item, AnyArrayItem::ArrayOfFloat(_)));
    assert!(matches!(doubles.item, AnyArrayItem::ArrayOfDouble(_)));

    // 1e300 has no `f32`, so a fixture that had collapsed into the float branch
    // could not carry this value at all - which is what makes it the right one
    // to assert on.
    let AnyArrayItem::ArrayOfDouble(array) = &doubles.item else {
        unreachable!()
    };
    assert_eq!(array.values[2], 1e300);
}

#[test]
fn every_index_value_branch_decodes_to_its_own_variant() {
    let null = IndexValue::from_avro(&fixture("IndexValue-00-null")).unwrap();
    assert_eq!(null.item, IndexValueItem::Null);

    let long = IndexValue::from_avro(&fixture("IndexValue-01-long")).unwrap();
    assert_eq!(long.item, IndexValueItem::Long(1_234_567_890_123));

    let double = IndexValue::from_avro(&fixture("IndexValue-02-double")).unwrap();
    assert_eq!(double.item, IndexValueItem::Double(-0.125));

    let depth = IndexValue::from_avro(&fixture("IndexValue-03-PassIndexedDepth")).unwrap();
    let IndexValueItem::PassIndexedDepth(inner) = &depth.item else {
        panic!("expected a PassIndexedDepth, got {:?}", depth.item)
    };
    assert_eq!(inner.pass, 3);
    assert_eq!(inner.direction, PassDirection::Down);
    assert_eq!(inner.depth, 42.5);

    for name in [
        "IndexValue-00-null",
        "IndexValue-01-long",
        "IndexValue-02-double",
        "IndexValue-03-PassIndexedDepth",
    ] {
        let bytes = fixture(name);
        assert_eq!(
            IndexValue::from_avro(&bytes).unwrap().to_avro(),
            bytes,
            "{name} re-encoded differently"
        );
    }
}

/// Nineteen branches, of which `fastavro` can reach eighteen. See
/// [`the_unreachable_branch_is_named_not_forgotten`].
#[test]
fn every_reachable_data_value_branch_decodes_to_its_own_variant() {
    for entry in index() {
        if entry["union"] != "DataValue" || entry["reachable"] != true {
            continue;
        }
        let want = entry["index"].as_i64().expect("index") as usize;
        let branch = entry["branch"].as_str().expect("branch");
        let name = format!("DataValue-{want:02}-{branch}");

        let bytes = fixture(&name);
        let decoded = DataValue::from_avro(&bytes).unwrap_or_else(|why| panic!("{name}: {why}"));
        assert_eq!(
            variant_index(&decoded.item),
            want,
            "{name} decoded to branch {} instead of {want}",
            variant_index(&decoded.item)
        );
        assert_eq!(decoded.to_avro(), bytes, "{name} re-encoded differently");
    }
}

/// One branch has no third-party fixture, and saying so is the point.
///
/// `DataValue.item` branch 4 is `float`, and branch 5 is `double`. A Python
/// number carries neither, and `-type` only disambiguates records, so
/// `fastavro` writes every Python float as branch 5 - there is no value we can
/// ask it for that comes out as a `float`.
///
/// The consequence is not about tests. **A Python ETP peer cannot put a `float`
/// in a `DataValue`**, so a capability that the specification types as a float
/// arrives as a double from such a peer. A reader that matched only on
/// `DataValueItem::Float` would silently see nothing - which is why the client
/// must accept the neighbouring width, and why this is written down here rather
/// than discovered against a live server.
///
/// The branch itself is still covered, by our own round trip. That is weaker -
/// no independent implementation confirms it - and the weakness is named.
#[test]
fn the_unreachable_branch_is_named_not_forgotten() {
    let unreachable: Vec<_> = index().into_iter().filter(|e| e["reachable"] == false).collect();
    assert_eq!(
        unreachable.len(),
        1,
        "the set of branches fastavro cannot reach changed: {unreachable:?}"
    );
    assert_eq!(unreachable[0]["union"], "DataValue");
    assert_eq!(unreachable[0]["branch"], "float");
    assert_eq!(unreachable[0]["index"], 4);
    assert_eq!(unreachable[0]["collapsedTo"], 5);

    // Covered by us alone, and it does round-trip.
    let value = DataValue {
        item: DataValueItem::Float(0.5),
    };
    let bytes = value.to_avro();
    assert_eq!(bytes[0] >> 1, 4, "our writer must still emit branch 4");
    assert_eq!(DataValue::from_avro(&bytes).unwrap(), value);
}

/// The whole point of the file, stated as an assertion: the fixtures cover
/// every branch the schemas declare, minus the one named above.
#[test]
fn the_coverage_is_complete() {
    let entries = index();
    assert_eq!(entries.len(), 30, "AnyArray 7 + IndexValue 4 + DataValue 19");

    for (union, count) in [("AnyArray", 7), ("IndexValue", 4), ("DataValue", 19)] {
        let seen: Vec<i64> = entries
            .iter()
            .filter(|e| e["union"] == union)
            .map(|e| e["index"].as_i64().expect("index"))
            .collect();
        let expected: Vec<i64> = (0..count).collect();
        assert_eq!(seen, expected, "{union} is missing a branch");
    }
}

/// The branch index of a value, written out rather than derived, because
/// deriving it from the encoding would be asking the encoder to grade itself.
fn variant_index(item: &DataValueItem) -> usize {
    match item {
        DataValueItem::Null => 0,
        DataValueItem::Boolean(_) => 1,
        DataValueItem::Int(_) => 2,
        DataValueItem::Long(_) => 3,
        DataValueItem::Float(_) => 4,
        DataValueItem::Double(_) => 5,
        DataValueItem::String(_) => 6,
        DataValueItem::ArrayOfBoolean(_) => 7,
        DataValueItem::ArrayOfNullableBoolean(_) => 8,
        DataValueItem::ArrayOfInt(_) => 9,
        DataValueItem::ArrayOfNullableInt(_) => 10,
        DataValueItem::ArrayOfLong(_) => 11,
        DataValueItem::ArrayOfNullableLong(_) => 12,
        DataValueItem::ArrayOfFloat(_) => 13,
        DataValueItem::ArrayOfDouble(_) => 14,
        DataValueItem::ArrayOfString(_) => 15,
        DataValueItem::ArrayOfBytes(_) => 16,
        DataValueItem::Bytes(_) => 17,
        DataValueItem::AnySparseArray(_) => 18,
    }
}
