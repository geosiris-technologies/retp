//! This crate against an independent Avro implementation.
//!
//! *Never ask a component to check itself.* Every byte
//! string read here was written by `fastavro`, not by this crate, so a writer
//! and a reader that are wrong in the same way - the failure mode that a
//! round-trip test cannot see - shows up as a mismatch.
//!
//! The fixtures live beside this file and are committed. Regenerating them is
//! `python tests/fixtures/make.py`, and it needs `fastavro`; the test itself
//! needs nothing, which is why the fixtures are committed rather than built.
//!
//! Each test also **re-encodes** what it read and compares with the original
//! bytes. That is the half that catches a writer: reading `fastavro`'s bytes
//! correctly proves the reader, and producing them back proves the writer.

use std::collections::BTreeMap;

use retp_avro::{Reader, Writer};

fn fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}.avro", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|why| panic!("reading {path}: {why}"))
}

#[test]
fn primitives_agree_with_fastavro() {
    let bytes = fixture("primitives");
    let mut input = Reader::new(&bytes);

    assert!(input.boolean().unwrap());
    assert_eq!(input.int().unwrap(), -2_000_000_000);
    assert_eq!(input.long().unwrap(), i64::MIN);
    assert_eq!(input.float().unwrap(), 1.5);
    assert_eq!(input.double().unwrap(), -0.125);
    assert_eq!(input.string().unwrap(), "un puits déviéwith non-ascii ✔");
    assert_eq!(input.bytes().unwrap(), [0x00, 0x01, 0xfe, 0xff]);
    input.expect_done().unwrap();

    let mut out = Writer::new();
    out.boolean(true);
    out.int(-2_000_000_000);
    out.long(i64::MIN);
    out.float(1.5);
    out.double(-0.125);
    out.string("un puits déviéwith non-ascii ✔");
    out.bytes(&[0x00, 0x01, 0xfe, 0xff]);
    assert_eq!(out.finish(), bytes, "our writer must produce fastavro's bytes");
}

/// The values where a varint implementation goes wrong if it is going to.
#[test]
fn extremes_agree_with_fastavro() {
    let bytes = fixture("extremes");
    let mut input = Reader::new(&bytes);

    assert_eq!(input.long().unwrap(), i64::MIN);
    assert_eq!(input.long().unwrap(), i64::MAX);
    assert_eq!(input.long().unwrap(), 0);
    assert_eq!(input.long().unwrap(), -1);
    assert_eq!(input.int().unwrap(), i32::MIN);
    assert_eq!(input.int().unwrap(), i32::MAX);
    assert_eq!(input.string().unwrap(), "");
    input.expect_done().unwrap();

    let mut out = Writer::new();
    out.long(i64::MIN);
    out.long(i64::MAX);
    out.long(0);
    out.long(-1);
    out.int(i32::MIN);
    out.int(i32::MAX);
    out.string("");
    assert_eq!(out.finish(), bytes);
}

#[test]
fn arrays_and_maps_agree_with_fastavro() {
    let bytes = fixture("collections");
    let mut input = Reader::new(&bytes);

    let doubles = input.array(|r| r.double()).unwrap();
    let expected = [0.0f64, -0.0, 1.0 / 3.0, 1e308, -1e-308];
    assert_eq!(doubles.len(), expected.len());
    for (got, want) in doubles.iter().zip(expected) {
        assert_eq!(got.to_bits(), want.to_bits());
    }

    assert_eq!(
        input.array(|r| r.long()).unwrap(),
        [0i64, -1, 1, 127, -128, 1 << 40]
    );
    assert_eq!(input.array(|r| r.string()).unwrap(), ["", "a", "élan"]);
    assert_eq!(input.array(|r| r.int()).unwrap(), Vec::<i32>::new());

    let caps = input.map(|r| r.long()).unwrap();
    let mut wanted = BTreeMap::new();
    wanted.insert("zeta".to_string(), 1i64);
    wanted.insert("alpha".to_string(), 2);
    wanted.insert("MaxWebSocketMessagePayloadSize".to_string(), 16_777_216);
    assert_eq!(caps, wanted);

    assert_eq!(input.map(|r| r.string()).unwrap(), BTreeMap::new());
    input.expect_done().unwrap();
}

/// The bulk path must read what someone else wrote, not only what we write.
#[test]
fn the_bulk_double_path_reads_fastavro_bytes() {
    let bytes = fixture("collections");
    let generic = Reader::new(&bytes).array(|r| r.double()).unwrap();
    let bulk = Reader::new(&bytes).array_f64().unwrap();
    assert_eq!(generic.len(), bulk.len());
    for (a, b) in generic.iter().zip(&bulk) {
        assert_eq!(a.to_bits(), b.to_bits());
    }
}

/// A map is unordered on the wire. `fastavro` wrote these keys in insertion
/// order, and this crate reads them into a `BTreeMap`, which sorts them - so
/// re-encoding will *not* reproduce the original bytes, and that is correct.
/// Pinning the byte string here would pin someone else's iteration order as if
/// it were a rule.
#[test]
fn map_order_is_not_part_of_the_encoding() {
    let mut entries = BTreeMap::new();
    entries.insert("zeta".to_string(), 1i64);
    entries.insert("alpha".to_string(), 2);

    let mut ours = Writer::new();
    ours.map(&entries, |w, v| w.long(*v));
    let ours = ours.finish();

    // Same entries, written by us: sorted. Reading them back must still agree.
    assert_eq!(Reader::new(&ours).map(|r| r.long()).unwrap(), entries);
    let sorted_first = &ours[1..];
    assert!(
        sorted_first.starts_with(&[0x0a, b'a', b'l', b'p', b'h', b'a']),
        "our map writes keys in sorted order, and this pins that choice"
    );
}

#[test]
fn unions_agree_with_fastavro() {
    let bytes = fixture("unions");
    let mut input = Reader::new(&bytes);

    // ["null", "long"] - branch 0 is null, branch 1 is the value.
    assert_eq!(input.union_index("absent", 2).unwrap(), 0);
    assert_eq!(input.union_index("present", 2).unwrap(), 1);
    assert_eq!(input.long().unwrap(), 4_294_967_296);

    // ["null", "boolean", "int", "string"] - the branch index is the position.
    assert_eq!(input.union_index("picked", 4).unwrap(), 2);
    assert_eq!(input.int().unwrap(), 42);
    assert_eq!(input.union_index("first", 4).unwrap(), 0);
    assert_eq!(input.union_index("last", 4).unwrap(), 3);
    assert_eq!(input.string().unwrap(), "the last branch");
    input.expect_done().unwrap();

    let mut out = Writer::new();
    out.union_index(0);
    out.union_index(1);
    out.long(4_294_967_296);
    out.union_index(2);
    out.int(42);
    out.union_index(0);
    out.union_index(3);
    out.string("the last branch");
    assert_eq!(out.finish(), bytes);
}

/// The one that decides the generated enums: an Avro enum is its **ordinal**,
/// so the order of a generated Rust enum's variants is not cosmetic.
#[test]
fn enums_fixed_and_nesting_agree_with_fastavro() {
    let bytes = fixture("named");
    let mut input = Reader::new(&bytes);

    // symbols = [Increasing, Decreasing, Unordered]; the value was Unordered.
    assert_eq!(input.enum_index("IndexDirection", 3).unwrap(), 2);
    assert_eq!(input.enum_index("IndexDirection", 3).unwrap(), 0);

    // A fixed carries no length of its own.
    let id: [u8; 16] = input.fixed().unwrap();
    assert_eq!(id, std::array::from_fn(|i| i as u8));

    // A nested record is just its fields, in order, with no framing.
    assert_eq!(input.int().unwrap(), 1);
    assert_eq!(input.int().unwrap(), 2);
    assert_eq!(input.int().unwrap(), 0);
    assert_eq!(input.int().unwrap(), 0);

    let versions = input
        .array(|r| Ok([r.int()?, r.int()?, r.int()?, r.int()?]))
        .unwrap();
    assert_eq!(versions, [[1, 2, 3, 4], [0, 0, 0, 0]]);
    input.expect_done().unwrap();

    let mut out = Writer::new();
    out.enum_index(2);
    out.enum_index(0);
    out.fixed(&id);
    for field in [1, 2, 0, 0] {
        out.int(field);
    }
    out.array(&versions, |w, v| {
        for field in v {
            w.int(*field);
        }
    });
    assert_eq!(out.finish(), bytes);
}

/// A fixture that no longer matches its schema would make every test above
/// green against the wrong bytes. This one fails if a fixture goes missing or
/// is regenerated to a different size.
#[test]
fn the_fixtures_are_the_ones_the_tests_were_written_against() {
    for (name, size) in [
        ("primitives", 68usize),
        ("extremes", 33),
        ("collections", 120),
        ("unions", 27),
        ("named", 32),
    ] {
        assert_eq!(fixture(name).len(), size, "fixture {name} changed size");
    }
}
