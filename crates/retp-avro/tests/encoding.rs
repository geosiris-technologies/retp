//! The Avro binary encoding, checked against the specification's own examples.
//!
//! The shape of every assertion comes from Avro 1.11 §"Binary encoding" and
//! from ETP v1.2 §3.8, never from what this crate happens to produce. Where the
//! Avro specification prints an expected byte string, it is used verbatim -
//! those are the only assertions here that a bug in *both* the writer and the
//! reader could not satisfy.

use std::collections::BTreeMap;

use retp_avro::{Avro, Error, Reader, Writer, zigzag_decode, zigzag_encode};

/// The Avro specification's own zigzag table.
///
/// > 0 is encoded as 0, -1 as 1, 1 as 2, -2 as 3, 2 as 4 …
#[test]
fn zigzag_matches_the_specification_table() {
    for (value, encoded) in [
        (0i64, 0u64),
        (-1, 1),
        (1, 2),
        (-2, 3),
        (2, 4),
        (-64, 127),
        (64, 128),
    ] {
        assert_eq!(zigzag_encode(value), encoded, "encoding {value}");
        assert_eq!(zigzag_decode(encoded), value, "decoding {encoded}");
    }
}

/// The specification: 3 is encoded as `06`, and -1 as `01`.
#[test]
fn varint_matches_the_specification_bytes() {
    let mut out = Writer::new();
    out.long(3);
    assert_eq!(out.as_slice(), [0x06]);

    let mut out = Writer::new();
    out.long(-1);
    assert_eq!(out.as_slice(), [0x01]);

    // The specification's worked example: 1_000_000 -> hex 80 89 7a.
    let mut out = Writer::new();
    out.long(1_000_000);
    assert_eq!(out.as_slice(), [0x80, 0x89, 0x7a]);
}

/// The specification: `"foo"` is the length 3 then the three bytes.
#[test]
fn string_is_a_length_then_utf8() {
    let mut out = Writer::new();
    out.string("foo");
    assert_eq!(out.as_slice(), [0x06, b'f', b'o', b'o']);
}

#[test]
fn every_long_survives_the_round_trip() {
    let interesting = [
        0i64,
        1,
        -1,
        63,
        64,
        -64,
        -65,
        i64::from(i32::MAX),
        i64::from(i32::MIN),
        i64::MAX,
        i64::MIN,
    ];
    for value in interesting {
        let mut out = Writer::new();
        out.long(value);
        let bytes = out.finish();
        let mut input = Reader::new(&bytes);
        assert_eq!(input.long().unwrap(), value, "round trip of {value}");
        assert!(input.is_done(), "{value} left bytes behind");
    }
}

#[test]
fn i64_min_and_max_stay_within_ten_bytes() {
    for value in [i64::MIN, i64::MAX] {
        let mut out = Writer::new();
        out.long(value);
        assert!(out.len() <= 10, "{value} took {} bytes", out.len());
    }
}

#[test]
fn floats_round_trip_including_the_awkward_ones() {
    let mut out = Writer::new();
    for value in [0.0f64, -0.0, 1.5, -1.5, f64::MIN, f64::MAX, f64::INFINITY] {
        out.double(value);
    }
    out.double(f64::NAN);
    let bytes = out.finish();
    let mut input = Reader::new(&bytes);
    for value in [0.0f64, -0.0, 1.5, -1.5, f64::MIN, f64::MAX, f64::INFINITY] {
        let back = input.double().unwrap();
        assert_eq!(back.to_bits(), value.to_bits(), "round trip of {value}");
    }
    assert!(input.double().unwrap().is_nan());
    assert!(input.is_done());
}

#[test]
fn an_empty_array_is_a_single_zero() {
    let mut out = Writer::new();
    out.array::<i64, _>(&[], |w, v| w.long(*v));
    assert_eq!(out.as_slice(), [0x00]);
}

#[test]
fn an_array_round_trips() {
    let values: Vec<i64> = (0..500).map(|i| i * 37 - 9_000).collect();
    let mut out = Writer::new();
    out.array(&values, |w, v| w.long(*v));
    let bytes = out.finish();
    let mut input = Reader::new(&bytes);
    assert_eq!(input.array(|r| r.long()).unwrap(), values);
    assert!(input.is_done());
}

/// A peer may split an array into several blocks, and may announce each block's
/// byte size with a negative count. Nothing in this crate writes that form, so
/// without this test the reading path would never run.
#[test]
fn a_multi_block_array_reads_as_one() {
    let mut out = Writer::new();
    // Block one: two items, announced the plain way.
    out.long(2);
    out.long(10);
    out.long(20);
    // Block two: two items, announced with a negative count and a byte size.
    out.long(-2);
    out.long(2); // the two items below take one byte each
    out.long(30);
    out.long(40);
    out.long(0);
    let bytes = out.finish();

    let mut input = Reader::new(&bytes);
    assert_eq!(input.array(|r| r.long()).unwrap(), [10, 20, 30, 40]);
    assert!(input.is_done());
}

#[test]
fn a_map_round_trips_and_keeps_its_keys() {
    let mut entries = BTreeMap::new();
    entries.insert("zeta".to_string(), 1i64);
    entries.insert("alpha".to_string(), 2);
    entries.insert("".to_string(), 3);

    let mut out = Writer::new();
    out.map(&entries, |w, v| w.long(*v));
    let bytes = out.finish();

    let mut input = Reader::new(&bytes);
    assert_eq!(input.map(|r| r.long()).unwrap(), entries);
    assert!(input.is_done());
}

#[test]
fn the_bulk_float_path_agrees_with_the_generic_one() {
    let values: Vec<f64> = (0..1_000).map(|i| f64::from(i) * 0.25 - 3.0).collect();

    let mut bulk = Writer::new();
    bulk.array_f64(&values);
    let mut generic = Writer::new();
    generic.array(&values, |w, v| w.double(*v));
    assert_eq!(
        bulk.as_slice(),
        generic.as_slice(),
        "the bulk and generic writers must agree byte for byte"
    );

    let bytes = bulk.finish();
    assert_eq!(Reader::new(&bytes).array_f64().unwrap(), values);
    assert_eq!(
        Reader::new(&bytes).array(|r| r.double()).unwrap(),
        values,
        "the generic reader must read what the bulk writer wrote"
    );
}

#[test]
fn the_bulk_f32_path_agrees_with_the_generic_one() {
    let values: Vec<f32> = (0..777).map(|i| i as f32 * 0.5).collect();
    let mut bulk = Writer::new();
    bulk.array_f32(&values);
    let bytes = bulk.finish();
    assert_eq!(Reader::new(&bytes).array_f32().unwrap(), values);
    assert_eq!(Reader::new(&bytes).array(|r| r.float()).unwrap(), values);
}

// ---------------------------------------------------------------------------
// Refusals. Each of these would be a silent wrong answer without the check.
// ---------------------------------------------------------------------------

#[test]
fn a_truncated_buffer_is_named_not_guessed() {
    let mut out = Writer::new();
    out.string("hello");
    let bytes = out.finish();
    let cut = &bytes[..bytes.len() - 2];

    match Reader::new(cut).string() {
        Err(Error::Truncated {
            reading,
            needed,
            available,
        }) => {
            assert_eq!(reading, "bytes");
            assert_eq!(needed, 5);
            assert_eq!(available, 3);
        }
        other => panic!("expected a named truncation, got {other:?}"),
    }
}

#[test]
fn a_runaway_varint_stops() {
    // Eleven continuation bytes: more than any 64-bit value can need.
    let bytes = [0xffu8; 11];
    assert_eq!(Reader::new(&bytes).long(), Err(Error::VarintTooLong));
}

#[test]
fn a_negative_length_is_refused() {
    let mut out = Writer::new();
    out.long(-4); // where a bytes length belongs
    let bytes = out.finish();
    assert_eq!(Reader::new(&bytes).bytes(), Err(Error::BadLength(-4)));
}

#[test]
fn a_string_that_is_not_utf8_is_refused() {
    let mut out = Writer::new();
    out.bytes(&[0xff, 0xfe]);
    let bytes = out.finish();
    assert_eq!(Reader::new(&bytes).string(), Err(Error::NotUtf8));
}

#[test]
fn a_union_branch_that_does_not_exist_is_named() {
    let mut out = Writer::new();
    out.union_index(7);
    let bytes = out.finish();
    match Reader::new(&bytes).union_index("Example", 3) {
        Err(Error::NoSuchBranch {
            in_type,
            index,
            branches,
        }) => {
            assert_eq!(in_type, "Example");
            assert_eq!(index, 7);
            assert_eq!(branches, 3);
        }
        other => panic!("expected a named branch refusal, got {other:?}"),
    }
}

#[test]
fn an_enum_ordinal_that_does_not_exist_is_named() {
    let mut out = Writer::new();
    out.enum_index(9);
    let bytes = out.finish();
    match Reader::new(&bytes).enum_index("IndexDirection", 3) {
        Err(Error::NoSuchSymbol { index, symbols, .. }) => {
            assert_eq!(index, 9);
            assert_eq!(symbols, 3);
        }
        other => panic!("expected a named symbol refusal, got {other:?}"),
    }
}

/// Trailing bytes mean the peer and this reader disagree about the schema.
/// Reading the value and shrugging at the rest would hide exactly that.
#[test]
fn trailing_bytes_are_refused() {
    #[derive(Debug, PartialEq)]
    struct One(i64);

    impl Avro for One {
        const FULL_NAME: &'static str = "Test.One";
        fn write(&self, out: &mut Writer) {
            out.long(self.0);
        }
        fn read(input: &mut Reader<'_>) -> retp_avro::Result<Self> {
            Ok(One(input.long()?))
        }
        fn to_json(&self) -> retp_avro::json::Value {
            self.0.into()
        }
        fn from_json(value: &retp_avro::json::Value) -> retp_avro::Result<Self> {
            Ok(One(retp_avro::json::as_i64(value, "Test.One")?))
        }
    }

    let mut bytes = One(42).to_avro();
    assert_eq!(One::from_avro(&bytes), Ok(One(42)));
    bytes.push(0x00);
    assert_eq!(One::from_avro(&bytes), Err(Error::TrailingBytes(1)));
}
