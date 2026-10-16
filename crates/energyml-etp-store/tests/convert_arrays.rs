//! The array conversions of `energyml_etp_store::convert::arrays`, both ways,
//! on values built by hand. Nothing here reads a file or a session.
//!
//! The rule under test: a value that goes to ETP and comes back is the same
//! value, and what cannot be said is refused by name.

use energyml_etp_store::convert::arrays::{
    LOGICAL_TYPES, array_data_from_etp, array_data_to_etp, array_metadata_to_etp, location_of,
    logical_from_symbol, logical_of_element, logical_type_of, sub_location_of,
};
use energyml_etp_store::discovery::{array_metadata_from_etp, identifier_of, logical_symbol};
use energyml_storage::array::{ArrayData, ArrayMetadata, ArrayValues};
use retp_client::arrays::allowed_transports;
use retp_types::datatypes::data_array_types::{DataArray, DataArrayIdentifier, DataArrayMetadata};
use retp_types::datatypes::{AnyArray, AnyArrayItem, AnyArrayType, AnyLogicalArrayType, ArrayOfString};

/// One array of each element type the storage holds, with a shape that is not
/// a line: 2 x 3, six values, extremes of the type included.
fn every_element_type() -> Vec<ArrayValues> {
    vec![
        ArrayValues::F64(vec![0.0, -1.5, 2.25, f64::MAX, f64::MIN_POSITIVE, 1e-300]),
        ArrayValues::F32(vec![0.0, -1.5, 2.25, f32::MAX, f32::MIN_POSITIVE, 1e-30]),
        ArrayValues::I64(vec![0, -1, 1, i64::MAX, i64::MIN, 42]),
        ArrayValues::I32(vec![0, -1, 1, i32::MAX, i32::MIN, 42]),
        ArrayValues::I16(vec![0, -1, 1, i16::MAX, i16::MIN, 42]),
        ArrayValues::I8(vec![0, -1, 1, i8::MAX, i8::MIN, 42]),
        ArrayValues::U64(vec![0, 1, 2, u64::MAX, u64::MAX - 1, 42]),
        ArrayValues::U32(vec![0, 1, 2, u32::MAX, u32::MAX - 1, 42]),
        ArrayValues::U16(vec![0, 1, 2, u16::MAX, u16::MAX - 1, 42]),
        ArrayValues::U8(vec![0, 1, 2, u8::MAX, u8::MAX - 1, 42]),
    ]
}

fn data(values: ArrayValues, dimensions: Vec<i64>) -> ArrayData {
    ArrayData {
        metadata: ArrayMetadata::new(dimensions, values.element_type()),
        values,
    }
}

fn logical_of(data: &ArrayData) -> AnyLogicalArrayType {
    logical_of_element(&data.metadata.element_type).expect("a known element type")
}

#[test]
fn values_of_every_element_type_make_the_round_trip() {
    for values in every_element_type() {
        let name = values.element_type();
        let original = data(values, vec![2, 3]);
        let wire = array_data_to_etp(&original).unwrap_or_else(|why| panic!("{name}: {why}"));
        assert_eq!(wire.dimensions, vec![2, 3], "{name}");
        let back =
            array_data_from_etp(&wire, logical_of(&original)).unwrap_or_else(|why| panic!("{name}: {why}"));
        assert_eq!(back, original, "{name}: storage -> ETP -> storage");
        let again = array_data_to_etp(&back).expect("a second time");
        assert_eq!(again, wire, "{name}: ETP -> storage -> ETP");
    }
}

#[test]
fn an_empty_array_travels_empty() {
    for values in [
        ArrayValues::F64(vec![]),
        ArrayValues::I32(vec![]),
        ArrayValues::U8(vec![]),
    ] {
        let original = data(values, vec![0]);
        let wire = array_data_to_etp(&original).expect("empty is an array");
        assert_eq!(wire.dimensions, vec![0]);
        let back = array_data_from_etp(&wire, logical_of(&original)).expect("and comes back");
        assert_eq!(back, original);
    }
}

#[test]
fn a_three_dimensional_array_keeps_its_shape_and_order() {
    let values: Vec<f64> = (0..24).map(f64::from).collect();
    let original = data(ArrayValues::F64(values), vec![2, 3, 4]);
    let wire = array_data_to_etp(&original).expect("sent");
    assert_eq!(wire.dimensions, vec![2, 3, 4]);
    let AnyArrayItem::ArrayOfDouble(items) = &wire.data.item else {
        panic!("doubles travel as arrayOfDouble");
    };
    assert_eq!(items.values[7], 7.0, "row-major, as stored");
    assert_eq!(
        array_data_from_etp(&wire, logical_of(&original)).expect("read"),
        original
    );
}

#[test]
fn the_transport_of_the_values_is_the_one_the_metadata_announces() {
    for values in every_element_type() {
        let name = values.element_type();
        let original = data(values, vec![2, 3]);
        let wire = array_data_to_etp(&original).expect("sent");
        let meta = array_metadata_to_etp(&original.metadata).expect("announced");
        let carried = match wire.data.item {
            AnyArrayItem::ArrayOfBoolean(_) => AnyArrayType::ArrayOfBoolean,
            AnyArrayItem::ArrayOfInt(_) => AnyArrayType::ArrayOfInt,
            AnyArrayItem::ArrayOfLong(_) => AnyArrayType::ArrayOfLong,
            AnyArrayItem::ArrayOfFloat(_) => AnyArrayType::ArrayOfFloat,
            AnyArrayItem::ArrayOfDouble(_) => AnyArrayType::ArrayOfDouble,
            AnyArrayItem::ArrayOfString(_) => AnyArrayType::ArrayOfString,
            AnyArrayItem::Bytes(_) => AnyArrayType::Bytes,
        };
        assert_eq!(meta.transport_array_type, carried, "{name}");
        assert_eq!(meta.logical_array_type, logical_of(&original), "{name}");
        assert_eq!(meta.dimensions, original.metadata.dimensions, "{name}");
    }
}

#[test]
fn values_that_do_not_fill_the_dimensions_are_refused_both_ways() {
    let short = data(ArrayValues::F64(vec![1.0, 2.0]), vec![2, 3]);
    let why = array_data_to_etp(&short).expect_err("short under a long shape");
    assert!(why.contains("promise 6 values"), "{why}");

    let wire = DataArray {
        dimensions: vec![2, 3],
        data: AnyArray {
            item: AnyArrayItem::ArrayOfDouble(retp_types::datatypes::ArrayOfDouble { values: vec![1.0] }),
        },
    };
    let why =
        array_data_from_etp(&wire, AnyLogicalArrayType::ArrayOfDouble64Le).expect_err("short on the wire");
    assert!(why.contains("promise 6 values"), "{why}");
}

#[test]
fn a_negative_dimension_is_refused_both_ways() {
    let negative = data(ArrayValues::F64(vec![]), vec![-1]);
    assert!(
        array_data_to_etp(&negative)
            .expect_err("refused")
            .contains("negative")
    );
    let wire = DataArray {
        dimensions: vec![-1],
        data: AnyArray {
            item: AnyArrayItem::ArrayOfDouble(retp_types::datatypes::ArrayOfDouble { values: vec![] }),
        },
    };
    assert!(
        array_data_from_etp(&wire, AnyLogicalArrayType::ArrayOfDouble64Le)
            .expect_err("refused")
            .contains("negative")
    );
}

#[test]
fn strings_are_not_a_storage_array() {
    let wire = DataArray {
        dimensions: vec![1],
        data: AnyArray {
            item: AnyArrayItem::ArrayOfString(ArrayOfString {
                values: vec!["a".into()],
            }),
        },
    };
    assert!(array_data_from_etp(&wire, AnyLogicalArrayType::ArrayOfString).is_err());
}

#[test]
fn big_endian_bytes_are_read_by_their_logical_type() {
    // 13.2.2.1: `bytes` carries each element in the logical byte order.
    let wire = DataArray {
        dimensions: vec![2],
        data: AnyArray {
            item: AnyArrayItem::Bytes(vec![0x00, 0x01, 0x01, 0x00]),
        },
    };
    let back = array_data_from_etp(&wire, AnyLogicalArrayType::ArrayOfInt16Be).expect("read");
    assert_eq!(back.values, ArrayValues::I16(vec![1, 256]));
    let little = array_data_from_etp(&wire, AnyLogicalArrayType::ArrayOfInt16Le).expect("read");
    assert_eq!(little.values, ArrayValues::I16(vec![256, 1]));
}

fn canonical_etp(logical: AnyLogicalArrayType) -> DataArrayMetadata {
    DataArrayMetadata {
        dimensions: vec![4, 5],
        preferred_subarray_dimensions: vec![2, 5],
        transport_array_type: allowed_transports(logical)
            .first()
            .copied()
            .unwrap_or(AnyArrayType::ArrayOfDouble),
        logical_array_type: logical,
        store_last_write: 1_700_000_000_123_456,
        store_created: 1_600_000_000_654_321,
        custom_data: Default::default(),
    }
}

#[test]
fn metadata_makes_the_round_trip_for_every_logical_type_the_wire_can_carry() {
    for logical in LOGICAL_TYPES {
        if logical == AnyLogicalArrayType::ArrayOfCustom {
            continue;
        }
        let symbol = logical_symbol(logical);
        let sent = canonical_etp(logical);
        // ETP -> storage -> ETP
        let stored = array_metadata_from_etp(&sent, logical);
        let first = array_metadata_to_etp(&stored).unwrap_or_else(|why| panic!("{symbol}: {why}"));
        assert_eq!(first.logical_array_type, logical, "{symbol}");
        assert_eq!(first.dimensions, sent.dimensions, "{symbol}");
        assert_eq!(
            first.preferred_subarray_dimensions, sent.preferred_subarray_dimensions,
            "{symbol}"
        );
        assert_eq!(first.store_last_write, sent.store_last_write, "{symbol}");
        assert_eq!(first.store_created, sent.store_created, "{symbol}");
        assert!(
            allowed_transports(logical).contains(&first.transport_array_type),
            "{symbol}: 13.2.2.1 allows {:?}, not {:?}",
            allowed_transports(logical),
            first.transport_array_type
        );
        // storage -> ETP -> storage, and ETP -> storage -> ETP from the canonical message
        assert_eq!(array_metadata_from_etp(&first, logical), stored, "{symbol}");
        let second = array_metadata_to_etp(&array_metadata_from_etp(&first, logical)).expect("again");
        assert_eq!(second, first, "{symbol}");
    }
}

#[test]
fn metadata_times_that_are_not_kept_are_sent_as_zero_and_come_back_unstated() {
    let mut stored = ArrayMetadata::new(vec![3], "double");
    stored.logical_type = Some("arrayOfDouble64LE".into());
    let sent = array_metadata_to_etp(&stored).expect("sent");
    assert_eq!((sent.store_created, sent.store_last_write), (0, 0));
    assert!(sent.preferred_subarray_dimensions.is_empty());
    assert!(sent.custom_data.is_empty());
    let back = array_metadata_from_etp(&sent, sent.logical_array_type);
    assert_eq!((back.store_created, back.store_last_write), (None, None));
}

#[test]
fn the_logical_type_a_store_stated_wins_over_its_element_type() {
    // A boolean array kept as bytes: the element type is uint8, the logical type says what it means.
    let mut stored = ArrayMetadata::new(vec![8], "uint8");
    assert_eq!(logical_type_of(&stored), Ok(AnyLogicalArrayType::ArrayOfUInt8));
    stored.logical_type = Some("arrayOfBoolean".into());
    assert_eq!(logical_type_of(&stored), Ok(AnyLogicalArrayType::ArrayOfBoolean));
    let sent = array_metadata_to_etp(&stored).expect("sent");
    assert_eq!(sent.logical_array_type, AnyLogicalArrayType::ArrayOfBoolean);
    assert_eq!(sent.transport_array_type, AnyArrayType::ArrayOfBoolean);
}

#[test]
fn what_no_logical_type_can_name_is_refused_by_name() {
    let mut stored = ArrayMetadata::new(vec![1], "complex128");
    let why = array_metadata_to_etp(&stored).expect_err("unknown element type");
    assert!(why.contains("complex128"), "{why}");
    stored.logical_type = Some("arrayOfQuaternion".into());
    let why = array_metadata_to_etp(&stored).expect_err("unknown word");
    assert!(why.contains("arrayOfQuaternion"), "{why}");
    // arrayOfCustom: 13.2.2.1 fixes no transport for it.
    stored.logical_type = Some("arrayOfCustom".into());
    let why = array_metadata_to_etp(&stored).expect_err("custom has no transport");
    assert!(why.contains("arrayOfCustom"), "{why}");
}

#[test]
fn every_logical_type_is_found_by_its_word_and_only_by_it() {
    assert_eq!(LOGICAL_TYPES.len(), 21);
    for logical in LOGICAL_TYPES {
        assert_eq!(logical_from_symbol(logical_symbol(logical)), Some(logical));
    }
    assert_eq!(
        logical_from_symbol("arrayofdouble64le"),
        None,
        "the spelling is exact"
    );
    assert_eq!(logical_from_symbol(""), None);
    // Distinct words: no two types share one.
    let mut words: Vec<_> = LOGICAL_TYPES.iter().map(|l| logical_symbol(*l)).collect();
    words.sort_unstable();
    words.dedup();
    assert_eq!(words.len(), 21);
}

#[test]
fn an_identifier_and_a_location_are_the_same_address() {
    for (uri, path) in [
        (
            "eml:///dataspace('demo')/resqml20.obj_Grid2dRepresentation(7c3d9a52-0000-4000-8000-000000000001)",
            "/RESQML/7c3d9a52-0000-4000-8000-000000000001/points_patch0",
        ),
        (
            "eml:///resqml22.PointSetRepresentation(11111111-2222-4333-8444-555555555555)",
            "/x",
        ),
        (
            "eml:///resqml22.PointSetRepresentation(11111111-2222-4333-8444-555555555555)",
            "",
        ),
    ] {
        let id = DataArrayIdentifier {
            uri: uri.into(),
            path_in_resource: path.into(),
        };
        let location = location_of(&id);
        assert_eq!(location.etp_address(), Some((uri, path)));
        assert!(location.file.is_empty(), "an ETP location names no file");
        assert_eq!(identifier_of(&location), Some(id));
    }
}

#[test]
fn a_sub_location_carries_the_block_and_keeps_the_address() {
    let id = DataArrayIdentifier {
        uri: "eml:///resqml22.PointSetRepresentation(11111111-2222-4333-8444-555555555555)".into(),
        path_in_resource: "/p".into(),
    };
    let block = sub_location_of(&id, &[10, 0], &[5, 3]);
    assert_eq!(block.start, vec![10, 0]);
    assert_eq!(block.count, vec![5, 3]);
    assert_eq!(identifier_of(&block), Some(id.clone()));
    // Empty means the whole array, as for any location.
    let whole = sub_location_of(&id, &[], &[]);
    assert_eq!(whole, location_of(&id));
}
