//! Arrays, in both directions: what a storage array becomes on the ETP wire
//! (the answer of a server to `GetDataArrays`, `GetDataArrayMetadata`,
//! `GetDataSubarrays`) and the way back.
//!
//! The pairs, found together:
//!
//! | storage | ETP | there | back |
//! |---|---|---|---|
//! | [`ArrayMetadata`] | `DataArrayMetadata` | [`array_metadata_to_etp`] | `discovery::array_metadata_from_etp` |
//! | element type name | `AnyLogicalArrayType` | [`logical_type_of`] | `discovery::logical_symbol` |
//! | [`ArrayLocation`] | `DataArrayIdentifier` | [`location_of`], [`sub_location_of`] | `discovery::identifier_of` |
//! | [`ArrayData`] | `DataArray` | [`array_data_to_etp`] | [`array_data_from_etp`] |
//!
//! **The values have one conversion.** [`array_data_to_etp`] is `push::to_wire`
//! (the one the client side writes with) plus the packing of its result into
//! the `AnyArray` union; [`array_data_from_etp`] is `values_of` (the one the
//! client side reads with). Neither direction re-spells how a number is widened
//! or reinterpreted.
//!
//! **What is refused is named**, never guessed: a logical type nobody can
//! carry (`arrayOfCustom`), an element type this build does not know, an array
//! whose values do not fill its dimensions.

use energyml_storage::array::{ArrayData, ArrayLocation, ArrayMetadata};
use retp_client::arrays::{Array, ArrayValues as Wire, Decoded};
use retp_types::datatypes::data_array_types::{DataArray, DataArrayIdentifier, DataArrayMetadata};
use retp_types::datatypes::{
    AnyArray, AnyArrayItem, AnyArrayType, AnyLogicalArrayType, ArrayOfBoolean, ArrayOfDouble, ArrayOfFloat,
    ArrayOfInt, ArrayOfLong, ArrayOfString,
};

use energyml_storage::array::ArrayValues;

use crate::discovery::store_time;
use crate::element_type;
use crate::push::to_wire;

/// Every logical type ETP 1.2 names (13.2.2.1), in the schema's order.
pub const LOGICAL_TYPES: [AnyLogicalArrayType; 21] = [
    AnyLogicalArrayType::ArrayOfBoolean,
    AnyLogicalArrayType::ArrayOfInt8,
    AnyLogicalArrayType::ArrayOfUInt8,
    AnyLogicalArrayType::ArrayOfInt16Le,
    AnyLogicalArrayType::ArrayOfInt32Le,
    AnyLogicalArrayType::ArrayOfInt64Le,
    AnyLogicalArrayType::ArrayOfUInt16Le,
    AnyLogicalArrayType::ArrayOfUInt32Le,
    AnyLogicalArrayType::ArrayOfUInt64Le,
    AnyLogicalArrayType::ArrayOfFloat32Le,
    AnyLogicalArrayType::ArrayOfDouble64Le,
    AnyLogicalArrayType::ArrayOfInt16Be,
    AnyLogicalArrayType::ArrayOfInt32Be,
    AnyLogicalArrayType::ArrayOfInt64Be,
    AnyLogicalArrayType::ArrayOfUInt16Be,
    AnyLogicalArrayType::ArrayOfUInt32Be,
    AnyLogicalArrayType::ArrayOfUInt64Be,
    AnyLogicalArrayType::ArrayOfFloat32Be,
    AnyLogicalArrayType::ArrayOfDouble64Be,
    AnyLogicalArrayType::ArrayOfString,
    AnyLogicalArrayType::ArrayOfCustom,
];

/// The logical type a word spells, the inverse of
/// [`logical_symbol`]. `None` for a word ETP
/// does not use.
pub fn logical_from_symbol(symbol: &str) -> Option<AnyLogicalArrayType> {
    LOGICAL_TYPES
        .into_iter()
        .find(|logical| logical_symbol(*logical) == symbol)
}

/// The logical type an element type name stands for (`double` is
/// `arrayOfDouble64LE`): the inverse of the name storage gives a type, for the
/// little-endian spelling, which is the one a server writes. `None` for a name
/// this build does not know. `uint8` is `arrayOfUInt8`; the boolean flavour is
/// only ever said by [`ArrayMetadata::logical_type`].
pub fn logical_of_element(element: &str) -> Option<AnyLogicalArrayType> {
    use AnyLogicalArrayType as L;
    Some(match element {
        "double" => L::ArrayOfDouble64Le,
        "float" => L::ArrayOfFloat32Le,
        "int64" => L::ArrayOfInt64Le,
        "int32" => L::ArrayOfInt32Le,
        "int16" => L::ArrayOfInt16Le,
        "int8" => L::ArrayOfInt8,
        "uint64" => L::ArrayOfUInt64Le,
        "uint32" => L::ArrayOfUInt32Le,
        "uint16" => L::ArrayOfUInt16Le,
        "uint8" => L::ArrayOfUInt8,
        "string" => L::ArrayOfString,
        "custom" => L::ArrayOfCustom,
        _ => return None,
    })
}

/// The logical type an [`ArrayMetadata`] carries: the word the store itself
/// used for `logicalArrayType` when it kept one
/// ([`ArrayMetadata::logical_type`]), else the one its element type stands for.
/// A word or a name nobody knows is refused by name.
pub fn logical_type_of(meta: &ArrayMetadata) -> Result<AnyLogicalArrayType, String> {
    match &meta.logical_type {
        Some(word) => logical_from_symbol(word)
            .ok_or_else(|| format!("`{word}` is not a logical array type of ETP 1.2 (13.2.2.1)")),
        None => logical_of_element(&meta.element_type).ok_or_else(|| {
            format!(
                "no logical array type is known for the element type `{}`",
                meta.element_type
            )
        }),
    }
}

/// The transport a logical type travels in when this crate writes it: the one
/// `push::to_wire` picks for the numbers it sends, else the first one 13.2.2.1
/// allows. `arrayOfCustom` has none (the specification leaves it open), and is
/// refused.
fn transport_for(logical: AnyLogicalArrayType) -> Result<AnyArrayType, String> {
    use AnyArrayType as T;
    use AnyLogicalArrayType as L;
    match logical {
        L::ArrayOfDouble64Le => Ok(T::ArrayOfDouble),
        L::ArrayOfFloat32Le => Ok(T::ArrayOfFloat),
        L::ArrayOfInt64Le | L::ArrayOfUInt64Le | L::ArrayOfUInt32Le => Ok(T::ArrayOfLong),
        L::ArrayOfInt32Le | L::ArrayOfInt16Le | L::ArrayOfInt8 | L::ArrayOfUInt8 | L::ArrayOfUInt16Le => {
            Ok(T::ArrayOfInt)
        }
        other => retp_client::arrays::allowed_transports(other)
            .first()
            .copied()
            .ok_or_else(|| {
                format!(
                    "`{}` has no transport type ETP 1.2 fixes (13.2.2.1)",
                    logical_symbol(other)
                )
            }),
    }
}

/// Array metadata as ETP's `DataArrayMetadata`, the answer to
/// `GetDataArrayMetadata` (13.2.2).
///
/// | storage | ETP |
/// |---|---|
/// | `dimensions` | `dimensions` |
/// | [`logical_type_of`] | `logicalArrayType` |
/// | the transport of that logical type | `transportArrayType` |
/// | `preferred_subarray_dimensions` | `preferredSubarrayDimensions` |
/// | `store_created`, `store_last_write` | the same, microseconds; `None` is `0` |
/// | (storage has none) | `customData`, empty |
///
/// **What is not said.** The transport is chosen, not stored: storage keeps
/// the element type, not how a particular server shipped it, so a server that
/// sent `int` for `arrayOfInt64LE` comes back as `long`. The same logical type
/// always gets the same transport, the one [`array_data_to_etp`] uses, so
/// metadata and data of one array agree. `customData` of an array cannot be
/// held by [`ArrayMetadata`], and is sent empty.
pub fn array_metadata_to_etp(meta: &ArrayMetadata) -> Result<DataArrayMetadata, String> {
    let logical = logical_type_of(meta)?;
    Ok(DataArrayMetadata {
        dimensions: meta.dimensions.clone(),
        preferred_subarray_dimensions: meta.preferred_subarray_dimensions.clone(),
        transport_array_type: transport_for(logical)?,
        logical_array_type: logical,
        store_last_write: meta.store_last_write.unwrap_or(0),
        store_created: meta.store_created.unwrap_or(0),
        custom_data: Default::default(),
    })
}

/// Where a `DataArrayIdentifier` points, as storage addresses it: the ETP
/// pair `(uri, pathInResource)` (13.2.1), through `ArrayLocation::etp`. The
/// inverse of `discovery::identifier_of`.
pub fn location_of(id: &DataArrayIdentifier) -> ArrayLocation {
    ArrayLocation::etp(id.uri.clone(), id.path_in_resource.clone())
}

/// The location of one block of an array, for `GetDataSubarrays` (13.2.4):
/// [`location_of`] with `start` and `count` set from `starts` and `counts`.
/// Both are kept as asked (empty means "from the beginning" and "the whole
/// dataset", as for [`ArrayLocation`]); whether they fit the array is the
/// storage's to refuse.
pub fn sub_location_of(id: &DataArrayIdentifier, starts: &[i64], counts: &[i64]) -> ArrayLocation {
    let mut location = location_of(id);
    location.start = starts.to_vec();
    location.count = counts.to_vec();
    location
}

/// The wire values packed into the `AnyArray` union (13.2.2.1).
fn item_of(values: Wire) -> AnyArrayItem {
    match values {
        Wire::Booleans(values) => AnyArrayItem::ArrayOfBoolean(ArrayOfBoolean { values }),
        Wire::Ints(values) => AnyArrayItem::ArrayOfInt(ArrayOfInt { values }),
        Wire::Longs(values) => AnyArrayItem::ArrayOfLong(ArrayOfLong { values }),
        Wire::Floats(values) => AnyArrayItem::ArrayOfFloat(ArrayOfFloat { values }),
        Wire::Doubles(values) => AnyArrayItem::ArrayOfDouble(ArrayOfDouble { values }),
        Wire::Strings(values) => AnyArrayItem::ArrayOfString(ArrayOfString { values }),
        Wire::Bytes(bytes) => AnyArrayItem::Bytes(bytes),
    }
}

/// The wire values an `AnyArray` union holds, with the transport type they
/// travelled in.
fn wire_of(item: &AnyArrayItem) -> (Wire, AnyArrayType) {
    match item {
        AnyArrayItem::ArrayOfBoolean(a) => (Wire::Booleans(a.values.clone()), AnyArrayType::ArrayOfBoolean),
        AnyArrayItem::ArrayOfInt(a) => (Wire::Ints(a.values.clone()), AnyArrayType::ArrayOfInt),
        AnyArrayItem::ArrayOfLong(a) => (Wire::Longs(a.values.clone()), AnyArrayType::ArrayOfLong),
        AnyArrayItem::ArrayOfFloat(a) => (Wire::Floats(a.values.clone()), AnyArrayType::ArrayOfFloat),
        AnyArrayItem::ArrayOfDouble(a) => (Wire::Doubles(a.values.clone()), AnyArrayType::ArrayOfDouble),
        AnyArrayItem::ArrayOfString(a) => (Wire::Strings(a.values.clone()), AnyArrayType::ArrayOfString),
        AnyArrayItem::Bytes(b) => (Wire::Bytes(b.clone()), AnyArrayType::Bytes),
    }
}

/// An array as ETP's `DataArray`, the content of an answer to
/// `GetDataArrays` (13.2.2) or `GetDataSubarrays`: the dimensions, and the
/// values packed in the `AnyArray` union.
///
/// The values and their transport come from `push::to_wire` (the type is the
/// storage element type's little-endian logical type, the transport the one
/// [`array_metadata_to_etp`] announces for it). Refused by name: an element
/// type this build does not send, a negative dimension, and values that do not
/// fill the dimensions (a server never sends a short array under a long
/// shape).
///
/// The logical type is not in a `DataArray`; it travels in the metadata, or is
/// asked by the reader. The inverse is [`array_data_from_etp`].
pub fn array_data_to_etp(data: &ArrayData) -> Result<DataArray, String> {
    if let Some(bad) = data.metadata.dimensions.iter().find(|d| **d < 0) {
        return Err(format!("a negative dimension ({bad}) cannot be sent"));
    }
    let wire = to_wire(data)?;
    let expected: u64 = wire.dimensions.iter().product();
    let held = data.values.len() as u64;
    if expected != held {
        return Err(format!(
            "the dimensions {:?} promise {expected} values and the array holds {held}",
            data.metadata.dimensions
        ));
    }
    let dimensions = wire
        .dimensions
        .iter()
        .map(|d| i64::try_from(*d).map_err(|_| format!("dimension {d} does not fit ETP's long")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DataArray {
        dimensions,
        data: AnyArray {
            item: item_of(wire.values),
        },
    })
}

/// A `DataArray` as storage holds it, the inverse of [`array_data_to_etp`]:
/// the values as numbers of `logical` (`values_of`), the metadata reduced to
/// what a `DataArray` says (dimensions, and the element type of `logical`).
///
/// `logical` is the type the array is **read** as; it comes from the metadata
/// ([`array_metadata_to_etp`]'s counterpart) or from
/// `retp_client::arrays::reading_type`. Refused by name: a negative
/// dimension, string values (a storage array holds numbers), values that do not
/// fill the dimensions.
pub fn array_data_from_etp(array: &DataArray, logical: AnyLogicalArrayType) -> Result<ArrayData, String> {
    let dimensions = array
        .dimensions
        .iter()
        .map(|d| u64::try_from(*d).map_err(|_| format!("a negative dimension ({d}) in a DataArray")))
        .collect::<Result<Vec<_>, _>>()?;
    let (values, transport) = wire_of(&array.data.item);
    let expected: u64 = dimensions.iter().product();
    let wire = Array {
        dimensions,
        logical,
        transport,
        values,
        blocks: 1,
    };
    let values = values_of(&wire)?;
    if expected != values.len() as u64 {
        return Err(format!(
            "the dimensions {:?} promise {expected} values and the array holds {}",
            array.dimensions,
            values.len()
        ));
    }
    Ok(ArrayData {
        metadata: ArrayMetadata::new(array.dimensions.clone(), element_type(logical)),
        values,
    })
}

// -- ETP to storage (the client's side), moved here beside their inverses ---------

/// The word ETP spells a logical array type with (`arrayOfDouble64LE`).
pub fn logical_symbol(logical: AnyLogicalArrayType) -> &'static str {
    use AnyLogicalArrayType as L;
    match logical {
        L::ArrayOfBoolean => "arrayOfBoolean",
        L::ArrayOfInt8 => "arrayOfInt8",
        L::ArrayOfUInt8 => "arrayOfUInt8",
        L::ArrayOfInt16Le => "arrayOfInt16LE",
        L::ArrayOfInt32Le => "arrayOfInt32LE",
        L::ArrayOfInt64Le => "arrayOfInt64LE",
        L::ArrayOfUInt16Le => "arrayOfUInt16LE",
        L::ArrayOfUInt32Le => "arrayOfUInt32LE",
        L::ArrayOfUInt64Le => "arrayOfUInt64LE",
        L::ArrayOfFloat32Le => "arrayOfFloat32LE",
        L::ArrayOfDouble64Le => "arrayOfDouble64LE",
        L::ArrayOfInt16Be => "arrayOfInt16BE",
        L::ArrayOfInt32Be => "arrayOfInt32BE",
        L::ArrayOfInt64Be => "arrayOfInt64BE",
        L::ArrayOfUInt16Be => "arrayOfUInt16BE",
        L::ArrayOfUInt32Be => "arrayOfUInt32BE",
        L::ArrayOfUInt64Be => "arrayOfUInt64BE",
        L::ArrayOfFloat32Be => "arrayOfFloat32BE",
        L::ArrayOfDouble64Be => "arrayOfDouble64BE",
        L::ArrayOfString => "arrayOfString",
        L::ArrayOfCustom => "arrayOfCustom",
    }
}

/// ETP array metadata as storage array metadata.
///
/// `logical` is the logical type the array is **read** as (the caller resolved
/// it with `retp_client::arrays::reading_type`, which may correct a server
/// that sends a type its transport cannot carry); it gives the element type,
/// as `lib.rs` has always done. [`ArrayMetadata::logical_type`] is the word
/// the server itself used for `logicalArrayType`, so a correction stays
/// visible. `preferredSubarrayDimensions` and the store times are carried
/// over (0 times are unstated).
pub fn array_metadata_from_etp(meta: &DataArrayMetadata, logical: AnyLogicalArrayType) -> ArrayMetadata {
    let mut out = ArrayMetadata::new(meta.dimensions.clone(), element_type(logical).to_string());
    out.logical_type = Some(logical_symbol(meta.logical_array_type).to_string());
    out.preferred_subarray_dimensions = meta.preferred_subarray_dimensions.clone();
    out.store_created = store_time(meta.store_created);
    out.store_last_write = store_time(meta.store_last_write);
    out
}

/// Where an array is asked for: the ETP address when the location has one
/// (`resource_uri`, then `path_in_file` as `pathInResource`), else its `file`
/// when that was filled with the URI of the holding object (what `arrays_of`
/// does today). `None` for a location that names nothing.
pub fn identifier_of(location: &ArrayLocation) -> Option<DataArrayIdentifier> {
    match location.etp_address() {
        Some((uri, path)) => Some(DataArrayIdentifier {
            uri: uri.to_string(),
            path_in_resource: path.to_string(),
        }),
        None => (!location.file.is_empty()).then(|| DataArrayIdentifier {
            uri: location.file.clone(),
            path_in_resource: location.path_in_file.clone(),
        }),
    }
}

/// An array as storage keeps it: numbers of the logical type the store gave.
/// Strings are not an array a package holds.
pub fn values_of(array: &retp_client::arrays::Array) -> std::result::Result<ArrayValues, String> {
    match array.values.decode(array.logical) {
        Some(Decoded::F64(v)) => Ok(ArrayValues::F64(v)),
        Some(Decoded::F32(v)) => Ok(ArrayValues::F32(v)),
        Some(Decoded::I64(v)) => Ok(ArrayValues::I64(v)),
        Some(Decoded::I32(v)) => Ok(ArrayValues::I32(v)),
        Some(Decoded::I16(v)) => Ok(ArrayValues::I16(v)),
        Some(Decoded::I8(v)) => Ok(ArrayValues::I8(v)),
        Some(Decoded::U64(v)) => Ok(ArrayValues::U64(v)),
        Some(Decoded::U32(v)) => Ok(ArrayValues::U32(v)),
        Some(Decoded::U16(v)) => Ok(ArrayValues::U16(v)),
        Some(Decoded::U8(v)) => Ok(ArrayValues::U8(v)),
        // Flags as the bytes HDF5 keeps them in.
        Some(Decoded::Booleans(v)) => Ok(ArrayValues::U8(v.into_iter().map(u8::from).collect())),
        Some(Decoded::Strings(_)) | None => Err(format!(
            "{:?} values are not numbers, and a storage array holds numbers",
            array.logical
        )),
    }
}
