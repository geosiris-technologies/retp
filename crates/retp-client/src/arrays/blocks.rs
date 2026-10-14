//! Writing **one block the caller chose** (`PutDataSubarrays`) and **declaring**
//! an array before it is written (`PutUninitializedDataArrays`) - the types and
//! the checks, and the blocking twins. The exchange itself, written once for
//! any `Caller`, is `arrays::asynchronous::blocks`.
//!
//! # What the specification says
//!
//! - ETP 1.2 §13.2.1.2 step 1: *« For a large array that will not fit in a
//!   single message, before a customer can put a data array in a store, it must
//!   specify the indexes and dimensions of the array »*, by
//!   `PutUninitializedDataArrays` (§13.3.9); step 1.a: *« If the array is small
//!   enough, this step is not required »*. Declaring a small array is therefore
//!   allowed, never required.
//! - §13.2.1.2 step 3.b: *« If the arrays are too large for one message, the
//!   customer MUST send multiple PutDataSubarrays messages »* (§13.3.7), each
//!   record a `PutDataSubarraysType` (§23.32.7): the identifier, the data, and
//!   `starts` and `counts` *« per dimension »*.
//! - §13.2.2 row 5: neither end may exceed the other's `MaxDataArraySize`;
//!   an end whose value is exceeded denies with `ELIMIT_EXCEEDED` (12).
//! - §13.2.2 row 9 and §13.2.2.1: a `DataArrayMetadata` carries both the
//!   logical and the transport type, and the transport must be one the table
//!   allows for the logical type.
//!
//! # What this module refuses by name, before anything leaves
//!
//! A write the specification does not describe is refused here rather than
//! sent and left to a store to interpret:
//!
//! - a block with no dimension, or with a count of 0 (nothing in §13.3.7
//!   says what a store does with an empty block);
//! - `starts` and `counts` of different lengths (§23.32.7: both are per
//!   dimension);
//! - a block that does not lie inside the array's declared dimensions, when
//!   the caller knows them (`SubarrayWrite::declared`). The specification
//!   states no rule for an out-of-range block, so the guard is the client's:
//!   `TODO(verify)`: which error a store answers for one.
//! - a declaration with no dimension or with a dimension of 0 (`TODO(verify)`:
//!   the specification does not say whether an empty array may be declared);
//! - a declaration whose transport type §13.2.2.1 does not allow for its
//!   logical type (`arrayOfCustom`, for which the table gives nothing, is
//!   not checked).
//!
//! # How to make the tests fail
//!
//! - `a_block_must_lie_inside_the_declared_array`: remove the
//!   `declared` comparison from [`check_subarray`].
//! - `a_declaration_must_use_a_transport_the_table_allows`: remove the
//!   `allowed_transports` test from [`check_declaration`].
//! - `starts_and_counts_go_together`: remove the length comparison.
//!
//! The exchange is tested in `crates/retp-client/tests/blocks.rs`.

use retp_types::datatypes::data_array_types::{DataArrayIdentifier, DataArrayMetadata};
use retp_types::datatypes::{AnyArrayType, AnyLogicalArrayType};

use super::{Array, ArrayFailure, ArrayProgress, allowed_transports};
use crate::ClientError;
use crate::caller::{Blocking, run};

/// A block to put into an array: where it starts, and its values.
///
/// The block's own shape is `block.dimensions` - the `counts` of the
/// `PutDataSubarraysType` (§23.32.7) - and its values are row-major in that
/// shape. A block too big for one message is cut again by the client, under
/// the same budget a whole array is cut under.
#[derive(Debug, Clone, PartialEq)]
pub struct SubarrayWrite {
    /// The array, by the URI of the object whose resource holds it and its
    /// path in that resource.
    pub id: DataArrayIdentifier,
    /// The first index of the block in the array, per dimension (§23.32.7
    /// `starts`).
    pub starts: Vec<u64>,
    /// The block: its shape is the `counts`, its values the data.
    pub block: Array,
    /// The dimensions the array was declared with, when the caller knows
    /// them: the block is then checked to lie inside before it leaves.
    /// `None` checks nothing against the array - the store decides.
    pub declared: Option<Vec<u64>>,
}

/// An array to declare before it is written (`PutUninitializedDataArrays`,
/// §13.3.9): its shape and its types, no value.
#[derive(Debug, Clone, PartialEq)]
pub struct Declaration {
    /// The array, as in [`SubarrayWrite::id`].
    pub id: DataArrayIdentifier,
    /// Its dimensions, outermost first.
    pub dimensions: Vec<u64>,
    /// What its elements are.
    pub logical: AnyLogicalArrayType,
    /// How its values will travel; one §13.2.2.1 allows for `logical`.
    pub transport: AnyArrayType,
    /// `preferredSubarrayDimensions` (§23.32.1, optional): the block shape
    /// the writer would like to be read by. Empty when it has no preference.
    pub preferred: Vec<u64>,
}

/// Whether a block can be put: pure, so that it is tested without a session.
///
/// `counts` is the block's shape. `declared` is the array's dimensions when
/// known. Every refusal is an [`ArrayFailure`] saying what is wrong.
pub fn check_subarray(starts: &[u64], counts: &[u64], declared: Option<&[u64]>) -> Result<(), ArrayFailure> {
    if counts.is_empty() {
        return Err(ArrayFailure::Plan("a block without dimensions".into()));
    }
    // §23.32.7: `starts` and `counts` are both « per dimension ».
    if starts.len() != counts.len() {
        return Err(ArrayFailure::Inconsistent(format!(
            "the block starts at {starts:?} but is {counts:?} long: starts and counts are per dimension (§23.32.7) and must have the same length"
        )));
    }
    if counts.contains(&0) {
        return Err(ArrayFailure::Plan(format!(
            "an empty block ({counts:?}): the specification does not say what a store does with one (§13.3.7)"
        )));
    }
    // The wire says `long`: an index a long cannot hold is not sent.
    for (start, count) in starts.iter().zip(counts) {
        match start.checked_add(*count) {
            Some(end) if i64::try_from(end).is_ok() => {}
            _ => {
                return Err(ArrayFailure::Inconsistent(format!(
                    "the block from {starts:?}, {counts:?} long, goes past what a long (§23.32.7) can index"
                )));
            }
        }
    }
    if let Some(declared) = declared {
        if declared.len() != counts.len() {
            return Err(ArrayFailure::Inconsistent(format!(
                "the block has {} dimension(s), the array was declared with {}",
                counts.len(),
                declared.len()
            )));
        }
        let inside = starts
            .iter()
            .zip(counts)
            .zip(declared)
            .all(|((start, count), dimension)| start + count <= *dimension);
        if !inside {
            return Err(ArrayFailure::Plan(format!(
                "the block from {starts:?}, {counts:?} long, is not inside the declared array of {declared:?}"
            )));
        }
    }
    Ok(())
}

/// Whether an array can be declared, and the `DataArrayMetadata`
/// (`PutUninitializedDataArrayType.metadata`, §23.32.6) it is declared with.
pub fn check_declaration(item: &Declaration) -> Result<DataArrayMetadata, ArrayFailure> {
    if item.dimensions.is_empty() {
        return Err(ArrayFailure::Plan("an array without dimensions".into()));
    }
    if item.dimensions.contains(&0) {
        return Err(ArrayFailure::Plan(format!(
            "an array of {:?} holds nothing: TODO(verify) whether ETP 1.2 §13.3.9 lets an empty array be declared",
            item.dimensions
        )));
    }
    let as_long = |values: &[u64], what: &str| -> Result<Vec<i64>, ArrayFailure> {
        values
            .iter()
            .map(|value| {
                i64::try_from(*value).map_err(|_| {
                    ArrayFailure::Inconsistent(format!("{what} {value} is more than a long can hold"))
                })
            })
            .collect()
    };
    let dimensions = as_long(&item.dimensions, "the dimension")?;
    // §13.2.2 row 9 and §13.2.2.1. The table gives nothing for `arrayOfCustom`.
    let allowed = allowed_transports(item.logical);
    if !allowed.is_empty() && !allowed.contains(&item.transport) {
        return Err(ArrayFailure::Inconsistent(format!(
            "{:?} cannot travel as {:?} (§13.2.2.1)",
            item.logical, item.transport
        )));
    }
    if !item.preferred.is_empty()
        && (item.preferred.len() != item.dimensions.len() || item.preferred.contains(&0))
    {
        return Err(ArrayFailure::Inconsistent(format!(
            "preferredSubarrayDimensions {:?} must give one positive size per dimension of {:?}",
            item.preferred, item.dimensions
        )));
    }
    let preferred = as_long(&item.preferred, "the preferred size")?;
    Ok(DataArrayMetadata {
        dimensions,
        preferred_subarray_dimensions: preferred,
        transport_array_type: item.transport,
        logical_array_type: item.logical,
        ..Default::default()
    })
}

/// [`super::asynchronous::write_subarrays`], on the blocking client.
pub fn write_subarrays<K: Blocking>(
    client: &mut K,
    items: &[SubarrayWrite],
    default_message: u64,
    progress: impl FnMut(ArrayProgress),
) -> Result<Vec<Result<(), ArrayFailure>>, ClientError> {
    run(super::asynchronous::write_subarrays(
        client,
        items,
        default_message,
        progress,
    ))
}

/// [`super::asynchronous::declare_arrays`], on the blocking client.
pub fn declare_arrays<K: Blocking>(
    client: &mut K,
    items: &[Declaration],
    default_message: u64,
) -> Result<Vec<Result<(), ArrayFailure>>, ClientError> {
    run(super::asynchronous::declare_arrays(
        client,
        items,
        default_message,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declaration(logical: AnyLogicalArrayType, transport: AnyArrayType) -> Declaration {
        Declaration {
            id: DataArrayIdentifier {
                uri: "eml:///resqml20.obj_PointSetRepresentation(bb000000-0000-4000-8000-000000000002)"
                    .into(),
                path_in_resource: "/points".into(),
            },
            dimensions: vec![4, 3],
            logical,
            transport,
            preferred: Vec::new(),
        }
    }

    /// §23.32.7: starts and counts are both per dimension.
    #[test]
    fn starts_and_counts_go_together() {
        assert!(check_subarray(&[0, 0], &[2, 3], None).is_ok());
        assert!(matches!(
            check_subarray(&[0], &[2, 3], None),
            Err(ArrayFailure::Inconsistent(_))
        ));
        assert!(matches!(
            check_subarray(&[], &[], None),
            Err(ArrayFailure::Plan(_))
        ));
        assert!(matches!(
            check_subarray(&[0, 0], &[2, 0], None),
            Err(ArrayFailure::Plan(why)) if why.contains("empty block")
        ));
    }

    /// A block past what a long indexes is refused, not wrapped.
    #[test]
    fn an_index_a_long_cannot_hold_is_refused() {
        assert!(matches!(
            check_subarray(&[i64::MAX as u64], &[1], None),
            Err(ArrayFailure::Inconsistent(_))
        ));
        assert!(matches!(
            check_subarray(&[u64::MAX], &[2], None),
            Err(ArrayFailure::Inconsistent(_))
        ));
    }

    /// A block that goes past the declared array is refused, one that ends
    /// exactly on its edge is not.
    #[test]
    fn a_block_must_lie_inside_the_declared_array() {
        assert!(check_subarray(&[2, 0], &[2, 3], Some(&[4, 3])).is_ok());
        assert!(matches!(
            check_subarray(&[3, 0], &[2, 3], Some(&[4, 3])),
            Err(ArrayFailure::Plan(why)) if why.contains("not inside")
        ));
        assert!(matches!(
            check_subarray(&[0], &[2], Some(&[4, 3])),
            Err(ArrayFailure::Inconsistent(_))
        ));
    }

    /// §13.2.2.1: `arrayOfDouble64LE` travels as `arrayOfDouble`, not as
    /// `arrayOfInt`.
    #[test]
    fn a_declaration_must_use_a_transport_the_table_allows() {
        use AnyArrayType as T;
        use AnyLogicalArrayType as L;
        let ok = check_declaration(&declaration(L::ArrayOfDouble64Le, T::ArrayOfDouble)).unwrap();
        assert_eq!(ok.dimensions, vec![4, 3]);
        assert_eq!(ok.transport_array_type, T::ArrayOfDouble);
        assert_eq!(ok.logical_array_type, L::ArrayOfDouble64Le);
        assert!(matches!(
            check_declaration(&declaration(L::ArrayOfDouble64Le, T::ArrayOfInt)),
            Err(ArrayFailure::Inconsistent(why)) if why.contains("13.2.2.1")
        ));
        // The table gives nothing for `arrayOfCustom`: nothing to check.
        assert!(check_declaration(&declaration(L::ArrayOfCustom, T::Bytes)).is_ok());
    }

    /// An array with no dimension, or an empty one, is not declared.
    #[test]
    fn nothing_without_a_shape_is_declared() {
        use AnyArrayType as T;
        use AnyLogicalArrayType as L;
        let mut none = declaration(L::ArrayOfDouble64Le, T::ArrayOfDouble);
        none.dimensions.clear();
        assert!(matches!(check_declaration(&none), Err(ArrayFailure::Plan(_))));
        let mut empty = declaration(L::ArrayOfDouble64Le, T::ArrayOfDouble);
        empty.dimensions = vec![4, 0];
        assert!(matches!(check_declaration(&empty), Err(ArrayFailure::Plan(_))));
        let mut skewed = declaration(L::ArrayOfDouble64Le, T::ArrayOfDouble);
        skewed.preferred = vec![2];
        assert!(matches!(
            check_declaration(&skewed),
            Err(ArrayFailure::Inconsistent(_))
        ));
        skewed.preferred = vec![2, 3];
        assert_eq!(
            check_declaration(&skewed).unwrap().preferred_subarray_dimensions,
            vec![2, 3]
        );
    }
}
