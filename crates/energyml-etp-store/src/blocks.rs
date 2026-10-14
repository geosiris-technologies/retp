//! Writing a block of an array and declaring an array, on an ETP dataspace -
//! the two helpers `put_arrays` and `declare_arrays` of the writer
//! (`writer.rs`) call.
//!
//! | the trait | ETP 1.2 |
//! |---|---|
//! | `put_arrays`, a location with `start` and `count` | DataArray (9), `PutDataSubarrays` (§13.3.7) |
//! | `declare_arrays` | DataArray (9), `PutUninitializedDataArrays` (§13.3.9) |
//!
//! The exchanges are `retp_client::arrays::{write_subarrays, declare_arrays}`;
//! nothing of the protocol is written here. What is here is the storage side
//! of the door: how an `ArrayLocation` and an `ArrayData` or `ArrayMetadata`
//! become the client's `SubarrayWrite` and `Declaration`.
//!
//! The checks that need the store's own state - a read-only transaction, the
//! array lying in this store's dataspace, the listing cache - stay in
//! `writer.rs`, which calls these with the identifier it has checked. The
//! helpers here take that identifier rather than recompute it: a second
//! implementation of the dataspace check would be a second door.
//!
//! # The addressing
//!
//! The same as `read_array` reads (`ArrayLocation::start` and `count`, per
//! dimension). A block is written where `start` says, `count` long:
//!
//! - `count` set, `start` empty: from the beginning (`start` is zeros), as
//!   the location documents;
//! - `start` set, `count` empty: **refused**. For a read that means « to the
//!   end of the dataset », which only the array's dimensions can say, and a
//!   write does not ask the store for them;
//! - the data must be shaped as `count` (`ArrayData::metadata.dimensions`) and
//!   hold that many values, or `ExternalDataReason::ShapeDisagrees` /
//!   a named backend refusal says so before anything leaves.
//!
//! A block is not checked against the array's declared dimensions here: the
//! store does not know them without asking. `TODO(verify)`: whether to ask
//! (`GetDataArrayMetadata`) before a block is sent; the client does the check
//! when a `SubarrayWrite` carries `declared`.
//!
//! # How to make the tests fail
//!
//! See `tests/blocks.rs`. In this file: drop the `start`/`count` length check in
//! `block_of` (a block shaped one way and placed another is then sent), map
//! every element type to `double` in `empty_values`, or skip the
//! `location.start`/`count` refusal in `declaration_of`.

use std::fmt::Display;

use energyml_storage::array::{ArrayData, ArrayLocation, ArrayMetadata, ArrayValues};
use energyml_storage::error::{Error, ExternalDataReason, Result};
use retp_client::arrays::{ArrayFailure, Declaration, SubarrayWrite, declare_arrays, write_subarrays};
use retp_client::{Clock, Transport};
use retp_types::datatypes::AnyLogicalArrayType;
use retp_types::datatypes::data_array_types::DataArrayIdentifier;

use crate::discovery::logical_symbol;
use crate::push::to_wire;
use crate::{DEFAULT_MESSAGE, EtpStore, element_type, remote};

/// Every logical array type, to find one by the word ETP spells it with
/// (`discovery::logical_symbol` is the one table of those words).
const LOGICALS: [AnyLogicalArrayType; 21] = [
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

fn named(id: &DataArrayIdentifier) -> String {
    format!("{} {}", id.uri, id.path_in_resource)
}

/// A refusal by name, in the words the writer uses for an array.
fn backend(id: &DataArrayIdentifier, why: impl Display) -> Error {
    Error::ExternalData {
        location: named(id),
        reason: ExternalDataReason::Backend(why.to_string()),
    }
}

/// What the client said of one array, as the writer says it (`array_error` of
/// `writer.rs`, whose words these are).
fn failure_error(id: &DataArrayIdentifier, failure: &ArrayFailure) -> Error {
    crate::writer::array_error(id, failure)
}

/// An empty vector of the storage type the element name stands for - the
/// names `ArrayValues::element_type` gives. `None` for a name this build does
/// not send.
fn empty_values(element_type: &str) -> Option<ArrayValues> {
    Some(match element_type {
        "double" => ArrayValues::F64(Vec::new()),
        "float" => ArrayValues::F32(Vec::new()),
        "int64" => ArrayValues::I64(Vec::new()),
        "int32" => ArrayValues::I32(Vec::new()),
        "int16" => ArrayValues::I16(Vec::new()),
        "int8" => ArrayValues::I8(Vec::new()),
        "uint64" => ArrayValues::U64(Vec::new()),
        "uint32" => ArrayValues::U32(Vec::new()),
        "uint16" => ArrayValues::U16(Vec::new()),
        "uint8" => ArrayValues::U8(Vec::new()),
        _ => return None,
    })
}

/// One block as the client writes it, or the reason it cannot be: pure, so
/// that it is tested without a session.
pub(crate) fn block_of(
    id: &DataArrayIdentifier,
    location: &ArrayLocation,
    data: &ArrayData,
) -> Result<SubarrayWrite> {
    // ETP 1.2 §23.32.7: `starts` and `counts` are both per dimension.
    if location.count.is_empty() {
        return Err(backend(
            id,
            "a block needs its `count` (ETP 1.2 §23.32.7): a `start` alone means « to the end » for a read, which only the array's dimensions can say",
        ));
    }
    if location.start.iter().chain(&location.count).any(|v| *v < 0) {
        return Err(backend(
            id,
            format!(
                "negative start {:?} or count {:?}",
                location.start, location.count
            ),
        ));
    }
    let counts: Vec<u64> = location.count.iter().map(|c| *c as u64).collect();
    // An empty `start` is « from the beginning », as the location documents.
    let starts: Vec<u64> = if location.start.is_empty() {
        vec![0; counts.len()]
    } else {
        location.start.iter().map(|s| *s as u64).collect()
    };
    if starts.len() != counts.len() {
        return Err(backend(
            id,
            format!(
                "the block starts at {:?} but is {:?} long: start and count are per dimension (ETP 1.2 §23.32.7)",
                location.start, location.count
            ),
        ));
    }
    // The values and the shape agree before anything leaves: a block that
    // promises more numbers than there are is a plausible block that nothing
    // flags.
    let promised = location.count.iter().fold(1i64, |acc, c| acc.saturating_mul(*c));
    if usize::try_from(promised).ok() != Some(data.values.len()) {
        return Err(Error::ExternalData {
            location: named(id),
            reason: ExternalDataReason::ShapeDisagrees {
                promised,
                held: data.values.len(),
            },
        });
    }
    if data.metadata.dimensions != location.count {
        return Err(backend(
            id,
            format!(
                "the data is shaped {:?} and the block's count says {:?}",
                data.metadata.dimensions, location.count
            ),
        ));
    }
    let block = to_wire(data).map_err(|why| backend(id, why))?;
    Ok(SubarrayWrite {
        id: id.clone(),
        starts,
        block,
        declared: None,
    })
}

/// One array as the client declares it, or the reason it cannot be: pure.
///
/// The element type gives the types through `to_wire` - the one conversion -,
/// so that a declaration and the values written after it travel as the same
/// type. A `logical_type` the metadata names is taken when it is the same
/// element (`element_type` of the crate root) and ETP 1.2 §13.2.2.1 allows the
/// transport the values will use (the client checks that); otherwise the
/// declaration is refused by name rather than declared as a type the values
/// will not match.
pub(crate) fn declaration_of(
    id: &DataArrayIdentifier,
    location: &ArrayLocation,
    metadata: &ArrayMetadata,
) -> Result<Declaration> {
    if !location.start.is_empty() || !location.count.is_empty() {
        return Err(backend(
            id,
            "declaring an array concerns the whole array (ETP 1.2 §13.3.9), not a block of it",
        ));
    }
    let dimensions = u64s(id, &metadata.dimensions, "dimension")?;
    let preferred = u64s(id, &metadata.preferred_subarray_dimensions, "preferred size")?;
    let values = empty_values(&metadata.element_type).ok_or_else(|| {
        backend(
            id,
            format!("this build does not declare {} arrays", metadata.element_type),
        )
    })?;
    let probe = to_wire(&ArrayData {
        metadata: metadata.clone(),
        values,
    })
    .map_err(|why| backend(id, why))?;
    let logical = match &metadata.logical_type {
        None => probe.logical,
        Some(symbol) => {
            let found = LOGICALS
                .iter()
                .copied()
                .find(|candidate| logical_symbol(*candidate) == symbol.as_str())
                .ok_or_else(|| backend(id, format!("`{symbol}` is not an ETP logical array type")))?;
            if element_type(found) != metadata.element_type {
                return Err(backend(
                    id,
                    format!(
                        "the logical type {symbol} does not match the element type {}",
                        metadata.element_type
                    ),
                ));
            }
            found
        }
    };
    Ok(Declaration {
        id: id.clone(),
        dimensions,
        logical,
        transport: probe.transport,
        preferred,
    })
}

fn u64s(id: &DataArrayIdentifier, values: &[i64], what: &str) -> Result<Vec<u64>> {
    values
        .iter()
        .map(|v| u64::try_from(*v).map_err(|_| backend(id, format!("negative {what} {v}"))))
        .collect()
}

impl<T: Transport, C: Clock> EtpStore<T, C> {
    /// Write blocks of arrays - `PutDataSubarrays` - one result per item, in
    /// order. Each item is its identifier (already checked to lie in this
    /// store's dataspace, `writer.rs`), the location whose `start` and `count`
    /// name the block, and the block's values.
    ///
    /// The caller forgets the listing cache afterwards.
    pub(crate) fn write_blocks(
        &self,
        items: &[(DataArrayIdentifier, &ArrayLocation, &ArrayData)],
    ) -> Vec<Result<()>> {
        let mut results: Vec<Option<Result<()>>> = items.iter().map(|_| None).collect();
        let mut slots = Vec::new();
        let mut writes = Vec::new();
        for (position, (id, location, data)) in items.iter().enumerate() {
            match block_of(id, location, data) {
                Ok(write) => {
                    slots.push(position);
                    writes.push(write);
                }
                Err(why) => results[position] = Some(Err(why)),
            }
        }
        if !writes.is_empty() {
            // ETP 1.2 §13.2.1.2 step 3.b; the client cuts a block that does not
            // fit a message.
            let written = write_subarrays(&mut *self.client(), &writes, DEFAULT_MESSAGE, |_| {});
            match written {
                Ok(outcomes) => {
                    let mut outcomes = outcomes.into_iter();
                    for (slot, write) in slots.iter().zip(&writes) {
                        results[*slot] = Some(match outcomes.next() {
                            Some(Ok(())) => Ok(()),
                            Some(Err(failure)) => Err(failure_error(&write.id, &failure)),
                            None => Err(remote(format!(
                                "{}: the client reported nothing for this block",
                                named(&write.id)
                            ))),
                        });
                    }
                }
                Err(why) => {
                    for slot in &slots {
                        results[*slot] = Some(Err(remote(&why)));
                    }
                }
            }
        }
        results
            .into_iter()
            .map(|one| one.unwrap_or_else(|| Err(remote("no answer was recorded for this block"))))
            .collect()
    }

    /// Declare arrays - `PutUninitializedDataArrays` - one result per item, in
    /// order. Each item is its identifier (already checked, as for
    /// [`Self::write_blocks`]), the location (whole: no `start`, no `count`)
    /// and the metadata whose dimensions and types are declared.
    ///
    /// The caller forgets the listing cache afterwards.
    pub(crate) fn declare(
        &self,
        items: &[(DataArrayIdentifier, &ArrayLocation, &ArrayMetadata)],
    ) -> Vec<Result<()>> {
        let mut results: Vec<Option<Result<()>>> = items.iter().map(|_| None).collect();
        let mut slots = Vec::new();
        let mut declarations = Vec::new();
        for (position, (id, location, metadata)) in items.iter().enumerate() {
            match declaration_of(id, location, metadata) {
                Ok(declaration) => {
                    slots.push(position);
                    declarations.push(declaration);
                }
                Err(why) => results[position] = Some(Err(why)),
            }
        }
        if !declarations.is_empty() {
            let declared = declare_arrays(&mut *self.client(), &declarations, DEFAULT_MESSAGE);
            match declared {
                Ok(outcomes) => {
                    let mut outcomes = outcomes.into_iter();
                    for (slot, declaration) in slots.iter().zip(&declarations) {
                        results[*slot] = Some(match outcomes.next() {
                            Some(Ok(())) => Ok(()),
                            Some(Err(failure)) => Err(failure_error(&declaration.id, &failure)),
                            None => Err(remote(format!(
                                "{}: the client reported nothing for this declaration",
                                named(&declaration.id)
                            ))),
                        });
                    }
                }
                Err(why) => {
                    for slot in &slots {
                        results[*slot] = Some(Err(remote(&why)));
                    }
                }
            }
        }
        results
            .into_iter()
            .map(|one| one.unwrap_or_else(|| Err(remote("no answer was recorded for this declaration"))))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use retp_types::datatypes::{AnyArrayType, AnyLogicalArrayType as L};

    fn id() -> DataArrayIdentifier {
        DataArrayIdentifier {
            uri: "eml:///dataspace('demo')/resqml20.obj_PointSetRepresentation(bb000000-0000-4000-8000-000000000002)"
                .into(),
            path_in_resource: "/points".into(),
        }
    }

    fn block(
        start: Vec<i64>,
        count: Vec<i64>,
        values: Vec<f64>,
        dims: Vec<i64>,
    ) -> (ArrayLocation, ArrayData) {
        let mut location = ArrayLocation::etp(id().uri, "/points");
        location.start = start;
        location.count = count;
        (
            location,
            ArrayData {
                metadata: ArrayMetadata::new(dims, "double"),
                values: ArrayValues::F64(values),
            },
        )
    }

    /// An empty `start` is the beginning; a `start` without a `count` is
    /// refused; the data must be shaped as the count.
    #[test]
    fn a_block_is_placed_by_its_start_and_shaped_by_its_count() {
        let (location, data) = block(vec![2, 0], vec![2, 3], vec![0.0; 6], vec![2, 3]);
        let write = block_of(&id(), &location, &data).unwrap();
        assert_eq!(write.starts, vec![2, 0]);
        assert_eq!(write.block.dimensions, vec![2, 3]);
        assert_eq!(write.declared, None);

        let (location, data) = block(vec![], vec![2, 3], vec![0.0; 6], vec![2, 3]);
        assert_eq!(block_of(&id(), &location, &data).unwrap().starts, vec![0, 0]);

        let (location, data) = block(vec![2, 0], vec![], vec![0.0; 6], vec![2, 3]);
        assert!(matches!(
            block_of(&id(), &location, &data),
            Err(Error::ExternalData { reason: ExternalDataReason::Backend(why), .. }) if why.contains("count")
        ));

        let (location, data) = block(vec![2], vec![2, 3], vec![0.0; 6], vec![2, 3]);
        assert!(
            block_of(&id(), &location, &data).is_err(),
            "start and count differ in length"
        );

        let (location, data) = block(vec![-1, 0], vec![2, 3], vec![0.0; 6], vec![2, 3]);
        assert!(block_of(&id(), &location, &data).is_err(), "negative start");

        let (location, data) = block(vec![0, 0], vec![2, 3], vec![0.0; 5], vec![2, 3]);
        assert!(matches!(
            block_of(&id(), &location, &data),
            Err(Error::ExternalData {
                reason: ExternalDataReason::ShapeDisagrees { promised: 6, held: 5 },
                ..
            })
        ));

        let (location, data) = block(vec![0, 0], vec![2, 3], vec![0.0; 6], vec![3, 2]);
        assert!(
            block_of(&id(), &location, &data).is_err(),
            "the data is shaped another way"
        );
    }

    /// The element type gives the types the values will travel in; a logical
    /// type of another element, or an element this build does not send, is
    /// refused by name.
    #[test]
    fn a_declaration_has_the_types_the_values_will_travel_in() {
        let whole = ArrayLocation::etp(id().uri, "/points");
        let declared = declaration_of(&id(), &whole, &ArrayMetadata::new(vec![4, 3], "int16")).unwrap();
        assert_eq!(declared.dimensions, vec![4, 3]);
        assert_eq!(declared.logical, L::ArrayOfInt16Le);
        assert_eq!(declared.transport, AnyArrayType::ArrayOfInt);

        let mut tagged = ArrayMetadata::new(vec![4], "double");
        tagged.logical_type = Some("arrayOfDouble64LE".into());
        assert_eq!(
            declaration_of(&id(), &whole, &tagged).unwrap().logical,
            L::ArrayOfDouble64Le
        );
        tagged.logical_type = Some("arrayOfInt32LE".into());
        assert!(declaration_of(&id(), &whole, &tagged).is_err(), "another element");
        tagged.logical_type = Some("arrayOfNothing".into());
        assert!(declaration_of(&id(), &whole, &tagged).is_err(), "not an ETP word");

        assert!(
            declaration_of(&id(), &whole, &ArrayMetadata::new(vec![4], "string")).is_err(),
            "an element this build does not send"
        );
        assert!(declaration_of(&id(), &whole, &ArrayMetadata::new(vec![-4], "double")).is_err());

        let mut part = whole.clone();
        part.count = vec![2];
        assert!(declaration_of(&id(), &part, &ArrayMetadata::new(vec![4], "double")).is_err());
    }

    /// The table of logical types holds every one of them once: the words of
    /// `logical_symbol` are all different, and each is found.
    #[test]
    fn the_logical_types_are_all_there_once() {
        let mut words: Vec<&str> = LOGICALS.iter().map(|l| logical_symbol(*l)).collect();
        words.sort_unstable();
        words.dedup();
        assert_eq!(words.len(), 21);
    }
}
