//! Writing a block the caller chose (`PutDataSubarrays`) and declaring an
//! array (`PutUninitializedDataArrays`), for any `Caller`.
//!
//! This module is a child of `arrays::asynchronous` **on purpose**: the helpers
//! it reuses - `prepare`, `block_request`, `said_by_key`, `entry_overhead`,
//! `nothing_said`, the `Prepared` plan - are private to that module, and a
//! second copy of them is exactly the two-doors defect this crate refuses.
//! The types and the pure checks are in `arrays::blocks`.
//!
//! # The order, and what it relies on
//!
//! ETP 1.2 §13.2.1.2: a customer declares an array that will not fit one
//! message (`PutUninitializedDataArrays`, step 1), then fills it with
//! `PutDataSubarrays` (step 3.b). [`declare_arrays`] is step 1 and
//! [`write_subarrays`] is step 3.b, on their own: `write_arrays_with` does both
//! for an array it holds whole; here the caller holds the block and chooses
//! where it goes.
//!
//! # Cutting
//!
//! A block is cut by [`plan`], the cut a whole array gets, under the same two
//! limits: one block may not exceed [`budget`] (the message, and both ends'
//! `MaxDataArraySize`, §13.2.2 row 5), and the blocks that share a message may
//! not exceed [`message_budget`]. A string block has no size before it is
//! written and travels alone, as `write_arrays_with` sends a string array.
//!
//! # What the messages promise
//!
//! The answers are read per key (§3.7.3): a refusal in a message of five marks
//! one block, hence one item, and the others go on. The first failure names the
//! item; a block that goes after it changes nothing about that.
//!
//! The items of one call are in flight `at_once` at a time and are **not
//! ordered against each other**: two items whose blocks overlap leave the
//! store with either one. Items that must be applied in order are separate
//! calls.
//!
//! # How to make the tests fail
//!
//! See `crates/retp-client/tests/blocks.rs`: sending the relative start of a
//! cut block (drop the `+= offset` shift in `write_subarrays`), keying every
//! block `"1"` in a shared message, or declaring after the blocks each break
//! one of them.

use super::*;
use crate::arrays::blocks::{Declaration, SubarrayWrite, check_declaration, check_subarray};

/// Put blocks, each at the place its `starts` name, and say what became of
/// each item - one outcome per item, in order.
///
/// An item that cannot be sent (a shape that lies, a block outside the
/// declared array, an element bigger than a message) fails alone, before any
/// request leaves; the others are written. A request the session cannot make
/// stops all of them, as a [`ClientError`].
///
/// Progress says `Planned` (once per item, with the number of blocks the item
/// is cut into) and `Written` (one more block of the item is in).
pub async fn write_subarrays<K: Caller>(
    client: &mut K,
    items: &[SubarrayWrite],
    default_message: u64,
    mut progress: impl FnMut(ArrayProgress),
) -> Result<Vec<Result<(), ArrayFailure>>, ClientError> {
    let negotiated = client.negotiated();
    let per_array = budget(&negotiated, default_message);
    let per_message = message_budget(&negotiated, default_message);
    let window = client.at_once().max(1);

    // Each item checked, its values against its shape, and planned: the cut
    // `write_arrays_with` uses, over the block's own shape.
    let mut results: Vec<Option<Result<(), ArrayFailure>>> = vec![None; items.len()];
    let mut prepared: Vec<Option<Prepared>> = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let ready = check_subarray(&item.starts, &item.block.dimensions, item.declared.as_deref())
            .and_then(|()| prepare(&item.id, &item.block, per_array, &mut progress));
        match ready {
            Ok(ready) => prepared.push(Some(ready)),
            Err(failure) => {
                results[index] = Some(Err(failure));
                prepared.push(None);
            }
        }
    }

    // The blocks of every item, in order, packed into messages greedily: a
    // block goes with the one before as long as the message holds both.
    let mut groups: Vec<Vec<(usize, usize)>> = Vec::new();
    let mut filling: Vec<(usize, usize)> = Vec::new();
    let mut filled = 0u64;
    for (index, item) in items.iter().enumerate() {
        let Some(ready) = &prepared[index] else { continue };
        let element = transport_element_bound(item.block.transport, item.block.logical);
        for (rank, block) in ready.blocks.iter().enumerate() {
            let cost = element.map(|element| {
                block
                    .len()
                    .saturating_mul(element)
                    .saturating_add(entry_overhead(&item.id))
            });
            match cost {
                // No size before it is written: alone.
                None => {
                    if !filling.is_empty() {
                        groups.push(std::mem::take(&mut filling));
                        filled = 0;
                    }
                    groups.push(vec![(index, rank)]);
                }
                Some(cost) => {
                    if !filling.is_empty() && filled.saturating_add(cost) > per_message {
                        groups.push(std::mem::take(&mut filling));
                        filled = 0;
                    }
                    filling.push((index, rank));
                    filled = filled.saturating_add(cost);
                }
            }
        }
    }
    if !filling.is_empty() {
        groups.push(filling);
    }

    // Built a window at a time: what waits in memory is one window of
    // messages, never the whole write twice.
    let mut done: BTreeMap<usize, usize> = BTreeMap::new();
    for chunk in groups.chunks(window) {
        let requests: Vec<PutDataSubarrays> = chunk
            .iter()
            .map(|group| PutDataSubarrays {
                data_subarrays: group
                    .iter()
                    .enumerate()
                    .map(|(k, &(index, rank))| {
                        let item = &items[index];
                        let ready = prepared[index].as_ref().expect("planned");
                        // The block as `write_arrays_with` builds it, relative to
                        // the block's own shape (`block_request` carves it out);
                        // its place in the array is the item's start plus the
                        // block's own.
                        let mut one = block_request(&item.id, &item.block, ready, &ready.blocks[rank]);
                        let mut entry = one
                            .data_subarrays
                            .remove("1")
                            .expect("block_request keys its one entry \"1\"");
                        for (start, offset) in entry.starts.iter_mut().zip(&item.starts) {
                            // `check_subarray` saw that the end fits a long.
                            *start += *offset as i64;
                        }
                        ((k + 1).to_string(), entry)
                    })
                    .collect(),
            })
            .collect();
        for (group, outcome) in chunk.iter().zip(client.call_all(&requests).await) {
            let said = said_by_key(outcome?, group.len(), |m| {
                matches!(m, Message::PutDataSubarraysResponse(_))
            });
            for (&(index, _), one) in group.iter().zip(said) {
                match one {
                    Err(failure) if results[index].is_none() => results[index] = Some(Err(failure)),
                    Err(_) => {}
                    Ok(()) => {
                        let of = prepared[index].as_ref().expect("planned").blocks.len();
                        let count = done.entry(index).or_insert(0);
                        *count += 1;
                        progress(ArrayProgress::Written {
                            path: items[index].id.path_in_resource.clone(),
                            done: *count,
                            of,
                        });
                        if *count == of && results[index].is_none() {
                            results[index] = Some(Ok(()));
                        }
                    }
                }
            }
        }
    }
    Ok(results
        .into_iter()
        .map(|one| one.unwrap_or(Err(ArrayFailure::Store(nothing_said()))))
        .collect())
}

/// Declare arrays - `PutUninitializedDataArrays`, §13.3.9 - and say what
/// became of each, one outcome per item, in order.
///
/// What is declared is the shape and the two types; no value. The items that
/// pass [`check_declaration`] go in as few messages as the budget takes, and
/// each answers on its own key (§3.7.3). `default_message` stands for a
/// message size neither end announced.
///
/// `TODO(verify)`: §13.2.3 defines `MaxDataArraySize` as *« the maximum size
/// in bytes of a data array allowed in a store »*. Read literally, an array
/// bigger than the store's value cannot be declared at all, which would make
/// cutting an array into blocks pointless; `write_arrays_with` reads it as the
/// size of **what travels** and declares such arrays. This function does the
/// same and sends the declaration as it is: a store that means the literal
/// reading denies it with `ELIMIT_EXCEEDED` (12, §13.2.2 row 5), and the denial
/// is that item's outcome.
pub async fn declare_arrays<K: Caller>(
    client: &mut K,
    items: &[Declaration],
    default_message: u64,
) -> Result<Vec<Result<(), ArrayFailure>>, ClientError> {
    let per_message = message_budget(&client.negotiated(), default_message);
    let window = client.at_once().max(1);

    let mut results: Vec<Option<Result<(), ArrayFailure>>> = vec![None; items.len()];
    let mut metadata: Vec<Option<DataArrayMetadata>> = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        match check_declaration(item) {
            Ok(meta) => metadata.push(Some(meta)),
            Err(failure) => {
                results[index] = Some(Err(failure));
                metadata.push(None);
            }
        }
    }

    // A declaration costs its identifier, and a varint (ten bytes at worst)
    // per dimension and per preferred size.
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut filling: Vec<usize> = Vec::new();
    let mut filled = 0u64;
    for (index, item) in items.iter().enumerate() {
        if metadata[index].is_none() {
            continue;
        }
        let cost = entry_overhead(&item.id)
            .saturating_add(10 * (item.dimensions.len() + item.preferred.len()) as u64);
        if !filling.is_empty() && filled.saturating_add(cost) > per_message {
            groups.push(std::mem::take(&mut filling));
            filled = 0;
        }
        filling.push(index);
        filled = filled.saturating_add(cost);
    }
    if !filling.is_empty() {
        groups.push(filling);
    }

    for chunk in groups.chunks(window) {
        let requests: Vec<PutUninitializedDataArrays> = chunk
            .iter()
            .map(|group| PutUninitializedDataArrays {
                data_arrays: group
                    .iter()
                    .enumerate()
                    .map(|(k, &index)| {
                        (
                            (k + 1).to_string(),
                            PutUninitializedDataArrayType {
                                uid: items[index].id.clone(),
                                metadata: metadata[index].clone().expect("checked"),
                            },
                        )
                    })
                    .collect(),
            })
            .collect();
        for (group, outcome) in chunk.iter().zip(client.call_all(&requests).await) {
            let said = said_by_key(outcome?, group.len(), |m| {
                matches!(m, Message::PutUninitializedDataArraysResponse(_))
            });
            for (&index, one) in group.iter().zip(said) {
                results[index] = Some(one);
            }
        }
    }
    Ok(results
        .into_iter()
        .map(|one| one.unwrap_or(Err(ArrayFailure::Store(nothing_said()))))
        .collect())
}
