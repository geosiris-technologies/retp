//! Splitting a collection over several messages so that each fits
//! `MaxWebSocketMessagePayloadSize` (§3.3.2.9 §4, §3.7.3.1 §10).
//!
//! Only the collection field may vary between the parts of a multipart
//! message; everything else is identical. So the caller hands over the items
//! and a way to build the message around a subset, and this packs them.

use std::fmt;

use retp_avro::Avro;

/// The largest a `MessageHeader` can be: two `int`s and a flag `int` at five
/// bytes each, two `long`s at ten - every size check measures the header at
/// this, so a message never fails for an id that grew a byte.
pub const HEADER_MAX: usize = 5 + 5 + 10 + 10 + 5;

/// An item that does not fit a message on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Oversized {
    /// Which item, in the order given.
    pub index: usize,
    /// The size of a message holding only it, header included.
    pub size: usize,
    /// The limit.
    pub limit: usize,
}

impl fmt::Display for Oversized {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "item {} alone makes a message of {} bytes, over the {}-byte limit: send it in Chunks, or refuse it with EMAXSIZE_EXCEEDED",
            self.index, self.size, self.limit
        )
    }
}

impl std::error::Error for Oversized {}

/// Pack `items` into as few messages as fit `limit` bytes each, header
/// included, in order.
///
/// Each item's share is measured once - a message of that item alone, less
/// the empty message - and the packing adds shares, with ten bytes of slack
/// for the block count, which Avro writes as a varint whose length grows with
/// the count - so the estimate is never below the real size.
pub fn split_to_fit<T: Clone, M: Avro>(
    items: Vec<T>,
    limit: usize,
    build: impl Fn(Vec<T>) -> M,
) -> Result<Vec<M>, Oversized> {
    let budget = limit.saturating_sub(HEADER_MAX);
    let empty = build(Vec::new()).to_avro().len();
    let mut groups: Vec<Vec<T>> = Vec::new();
    let mut current: Vec<T> = Vec::new();
    let mut size = empty + 10;
    for (index, item) in items.into_iter().enumerate() {
        let alone = build(vec![item.clone()]).to_avro().len();
        if alone > budget {
            return Err(Oversized {
                index,
                size: alone + HEADER_MAX,
                limit,
            });
        }
        let share = alone - empty;
        if !current.is_empty() && size + share > budget {
            groups.push(std::mem::take(&mut current));
            size = empty + 10;
        }
        size += share;
        current.push(item);
    }
    if !current.is_empty() || groups.is_empty() {
        groups.push(current);
    }
    let messages: Vec<M> = groups.into_iter().map(&build).collect();
    // The arithmetic errs on the safe side; were it ever wrong, the wire's own
    // size check would still refuse the message by name.
    debug_assert!(messages.iter().all(|m| m.to_avro().len() + HEADER_MAX <= limit));
    Ok(messages)
}
