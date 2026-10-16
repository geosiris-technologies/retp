//! Reading data arrays - DataArray, protocol 9, the way §13.2.1.1 asks.
//!
//! *« First a customer needs to determine the type and dimensions of the
//! arrays »*: `GetDataArrayMetadata`. Then, from those dimensions, *« if the
//! data array is small enough that it can be retrieved with one call »*,
//! `GetDataArrays`; *« if the data arrays are too large to get in one call,
//! then the customer must retrieve the array in multiple calls, using
//! GetDataSubarrays »* and assemble the pieces. And neither end may exceed
//! the other's `MaxDataArraySize` (§13.2.2 row 5).
//!
//! What decides is a **budget** - the bytes of array one message may carry -
//! and a **plan**: which blocks, of which shape. Both are plain functions,
//! tested without a network; the exchange only follows the plan, and checks
//! every block it gets back against what it asked for.

use std::collections::BTreeMap;
use std::fmt;

use retp_avro::{Avro, EtpMessage};
use retp_proto::capabilities::names;
use retp_proto::negotiate::Negotiated;
use retp_proto::wire::Tolerance;
use retp_proto::wire::{Body, Delivery, Exception};
use retp_types::datatypes::data_array_types::{
    DataArray, DataArrayIdentifier, DataArrayMetadata, GetDataSubarraysType, PutDataArraysType,
    PutDataSubarraysType, PutUninitializedDataArrayType,
};
use retp_types::datatypes::{
    AnyArray, AnyArrayItem, AnyArrayType, AnyLogicalArrayType, ArrayOfBoolean, ArrayOfDouble, ArrayOfFloat,
    ArrayOfInt, ArrayOfLong, ArrayOfString, ErrorInfo,
};
use retp_types::dispatch::Message;
use retp_types::protocol::data_array::{
    GetDataArrayMetadata, GetDataArrays, GetDataSubarrays, PutDataArrays, PutDataSubarrays,
    PutUninitializedDataArrays,
};

use crate::ClientError;
use crate::caller::{Blocking, run};

/// Writing one block the caller chose (`PutDataSubarrays`) and declaring an
/// array before writing it (`PutUninitializedDataArrays`), on the blocking client.
mod blocks;
pub use blocks::{
    Declaration, SubarrayWrite, check_declaration, check_subarray, declare_arrays, write_subarrays,
};

/// DataArray's protocol number.
pub const DATA_ARRAY: i32 = 9;

/// The bytes one element takes in the store's accounting - the logical type,
/// as `MaxDataArraySize` counts (§13.2.3): *« the product of all array
/// dimensions multiplied by the size in bytes of a single array element »*.
/// `None` for strings and custom types, which have no fixed size.
pub fn logical_element_size(logical: AnyLogicalArrayType) -> Option<u64> {
    use AnyLogicalArrayType as L;
    Some(match logical {
        L::ArrayOfBoolean | L::ArrayOfInt8 | L::ArrayOfUInt8 => 1,
        L::ArrayOfInt16Le | L::ArrayOfUInt16Le | L::ArrayOfInt16Be | L::ArrayOfUInt16Be => 2,
        L::ArrayOfInt32Le | L::ArrayOfUInt32Le | L::ArrayOfInt32Be | L::ArrayOfUInt32Be => 4,
        L::ArrayOfFloat32Le | L::ArrayOfFloat32Be => 4,
        L::ArrayOfInt64Le | L::ArrayOfUInt64Le | L::ArrayOfInt64Be | L::ArrayOfUInt64Be => 8,
        L::ArrayOfDouble64Le | L::ArrayOfDouble64Be => 8,
        L::ArrayOfString | L::ArrayOfCustom => return None,
    })
}

/// The most bytes one element can take **on the wire**: `bytes` carries the
/// logical size (§13.2.2.1 rule 2), `arrayOfInt` and `arrayOfLong` are Avro
/// varints - 5 and 10 bytes at worst (rule 3) -, `arrayOfBoolean` one byte.
pub fn transport_element_bound(transport: AnyArrayType, logical: AnyLogicalArrayType) -> Option<u64> {
    Some(match transport {
        AnyArrayType::Bytes => logical_element_size(logical)?,
        AnyArrayType::ArrayOfBoolean => 1,
        AnyArrayType::ArrayOfInt => 5,
        AnyArrayType::ArrayOfLong => 10,
        AnyArrayType::ArrayOfFloat => 4,
        AnyArrayType::ArrayOfDouble => 8,
        AnyArrayType::ArrayOfString => return None,
    })
}

/// The transport types §13.2.2.1 allows for a logical type: *« Values for
/// logicalArrayType can only use specific values for transportArrayType »*.
/// Empty for `arrayOfCustom`, which the specification leaves open.
pub fn allowed_transports(logical: AnyLogicalArrayType) -> &'static [AnyArrayType] {
    use AnyArrayType as T;
    use AnyLogicalArrayType as L;
    match logical {
        L::ArrayOfBoolean => &[T::ArrayOfBoolean],
        L::ArrayOfInt8
        | L::ArrayOfUInt8
        | L::ArrayOfInt16Le
        | L::ArrayOfInt32Le
        | L::ArrayOfUInt16Le
        | L::ArrayOfUInt32Le => &[T::Bytes, T::ArrayOfInt, T::ArrayOfLong],
        L::ArrayOfInt64Le | L::ArrayOfUInt64Le => &[T::Bytes, T::ArrayOfLong],
        L::ArrayOfFloat32Le => &[T::ArrayOfFloat],
        L::ArrayOfDouble64Le => &[T::ArrayOfDouble],
        L::ArrayOfInt16Be
        | L::ArrayOfInt32Be
        | L::ArrayOfInt64Be
        | L::ArrayOfUInt16Be
        | L::ArrayOfUInt32Be
        | L::ArrayOfUInt64Be
        | L::ArrayOfFloat32Be
        | L::ArrayOfDouble64Be => &[T::Bytes],
        L::ArrayOfString => &[T::ArrayOfString],
        L::ArrayOfCustom => &[],
    }
}

/// The one logical type a transport type can mean on its own, when a store
/// names a logical type the transport cannot carry: `arrayOfDouble` is only
/// ever `arrayOfDouble64LE`. `None` for `bytes`, which says nothing of what
/// the bytes are.
pub fn logical_of(transport: AnyArrayType) -> Option<AnyLogicalArrayType> {
    use AnyArrayType as T;
    use AnyLogicalArrayType as L;
    Some(match transport {
        T::ArrayOfBoolean => L::ArrayOfBoolean,
        T::ArrayOfInt => L::ArrayOfInt32Le,
        T::ArrayOfLong => L::ArrayOfInt64Le,
        T::ArrayOfFloat => L::ArrayOfFloat32Le,
        T::ArrayOfDouble => L::ArrayOfDouble64Le,
        T::ArrayOfString => L::ArrayOfString,
        T::Bytes => return None,
    })
}

/// The logical type to read an array as: the store's, when its transport
/// may carry it; otherwise - a store that sends `arrayOfBoolean` as
/// `arrayOfDouble`, as one vendor's RDDMS does - the one the transport
/// means, taken and logged when tolerant, refused when strict or
/// when the transport says nothing (`bytes`).
pub fn reading_type(
    path: &str,
    logical: AnyLogicalArrayType,
    transport: AnyArrayType,
    tolerance: Tolerance,
) -> Result<AnyLogicalArrayType, ArrayFailure> {
    let allowed = allowed_transports(logical);
    if allowed.is_empty() || allowed.contains(&transport) {
        return Ok(logical);
    }
    match (logical_of(transport), tolerance) {
        (Some(meant), Tolerance::Tolerant) => {
            tracing::warn!(
                target: "retp_proto",
                "{path}: the store says {logical:?} carried as {transport:?}, which §13.2.2.1 does not allow; read as {meant:?}"
            );
            Ok(meant)
        }
        _ => Err(ArrayFailure::Inconsistent(format!(
            "{logical:?} cannot travel as {transport:?} (§13.2.2.1), and nothing says what it is"
        ))),
    }
}

/// The bytes of array **one message** may carry in this session: the
/// negotiated message size, less a sixteenth for the header, the keys, the
/// dimensions and the Avro block counts around the values.
/// `default_message` stands for a message size no end announced.
///
/// `MaxDataArraySize` does not come in: it is « the maximum size in bytes of
/// **a data array** » (§13.2.3), which limits one array or one of its
/// blocks, not what a message may carry - so several blocks of that size may
/// travel together.
pub fn message_budget(negotiated: &Negotiated, default_message: u64) -> u64 {
    let message = negotiated
        .max_payload()
        .and_then(|m| u64::try_from(m).ok())
        .unwrap_or(default_message);
    message.saturating_sub(message / 16 + 1024).max(1)
}

/// The bytes **one array, or one block of it**, may be: the smaller of
/// [`message_budget`] and both ends' `MaxDataArraySize`.
pub fn budget(negotiated: &Negotiated, default_message: u64) -> u64 {
    let mut budget = message_budget(negotiated, default_message);
    if let Some(protocol) = negotiated.protocols.get(&DATA_ARRAY) {
        for side in [&protocol.ours, &protocol.theirs] {
            if let Some(limit) = side
                .long(names::MAX_DATA_ARRAY_SIZE)
                .and_then(|l| u64::try_from(l).ok())
            {
                budget = budget.min(limit);
            }
        }
    }
    budget
}

/// One block of an array: where it starts, and how far it goes, per
/// dimension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    /// The first index, per dimension.
    pub starts: Vec<u64>,
    /// How many, per dimension.
    pub counts: Vec<u64>,
}

impl Block {
    /// The elements it holds.
    pub fn len(&self) -> u64 {
        self.counts.iter().product()
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// How an array is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// In one `GetDataArrays`.
    Whole,
    /// In these blocks, by `GetDataSubarrays`, in row-major order.
    Blocks(Vec<Block>),
}

/// Why no plan could be made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanRefusal(pub String);

impl fmt::Display for PlanRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The shape of the blocks: whole inner dimensions first - row-major, the
/// order of HDF5 -, the outermost split; when one row is already too big, the
/// next dimension in is split as well, and so on.
fn block_shape(dimensions: &[u64], element: u64, budget: u64) -> Vec<u64> {
    let mut shape = vec![1; dimensions.len()];
    let mut inner = element;
    for k in (0..dimensions.len()).rev() {
        let whole = dimensions[k].saturating_mul(inner);
        if whole <= budget {
            shape[k] = dimensions[k];
            inner = whole;
        } else {
            shape[k] = (budget / inner).clamp(1, dimensions[k].max(1));
            break;
        }
    }
    shape
}

/// Plan the reading of an array of `dimensions`, whose elements take
/// `element` bytes at most on the wire, within `budget` bytes a message.
///
/// `preferred` is the store's native chunking (`preferredSubarrayDimensions`):
/// reading against it *« may result in significant performance issues »*, so
/// it is the block shape whenever it fits the budget.
pub fn plan(dimensions: &[u64], element: u64, budget: u64, preferred: &[u64]) -> Result<Plan, PlanRefusal> {
    if dimensions.is_empty() {
        return Err(PlanRefusal("an array without dimensions".into()));
    }
    if element > budget {
        return Err(PlanRefusal(format!(
            "one element ({element} bytes) is more than one message may carry ({budget} bytes)"
        )));
    }
    let total = dimensions.iter().try_fold(element, |acc, d| acc.checked_mul(*d));
    if total.is_some_and(|t| t <= budget) {
        return Ok(Plan::Whole);
    }
    let fits = preferred.len() == dimensions.len()
        && preferred.iter().all(|p| *p > 0)
        && preferred
            .iter()
            .try_fold(element, |acc, p| acc.checked_mul(*p))
            .is_some_and(|b| b <= budget);
    let shape: Vec<u64> = if fits {
        preferred
            .iter()
            .zip(dimensions)
            .map(|(p, d)| (*p).min(*d))
            .collect()
    } else {
        block_shape(dimensions, element, budget)
    };
    // Every start of the grid, in row-major order.
    let mut blocks = Vec::new();
    let mut starts = vec![0u64; dimensions.len()];
    if dimensions.contains(&0) {
        return Ok(Plan::Whole);
    }
    loop {
        let counts = starts
            .iter()
            .zip(shape.iter().zip(dimensions))
            .map(|(s, (b, d))| (*b).min(d - s))
            .collect();
        blocks.push(Block {
            starts: starts.clone(),
            counts,
        });
        // The next start: the last dimension moves fastest.
        let mut k = dimensions.len();
        loop {
            if k == 0 {
                return Ok(Plan::Blocks(blocks));
            }
            k -= 1;
            starts[k] += shape[k];
            if starts[k] < dimensions[k] {
                break;
            }
            starts[k] = 0;
        }
    }
}

/// The values of an array, in the type they travelled in.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayValues {
    /// `arrayOfBoolean`.
    Booleans(Vec<bool>),
    /// `arrayOfInt`.
    Ints(Vec<i32>),
    /// `arrayOfLong`.
    Longs(Vec<i64>),
    /// `arrayOfFloat`.
    Floats(Vec<f32>),
    /// `arrayOfDouble`.
    Doubles(Vec<f64>),
    /// `arrayOfString`.
    Strings(Vec<String>),
    /// `bytes`: each element the logical size, in the logical byte order.
    Bytes(Vec<u8>),
}

impl ArrayValues {
    /// The values as their **logical** type says they are: `bytes` decoded
    /// by its size, sign and byte order; `arrayOfInt` and `arrayOfLong` cast
    /// to the width the logical type names - reinterpreted, for the unsigned
    /// ones, as §13.2.2.1 asks. `None` for bytes of a type without a fixed
    /// size.
    pub fn decode(&self, logical: AnyLogicalArrayType) -> Option<Decoded> {
        use AnyLogicalArrayType as L;
        Some(match self {
            ArrayValues::Booleans(v) => Decoded::Booleans(v.clone()),
            ArrayValues::Strings(v) => Decoded::Strings(v.clone()),
            ArrayValues::Floats(v) => Decoded::F32(v.clone()),
            ArrayValues::Doubles(v) => Decoded::F64(v.clone()),
            ArrayValues::Ints(v) => match logical {
                L::ArrayOfInt8 => Decoded::I8(v.iter().map(|x| *x as i8).collect()),
                L::ArrayOfUInt8 => Decoded::U8(v.iter().map(|x| *x as u8).collect()),
                L::ArrayOfInt16Le => Decoded::I16(v.iter().map(|x| *x as i16).collect()),
                L::ArrayOfUInt16Le => Decoded::U16(v.iter().map(|x| *x as u16).collect()),
                L::ArrayOfUInt32Le => Decoded::U32(v.iter().map(|x| *x as u32).collect()),
                _ => Decoded::I32(v.clone()),
            },
            ArrayValues::Longs(v) => match logical {
                L::ArrayOfInt8 => Decoded::I8(v.iter().map(|x| *x as i8).collect()),
                L::ArrayOfUInt8 => Decoded::U8(v.iter().map(|x| *x as u8).collect()),
                L::ArrayOfInt16Le => Decoded::I16(v.iter().map(|x| *x as i16).collect()),
                L::ArrayOfUInt16Le => Decoded::U16(v.iter().map(|x| *x as u16).collect()),
                L::ArrayOfInt32Le => Decoded::I32(v.iter().map(|x| *x as i32).collect()),
                L::ArrayOfUInt32Le => Decoded::U32(v.iter().map(|x| *x as u32).collect()),
                L::ArrayOfUInt64Le => Decoded::U64(v.iter().map(|x| *x as u64).collect()),
                _ => Decoded::I64(v.clone()),
            },
            ArrayValues::Bytes(bytes) => {
                let size = logical_element_size(logical)? as usize;
                let chunks = bytes.chunks_exact(size);
                macro_rules! read {
                    ($variant:ident, $ty:ty, $from:ident) => {
                        Decoded::$variant(
                            chunks
                                .map(|c| <$ty>::$from(c.try_into().expect("sized")))
                                .collect(),
                        )
                    };
                }
                match logical {
                    L::ArrayOfBoolean => Decoded::Booleans(bytes.iter().map(|b| *b != 0).collect()),
                    L::ArrayOfUInt8 => Decoded::U8(bytes.clone()),
                    L::ArrayOfInt8 => Decoded::I8(bytes.iter().map(|b| *b as i8).collect()),
                    L::ArrayOfInt16Le => read!(I16, i16, from_le_bytes),
                    L::ArrayOfInt16Be => read!(I16, i16, from_be_bytes),
                    L::ArrayOfUInt16Le => read!(U16, u16, from_le_bytes),
                    L::ArrayOfUInt16Be => read!(U16, u16, from_be_bytes),
                    L::ArrayOfInt32Le => read!(I32, i32, from_le_bytes),
                    L::ArrayOfInt32Be => read!(I32, i32, from_be_bytes),
                    L::ArrayOfUInt32Le => read!(U32, u32, from_le_bytes),
                    L::ArrayOfUInt32Be => read!(U32, u32, from_be_bytes),
                    L::ArrayOfInt64Le => read!(I64, i64, from_le_bytes),
                    L::ArrayOfInt64Be => read!(I64, i64, from_be_bytes),
                    L::ArrayOfUInt64Le => read!(U64, u64, from_le_bytes),
                    L::ArrayOfUInt64Be => read!(U64, u64, from_be_bytes),
                    L::ArrayOfFloat32Le => read!(F32, f32, from_le_bytes),
                    L::ArrayOfFloat32Be => read!(F32, f32, from_be_bytes),
                    L::ArrayOfDouble64Le => read!(F64, f64, from_le_bytes),
                    L::ArrayOfDouble64Be => read!(F64, f64, from_be_bytes),
                    L::ArrayOfString | L::ArrayOfCustom => return None,
                }
            }
        })
    }

    /// The values as numbers, whatever they travelled in - [`Self::decode`],
    /// then widened. `None` for strings.
    pub fn to_f64s(&self, logical: AnyLogicalArrayType) -> Option<Vec<f64>> {
        Some(match self.decode(logical)? {
            Decoded::F64(v) => v,
            Decoded::F32(v) => v.into_iter().map(f64::from).collect(),
            Decoded::I64(v) => v.into_iter().map(|x| x as f64).collect(),
            Decoded::I32(v) => v.into_iter().map(f64::from).collect(),
            Decoded::I16(v) => v.into_iter().map(f64::from).collect(),
            Decoded::I8(v) => v.into_iter().map(f64::from).collect(),
            Decoded::U64(v) => v.into_iter().map(|x| x as f64).collect(),
            Decoded::U32(v) => v.into_iter().map(f64::from).collect(),
            Decoded::U16(v) => v.into_iter().map(f64::from).collect(),
            Decoded::U8(v) => v.into_iter().map(f64::from).collect(),
            Decoded::Booleans(v) => v.into_iter().map(|b| f64::from(u8::from(b))).collect(),
            Decoded::Strings(_) => return None,
        })
    }

    /// How many units the vector holds - bytes for `bytes`, elements
    /// otherwise.
    fn units(&self) -> usize {
        match self {
            ArrayValues::Booleans(v) => v.len(),
            ArrayValues::Ints(v) => v.len(),
            ArrayValues::Longs(v) => v.len(),
            ArrayValues::Floats(v) => v.len(),
            ArrayValues::Doubles(v) => v.len(),
            ArrayValues::Strings(v) => v.len(),
            ArrayValues::Bytes(v) => v.len(),
        }
    }

    fn transport(&self) -> AnyArrayType {
        match self {
            ArrayValues::Booleans(_) => AnyArrayType::ArrayOfBoolean,
            ArrayValues::Ints(_) => AnyArrayType::ArrayOfInt,
            ArrayValues::Longs(_) => AnyArrayType::ArrayOfLong,
            ArrayValues::Floats(_) => AnyArrayType::ArrayOfFloat,
            ArrayValues::Doubles(_) => AnyArrayType::ArrayOfDouble,
            ArrayValues::Strings(_) => AnyArrayType::ArrayOfString,
            ArrayValues::Bytes(_) => AnyArrayType::Bytes,
        }
    }

    /// The Avro union member these values travel in - the inverse of
    /// [`ArrayValues::from_item`].
    fn into_item(self) -> AnyArrayItem {
        match self {
            ArrayValues::Booleans(values) => AnyArrayItem::ArrayOfBoolean(ArrayOfBoolean { values }),
            ArrayValues::Ints(values) => AnyArrayItem::ArrayOfInt(ArrayOfInt { values }),
            ArrayValues::Longs(values) => AnyArrayItem::ArrayOfLong(ArrayOfLong { values }),
            ArrayValues::Floats(values) => AnyArrayItem::ArrayOfFloat(ArrayOfFloat { values }),
            ArrayValues::Doubles(values) => AnyArrayItem::ArrayOfDouble(ArrayOfDouble { values }),
            ArrayValues::Strings(values) => AnyArrayItem::ArrayOfString(ArrayOfString { values }),
            ArrayValues::Bytes(bytes) => AnyArrayItem::Bytes(bytes),
        }
    }

    fn from_item(item: AnyArrayItem) -> Self {
        match item {
            AnyArrayItem::ArrayOfBoolean(a) => ArrayValues::Booleans(a.values),
            AnyArrayItem::ArrayOfInt(a) => ArrayValues::Ints(a.values),
            AnyArrayItem::ArrayOfLong(a) => ArrayValues::Longs(a.values),
            AnyArrayItem::ArrayOfFloat(a) => ArrayValues::Floats(a.values),
            AnyArrayItem::ArrayOfDouble(a) => ArrayValues::Doubles(a.values),
            AnyArrayItem::ArrayOfString(a) => ArrayValues::Strings(a.values),
            AnyArrayItem::Bytes(b) => ArrayValues::Bytes(b),
        }
    }

    fn empty(transport: AnyArrayType, units: usize) -> Self {
        match transport {
            AnyArrayType::ArrayOfBoolean => ArrayValues::Booleans(vec![false; units]),
            AnyArrayType::ArrayOfInt => ArrayValues::Ints(vec![0; units]),
            AnyArrayType::ArrayOfLong => ArrayValues::Longs(vec![0; units]),
            AnyArrayType::ArrayOfFloat => ArrayValues::Floats(vec![0.0; units]),
            AnyArrayType::ArrayOfDouble => ArrayValues::Doubles(vec![0.0; units]),
            AnyArrayType::ArrayOfString => ArrayValues::Strings(vec![String::new(); units]),
            AnyArrayType::Bytes => ArrayValues::Bytes(vec![0; units]),
        }
    }

    /// Copy `len` units from `from` at `at_from` to `at` in `self`.
    fn copy_run(&mut self, at: usize, from: &ArrayValues, at_from: usize, len: usize) {
        match (self, from) {
            (ArrayValues::Booleans(d), ArrayValues::Booleans(s)) => {
                d[at..at + len].copy_from_slice(&s[at_from..at_from + len])
            }
            (ArrayValues::Ints(d), ArrayValues::Ints(s)) => {
                d[at..at + len].copy_from_slice(&s[at_from..at_from + len])
            }
            (ArrayValues::Longs(d), ArrayValues::Longs(s)) => {
                d[at..at + len].copy_from_slice(&s[at_from..at_from + len])
            }
            (ArrayValues::Floats(d), ArrayValues::Floats(s)) => {
                d[at..at + len].copy_from_slice(&s[at_from..at_from + len])
            }
            (ArrayValues::Doubles(d), ArrayValues::Doubles(s)) => {
                d[at..at + len].copy_from_slice(&s[at_from..at_from + len])
            }
            (ArrayValues::Strings(d), ArrayValues::Strings(s)) => {
                d[at..at + len].clone_from_slice(&s[at_from..at_from + len])
            }
            (ArrayValues::Bytes(d), ArrayValues::Bytes(s)) => {
                d[at..at + len].copy_from_slice(&s[at_from..at_from + len])
            }
            _ => unreachable!("the transport is checked before copying"),
        }
    }
}

/// An array's values as their logical type says they are - what a reader
/// wants, whatever the store sent them as.
#[derive(Debug, Clone, PartialEq)]
pub enum Decoded {
    /// 64-bit floats.
    F64(Vec<f64>),
    /// 32-bit floats.
    F32(Vec<f32>),
    /// Signed 64-bit integers.
    I64(Vec<i64>),
    /// Signed 32-bit integers.
    I32(Vec<i32>),
    /// Signed 16-bit integers.
    I16(Vec<i16>),
    /// Signed 8-bit integers.
    I8(Vec<i8>),
    /// Unsigned 64-bit integers.
    U64(Vec<u64>),
    /// Unsigned 32-bit integers.
    U32(Vec<u32>),
    /// Unsigned 16-bit integers.
    U16(Vec<u16>),
    /// Unsigned 8-bit integers.
    U8(Vec<u8>),
    /// Flags.
    Booleans(Vec<bool>),
    /// Strings.
    Strings(Vec<String>),
}

/// An array read in full.
#[derive(Debug, Clone, PartialEq)]
pub struct Array {
    /// Its dimensions, outermost first.
    pub dimensions: Vec<u64>,
    /// What its elements are.
    pub logical: AnyLogicalArrayType,
    /// How they travelled.
    pub transport: AnyArrayType,
    /// Its values, row-major.
    pub values: ArrayValues,
    /// How it was read: whole, or in how many blocks.
    pub blocks: usize,
}

/// How several arrays travel when written together.
///
/// The wire allows several arrays in one `PutDataArrays` (§13.1.2, a map by
/// key), and a round trip costs more than what it carries: the arrays of one
/// island go **together**, as many per message as the negotiated budget
/// takes, and their blocks in flight `at_once` at a time. That is the
/// default, as the chunking of a big object is - nobody is asked to manage
/// it. [`Grouping::OneByOne`] is the bypass: each array in its own message,
/// one after the other, for a store that mishandles the map or to see what
/// one array alone provokes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Grouping {
    /// Several per message, under the budget; blocks in flight.
    #[default]
    Together,
    /// One array per message, each awaited before the next.
    OneByOne,
}

/// Why one array could not be read.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayFailure {
    /// The store answered it with an error.
    Store(ErrorInfo),
    /// No plan fits: an element bigger than a message, a string array whose
    /// size cannot be known before it is read and that is too big to be read
    /// whole.
    Plan(String),
    /// What came back is not what was asked: other dimensions, another
    /// transport type than the metadata said, the wrong count of values.
    Inconsistent(String),
}

impl fmt::Display for ArrayFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArrayFailure::Store(error) => write!(f, "error {}: {}", error.code, error.message),
            ArrayFailure::Plan(why) => write!(f, "no way to read it: {why}"),
            ArrayFailure::Inconsistent(why) => write!(f, "the store's answer does not hold together: {why}"),
        }
    }
}

/// Where a reading is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrayProgress {
    /// The metadata came: this array will be read whole, or in `blocks`.
    Planned {
        /// The array.
        path: String,
        /// Its blocks, 1 when whole.
        blocks: usize,
        /// Its size, in the store's accounting, when it has one.
        bytes: Option<u64>,
    },
    /// One more request answered.
    Read {
        /// Requests answered.
        done: usize,
        /// Requests to make.
        total: usize,
    },
    /// One more block of this array written.
    ///
    /// A variant of its own rather than [`ArrayProgress::Read`] reused: a
    /// progress bar saying *read* while a write is in flight is a small lie,
    /// and the one place somebody looks when a write hangs.
    Written {
        /// The array.
        path: String,
        /// Blocks written.
        done: usize,
        /// Blocks to write.
        of: usize,
    },
}

/// Keys `"1"`, `"2"`… for a map request; the answers are keyed back by
/// position.
fn keyed<T: Clone>(items: &[T]) -> BTreeMap<String, T> {
    items
        .iter()
        .enumerate()
        .map(|(i, t)| ((i + 1).to_string(), t.clone()))
        .collect()
}

/// The answers of a map request, key by key (§3.7.3).
struct MapAnswer<V> {
    /// What came, by key.
    values: BTreeMap<String, V>,
    /// The error each key got.
    errors: BTreeMap<String, ErrorInfo>,
    /// The error that ended the whole answer, if one did.
    ended: Option<ErrorInfo>,
}

/// The array reads, for any `Caller` - written once, and run on the
/// blocking client by the functions beside this module.
pub mod asynchronous {
    /// The same two exchanges on a [`Caller`], built on this module's own
    /// block requests.
    mod blocks;
    use super::*;
    use crate::caller::Caller;
    pub use blocks::{declare_arrays, write_subarrays};

    async fn answers<K: Caller, V>(
        client: &mut K,
        request: &impl retp_avro::EtpMessage,
        take: impl FnMut(Message) -> Option<BTreeMap<String, V>>,
    ) -> Result<MapAnswer<V>, ClientError> {
        Ok(map_answer(client.call(request).await?, take))
    }

    /// A map answer, read off the deliveries of one call.
    fn map_answer<V>(
        deliveries: Vec<Delivery>,
        mut take: impl FnMut(Message) -> Option<BTreeMap<String, V>>,
    ) -> MapAnswer<V> {
        let mut values = BTreeMap::new();
        let mut errors = BTreeMap::new();
        let mut ended = None;
        for delivery in deliveries {
            let Body::Message(message) = delivery.body else {
                continue;
            };
            match message {
                Message::ProtocolException(exception) => match Exception::from_message(&exception) {
                    Ok(Exception::Map(map)) => errors.extend(map),
                    Ok(Exception::Single(error)) => ended = Some(error),
                    Err(_) => {}
                },
                other => {
                    if let Some(map) = take(other) {
                        values.extend(map);
                    }
                }
            }
        }
        MapAnswer {
            values,
            errors,
            ended,
        }
    }

    /// `GetDataArrayMetadata`: each array's dimensions and types, or its error.
    pub async fn array_metadata<K: Caller>(
        client: &mut K,
        ids: &[DataArrayIdentifier],
    ) -> Result<Vec<Result<DataArrayMetadata, ErrorInfo>>, ClientError> {
        let request = GetDataArrayMetadata {
            data_arrays: keyed(ids),
        };
        let MapAnswer {
            values: mut found,
            errors,
            ended,
        } = answers(client, &request, |m| match m {
            Message::GetDataArrayMetadataResponse(r) => Some(r.array_metadata),
            _ => None,
        })
        .await?;
        // In the order asked, which is **not** the order of the keys: a map
        // is keyed by strings, and `"10"` sorts before `"2"` (§3.7.3). Ten
        // arrays in one message was the first time it showed.
        Ok((1..=ids.len())
            .map(|position| {
                let key = position.to_string();
                found.remove(&key).ok_or_else(|| missing(&key, &errors, &ended))
            })
            .collect())
    }

    /// Read `ids` in full, each as §13.2.1.1 asks: the metadata first, then
    /// whole arrays by `GetDataArrays` and the others by `GetDataSubarrays`,
    /// never more than [`budget`] bytes a message. `default_message` stands for a
    /// message size neither end announced.
    ///
    /// Each array gets its own answer; a request the session cannot make stops
    /// all of them, as a [`ClientError`].
    pub async fn read_arrays<K: Caller>(
        client: &mut K,
        ids: &[DataArrayIdentifier],
        default_message: u64,
        progress: impl FnMut(ArrayProgress),
    ) -> Result<Vec<Result<Array, ArrayFailure>>, ClientError> {
        let whole: Vec<(DataArrayIdentifier, Option<Block>)> =
            ids.iter().map(|id| (id.clone(), None)).collect();
        read_regions(client, &whole, default_message, progress).await
    }

    /// Write arrays, each whole or **block by block**, and say what became of
    /// each.
    ///
    /// The cut is [`plan`], the same one reading uses: *what fits a message*
    /// is one question, and answering it twice is how a writer and a reader
    /// end up disagreeing about an array neither can show is wrong. An array
    /// that fits goes in one `PutDataArrays`; one that does not is
    /// **declared first** - `PutUninitializedDataArrays`, so the store knows
    /// its shape and type before any values - and then filled by
    /// `PutDataSubarrays`, block by block.
    ///
    /// The order matters and is not an optimisation: a subarray written into
    /// an array the store has not been told about has nowhere to go, and
    /// stores differ on whether they invent one. Declaring first is what the
    /// specification's own sequence does (§13.1.2).
    ///
    /// Each array answers on its own. A block that fails marks **its** array
    /// and the others go on: an array half written is a failure that must be
    /// named, never one that carries off its neighbours.
    pub async fn write_arrays<K: Caller>(
        client: &mut K,
        writes: &[(DataArrayIdentifier, Array)],
        default_message: u64,
        progress: impl FnMut(ArrayProgress),
    ) -> Result<Vec<Result<(), ArrayFailure>>, ClientError> {
        write_arrays_with(client, writes, default_message, Grouping::Together, progress).await
    }

    /// [`write_arrays`], told how the arrays travel. [`Grouping::Together`]
    /// is what `write_arrays` does: the whole arrays packed into as few
    /// `PutDataArrays` as the message budget allows, the others declared in
    /// one `PutUninitializedDataArrays`, and every message in flight
    /// `at_once` at a time. [`Grouping::OneByOne`] is the bypass.
    ///
    /// Each array still answers on its own, by its key (§3.7.3): a refusal
    /// in a message of five marks one array, and the four go through.
    pub async fn write_arrays_with<K: Caller>(
        client: &mut K,
        writes: &[(DataArrayIdentifier, Array)],
        default_message: u64,
        grouping: Grouping,
        mut progress: impl FnMut(ArrayProgress),
    ) -> Result<Vec<Result<(), ArrayFailure>>, ClientError> {
        let budget = budget(&client.negotiated(), default_message);
        if grouping == Grouping::OneByOne {
            let mut results: Vec<Result<(), ArrayFailure>> = Vec::with_capacity(writes.len());
            for (id, array) in writes {
                results.push(write_one(client, id, array, budget, &mut progress).await?);
            }
            return Ok(results);
        }
        let message = message_budget(&client.negotiated(), default_message);
        write_together(client, writes, budget, message, &mut progress).await
    }

    /// An array checked and planned: what the messages need of it.
    struct Prepared {
        /// Bytes per element of the values when the transport is `Bytes`, 1 otherwise.
        unit: usize,
        /// Its blocks - one, the whole, when it fits.
        blocks: Vec<Block>,
        /// As `PutUninitializedDataArrays` declares it.
        metadata: DataArrayMetadata,
        /// Its size on the wire, when its type has one; strings have none.
        bytes: Option<u64>,
    }

    impl Prepared {
        fn is_whole(&self, dimensions: &[u64]) -> bool {
            self.blocks.len() == 1 && self.blocks[0].counts == dimensions
        }
    }

    /// Check the values against the dimensions and plan the blocks - the
    /// same [`plan`] reading uses. Says `Planned`.
    fn prepare(
        id: &DataArrayIdentifier,
        array: &Array,
        budget: u64,
        progress: &mut impl FnMut(ArrayProgress),
    ) -> Result<Prepared, ArrayFailure> {
        let unit = if array.transport == AnyArrayType::Bytes {
            logical_element_size(array.logical).unwrap_or(1) as usize
        } else {
            1
        };
        let count: u64 = array.dimensions.iter().product();
        if array.values.units() != count as usize * unit {
            return Err(ArrayFailure::Inconsistent(format!(
                "{:?} needs {} {}, {} were given",
                array.dimensions,
                count as usize * unit,
                if unit > 1 { "bytes" } else { "values" },
                array.values.units()
            )));
        }
        let element = transport_element_bound(array.transport, array.logical);
        let planned = match element {
            Some(element) => plan(&array.dimensions, element, budget, &[]),
            // Strings have no size before they are written; whole it is, and a
            // store that cannot take it says so.
            None => Ok(Plan::Whole),
        };
        let blocks: Vec<Block> = match planned {
            Ok(Plan::Whole) => vec![Block {
                starts: vec![0; array.dimensions.len()],
                counts: array.dimensions.clone(),
            }],
            Ok(Plan::Blocks(blocks)) => blocks,
            Err(refusal) => return Err(ArrayFailure::Plan(refusal.0)),
        };
        progress(ArrayProgress::Planned {
            path: id.path_in_resource.clone(),
            blocks: blocks.len(),
            bytes: logical_element_size(array.logical).map(|e| e.saturating_mul(count)),
        });
        Ok(Prepared {
            unit,
            blocks,
            metadata: DataArrayMetadata {
                dimensions: array.dimensions.iter().map(|d| *d as i64).collect(),
                transport_array_type: array.transport,
                logical_array_type: array.logical,
                ..Default::default()
            },
            bytes: element.map(|e| e.saturating_mul(count)),
        })
    }

    /// What one entry costs a message beyond its values: the identifier,
    /// the key, the dimensions, the Avro counts around the values.
    fn entry_overhead(id: &DataArrayIdentifier) -> u64 {
        (id.uri.len() + id.path_in_resource.len()) as u64 + 64
    }

    /// The whole array as one `PutDataArrays` entry.
    fn whole_entry(id: &DataArrayIdentifier, array: &Array, prepared: &Prepared) -> PutDataArraysType {
        PutDataArraysType {
            uid: id.clone(),
            array: DataArray {
                dimensions: prepared.metadata.dimensions.clone(),
                data: AnyArray {
                    item: array.values.clone().into_item(),
                },
            },
            custom_data: BTreeMap::new(),
        }
    }

    /// One block of an array as a `PutDataSubarrays` of its own.
    fn block_request(
        id: &DataArrayIdentifier,
        array: &Array,
        prepared: &Prepared,
        block: &Block,
    ) -> PutDataSubarrays {
        PutDataSubarrays {
            data_subarrays: [(
                "1".to_string(),
                PutDataSubarraysType {
                    uid: id.clone(),
                    data: AnyArray {
                        item: carve(&array.values, &array.dimensions, block, prepared.unit).into_item(),
                    },
                    starts: block.starts.iter().map(|s| *s as i64).collect(),
                    counts: block.counts.iter().map(|c| *c as i64).collect(),
                },
            )]
            .into_iter()
            .collect(),
        }
    }

    /// The arrays together: the whole ones packed under `message` bytes a
    /// `PutDataArrays`, the others declared in one message and filled by
    /// their blocks, every message in flight `at_once` at a time. A group
    /// is built only when its turn comes, so that what waits in memory is
    /// one window of messages, never the island twice.
    async fn write_together<K: Caller>(
        client: &mut K,
        writes: &[(DataArrayIdentifier, Array)],
        budget: u64,
        message: u64,
        progress: &mut impl FnMut(ArrayProgress),
    ) -> Result<Vec<Result<(), ArrayFailure>>, ClientError> {
        let window = client.at_once().max(1);
        let mut results: Vec<Option<Result<(), ArrayFailure>>> = vec![None; writes.len()];
        let mut prepared: Vec<Option<Prepared>> = Vec::with_capacity(writes.len());
        for (index, (id, array)) in writes.iter().enumerate() {
            match prepare(id, array, budget, progress) {
                Ok(ready) => prepared.push(Some(ready)),
                Err(failure) => {
                    results[index] = Some(Err(failure));
                    prepared.push(None);
                }
            }
        }

        // The whole arrays, greedily: an entry goes with the one before as
        // long as the message holds both; one of no known size goes alone.
        let mut groups: Vec<Vec<usize>> = Vec::new();
        let mut filling: Vec<usize> = Vec::new();
        let mut filled = 0u64;
        for (index, (id, array)) in writes.iter().enumerate() {
            let Some(ready) = &prepared[index] else { continue };
            if !ready.is_whole(&array.dimensions) {
                continue;
            }
            match ready.bytes.map(|bytes| bytes + entry_overhead(id)) {
                None => {
                    if !filling.is_empty() {
                        groups.push(std::mem::take(&mut filling));
                        filled = 0;
                    }
                    groups.push(vec![index]);
                }
                Some(cost) => {
                    if !filling.is_empty() && filled + cost > message {
                        groups.push(std::mem::take(&mut filling));
                        filled = 0;
                    }
                    filling.push(index);
                    filled += cost;
                }
            }
        }
        if !filling.is_empty() {
            groups.push(filling);
        }
        for chunk in groups.chunks(window) {
            let requests: Vec<PutDataArrays> = chunk
                .iter()
                .map(|group| PutDataArrays {
                    data_arrays: group
                        .iter()
                        .enumerate()
                        .map(|(k, &index)| {
                            let (id, array) = &writes[index];
                            (
                                (k + 1).to_string(),
                                whole_entry(id, array, prepared[index].as_ref().expect("planned")),
                            )
                        })
                        .collect(),
                })
                .collect();
            for (group, outcome) in chunk.iter().zip(client.call_all(&requests).await) {
                let said = said_by_key(outcome?, group.len(), |m| {
                    matches!(m, Message::PutDataArraysResponse(_))
                });
                for (&index, one) in group.iter().zip(said) {
                    results[index] = Some(one);
                }
            }
        }

        // The others: declared together, so that every block has somewhere
        // to go, then filled - the blocks of all of them in flight.
        let blocked: Vec<usize> = (0..writes.len())
            .filter(|&i| results[i].is_none() && prepared[i].is_some())
            .collect();
        if !blocked.is_empty() {
            let declare = PutUninitializedDataArrays {
                data_arrays: blocked
                    .iter()
                    .enumerate()
                    .map(|(k, &index)| {
                        (
                            (k + 1).to_string(),
                            PutUninitializedDataArrayType {
                                uid: writes[index].0.clone(),
                                metadata: prepared[index].as_ref().expect("planned").metadata.clone(),
                            },
                        )
                    })
                    .collect(),
            };
            let said = said_by_key(client.call(&declare).await?, blocked.len(), |m| {
                matches!(m, Message::PutUninitializedDataArraysResponse(_))
            });
            for (&index, one) in blocked.iter().zip(said) {
                if one.is_err() {
                    results[index] = Some(one);
                }
            }
            // (array index, block rank) in order; the requests are built a
            // window at a time.
            let mut runs: Vec<(usize, usize)> = Vec::new();
            for &index in &blocked {
                if results[index].is_none() {
                    runs.extend(
                        (0..prepared[index].as_ref().expect("planned").blocks.len())
                            .map(|rank| (index, rank)),
                    );
                }
            }
            let mut done: BTreeMap<usize, usize> = BTreeMap::new();
            for chunk in runs.chunks(window) {
                let requests: Vec<PutDataSubarrays> = chunk
                    .iter()
                    .map(|&(index, rank)| {
                        let (id, array) = &writes[index];
                        let ready = prepared[index].as_ref().expect("planned");
                        block_request(id, array, ready, &ready.blocks[rank])
                    })
                    .collect();
                for (&(index, _), outcome) in chunk.iter().zip(client.call_all(&requests).await) {
                    let one = said_by_key(outcome?, 1, |m| matches!(m, Message::PutDataSubarraysResponse(_)))
                        .remove(0);
                    match one {
                        // The first failure names the array; a block that
                        // went after it changes nothing about that.
                        Err(failure) if results[index].is_none() => results[index] = Some(Err(failure)),
                        Err(_) => {}
                        Ok(()) => {
                            let of = prepared[index].as_ref().expect("planned").blocks.len();
                            let count = done.entry(index).or_insert(0);
                            *count += 1;
                            progress(ArrayProgress::Written {
                                path: writes[index].0.path_in_resource.clone(),
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

    /// One array: the plan, then the messages it calls for, each awaited.
    async fn write_one<K: Caller>(
        client: &mut K,
        id: &DataArrayIdentifier,
        array: &Array,
        budget: u64,
        progress: &mut impl FnMut(ArrayProgress),
    ) -> Result<Result<(), ArrayFailure>, ClientError> {
        let ready = match prepare(id, array, budget, progress) {
            Ok(ready) => ready,
            Err(failure) => return Ok(Err(failure)),
        };
        // Whole: one message, and the store allocates as it writes.
        if ready.is_whole(&array.dimensions) {
            let request = PutDataArrays {
                data_arrays: [("1".to_string(), whole_entry(id, array, &ready))]
                    .into_iter()
                    .collect(),
            };
            return said_by(client, &request, |m| {
                matches!(m, Message::PutDataArraysResponse(_))
            })
            .await;
        }

        // In blocks: the shape first, so that every subarray has somewhere to
        // go, then the values.
        let declare = PutUninitializedDataArrays {
            data_arrays: [(
                "1".to_string(),
                PutUninitializedDataArrayType {
                    uid: id.clone(),
                    metadata: ready.metadata.clone(),
                },
            )]
            .into_iter()
            .collect(),
        };
        if let Err(failure) = said_by(client, &declare, |m| {
            matches!(m, Message::PutUninitializedDataArraysResponse(_))
        })
        .await?
        {
            return Ok(Err(failure));
        }
        for (done, block) in ready.blocks.iter().enumerate() {
            let request = block_request(id, array, &ready, block);
            if let Err(failure) = said_by(client, &request, |m| {
                matches!(m, Message::PutDataSubarraysResponse(_))
            })
            .await?
            {
                return Ok(Err(failure));
            }
            progress(ArrayProgress::Written {
                path: id.path_in_resource.clone(),
                done: done + 1,
                of: ready.blocks.len(),
            });
        }
        Ok(Ok(()))
    }

    /// The failure of a write nobody stated the outcome of.
    fn nothing_said() -> ErrorInfo {
        crate::operations::asynchronous::nothing_said()
    }

    /// What a keyed write said of each of its `n` keys, `1..=n` - the
    /// reading every keyed request shares (`operations::keyed_outcomes`),
    /// each failure named as the store's.
    fn said_by_key(
        deliveries: Vec<Delivery>,
        n: usize,
        is_the_answer: impl Fn(&Message) -> bool,
    ) -> Vec<Result<(), ArrayFailure>> {
        crate::operations::asynchronous::keyed_outcomes(deliveries, n, is_the_answer)
            .into_iter()
            .map(|one| one.map_err(ArrayFailure::Store))
            .collect()
    }

    /// Send one write and read what the store said of it: the response it
    /// expects, or the error it sent instead.
    async fn said_by<K: Caller, R: EtpMessage + Avro>(
        client: &mut K,
        request: &R,
        is_the_answer: impl Fn(&Message) -> bool,
    ) -> Result<Result<(), ArrayFailure>, ClientError> {
        let mut answered = false;
        let mut failed = None;
        for delivery in client.call(request).await? {
            let Body::Message(message) = &delivery.body else {
                continue;
            };
            match message {
                Message::ProtocolException(exception) => match Exception::from_message(exception) {
                    Ok(Exception::Map(errors)) => failed = errors.into_values().next(),
                    Ok(Exception::Single(error)) => failed = Some(error),
                    Err(_) => {}
                },
                other if is_the_answer(other) => answered = true,
                _ => {}
            }
        }
        Ok(match (answered, failed) {
            (_, Some(error)) => Err(ArrayFailure::Store(error)),
            (true, None) => Ok(()),
            // Nothing said is not success: a write whose outcome nobody stated
            // is the one a caller must not take for done.
            (false, None) => Err(ArrayFailure::Store(nothing_said())),
        })
    }

    /// Read each array whole, or only a region of it - the `StartIndex` and
    /// `Count` an eml 2.3 `ExternalDataArrayPart` carries. A region is read by
    /// `GetDataSubarrays`, cut into blocks like a whole array when it does not fit
    /// one message; the result has the region's dimensions. A region that does
    /// not lie inside the array is refused by name, not clipped.
    pub async fn read_regions<K: Caller>(
        client: &mut K,
        requests: &[(DataArrayIdentifier, Option<Block>)],
        default_message: u64,
        mut progress: impl FnMut(ArrayProgress),
    ) -> Result<Vec<Result<Array, ArrayFailure>>, ClientError> {
        let ids: Vec<DataArrayIdentifier> = requests.iter().map(|(id, _)| id.clone()).collect();
        let ids = ids.as_slice();
        let budget = budget(&client.negotiated(), default_message);
        // What one array may be, and what one message may carry, are two
        // limits: blocks of the first size ride together up to the second.
        let per_message = message_budget(&client.negotiated(), default_message);
        let tolerance = client.tolerance();
        let metadata = array_metadata(client, ids).await?;

        // What each array needs.
        let mut assemblies: Vec<Option<Assembly>> = Vec::new();
        let mut whole: Vec<(usize, u64)> = Vec::new();
        // Each block of a sub-array read: its array, where it is in the array
        // (what is asked), where it is in what is assembled, and its size.
        let mut pieces: Vec<(usize, Block, Block, u64)> = Vec::new();
        let mut results: Vec<Option<Result<Array, ArrayFailure>>> = vec![None; ids.len()];
        for (i, answer) in metadata.into_iter().enumerate() {
            let mut meta = match answer {
                Ok(meta) => meta,
                Err(error) => {
                    results[i] = Some(Err(ArrayFailure::Store(error)));
                    assemblies.push(None);
                    continue;
                }
            };
            match reading_type(
                &ids[i].path_in_resource,
                meta.logical_array_type,
                meta.transport_array_type,
                tolerance,
            ) {
                Ok(logical) => meta.logical_array_type = logical,
                Err(failure) => {
                    results[i] = Some(Err(failure));
                    assemblies.push(None);
                    continue;
                }
            }
            let full: Vec<u64> = meta.dimensions.iter().map(|d| (*d).max(0) as u64).collect();
            let preferred: Vec<u64> = meta
                .preferred_subarray_dimensions
                .iter()
                .map(|d| (*d).max(0) as u64)
                .collect();
            // What is asked: all of it, or a region of it.
            let (origin, dimensions) = match &requests[i].1 {
                None => (vec![0; full.len()], full.clone()),
                Some(region) => {
                    let inside = region.starts.len() == full.len()
                        && region.counts.len() == full.len()
                        && region
                            .starts
                            .iter()
                            .zip(&region.counts)
                            .zip(&full)
                            .all(|((s, c), d)| s + c <= *d);
                    if !inside {
                        results[i] = Some(Err(ArrayFailure::Plan(format!(
                            "the region from {:?}, {:?} long, is not inside the array of {full:?}",
                            region.starts, region.counts
                        ))));
                        assemblies.push(None);
                        continue;
                    }
                    (region.starts.clone(), region.counts.clone())
                }
            };
            let whole_array = origin.iter().all(|s| *s == 0) && dimensions == full;
            let unit = if meta.transport_array_type == AnyArrayType::Bytes {
                logical_element_size(meta.logical_array_type).unwrap_or(1) as usize
            } else {
                1
            };
            let count: u64 = dimensions.iter().product();
            let bytes = logical_element_size(meta.logical_array_type).map(|e| e.saturating_mul(count));
            let planned = match transport_element_bound(meta.transport_array_type, meta.logical_array_type) {
                Some(element) => plan(&dimensions, element, budget, &preferred),
                // Strings: no size before reading. Whole it is, and a store that
                // cannot send it whole will say so.
                None => Ok(Plan::Whole),
            };
            let on_the_wire = transport_element_bound(meta.transport_array_type, meta.logical_array_type)
                .map_or(0, |element| element.saturating_mul(count));
            let element =
                transport_element_bound(meta.transport_array_type, meta.logical_array_type).unwrap_or(1);
            let at = |relative: &Block| Block {
                starts: relative.starts.iter().zip(&origin).map(|(s, o)| s + o).collect(),
                counts: relative.counts.clone(),
            };
            let blocks = match planned {
                Ok(Plan::Whole) if whole_array => {
                    whole.push((i, on_the_wire));
                    1
                }
                Ok(Plan::Whole) => {
                    // A region that fits: one sub-array, the region itself.
                    let relative = Block {
                        starts: vec![0; dimensions.len()],
                        counts: dimensions.clone(),
                    };
                    pieces.push((i, at(&relative), relative, on_the_wire));
                    1
                }
                Ok(Plan::Blocks(blocks)) => {
                    let n = blocks.len();
                    pieces.extend(blocks.into_iter().map(|b| {
                        let size = b.len() * element;
                        (i, at(&b), b, size)
                    }));
                    n
                }
                Err(refusal) => {
                    results[i] = Some(Err(ArrayFailure::Plan(refusal.0)));
                    assemblies.push(None);
                    continue;
                }
            };
            progress(ArrayProgress::Planned {
                path: ids[i].path_in_resource.clone(),
                blocks,
                bytes,
            });
            assemblies.push(Some(Assembly {
                values: ArrayValues::empty(meta.transport_array_type, count as usize * unit),
                dimensions,
                logical: meta.logical_array_type,
                transport: meta.transport_array_type,
                unit,
                blocks,
                failure: None,
            }));
        }

        // Requests: whole arrays, then blocks, grouped while they fit one
        // message - one round trip for as many as it may carry.
        let groups = |items: Vec<(usize, u64)>| -> Vec<Vec<usize>> {
            let mut out: Vec<Vec<usize>> = Vec::new();
            let mut size = 0;
            for (index, bytes) in items {
                if out.is_empty() || size + bytes > per_message {
                    out.push(Vec::new());
                    size = 0;
                }
                out.last_mut().expect("pushed").push(index);
                size += bytes;
            }
            out
        };
        let whole_groups = groups(whole.clone());
        let piece_groups = groups(
            pieces
                .iter()
                .enumerate()
                .map(|(n, (_, _, _, size))| (n, *size))
                .collect(),
        );
        let total = whole_groups.len() + piece_groups.len();
        let mut done = 0;

        // **The groups go out together**, `at_once` in flight:
        // each is its own request, answered to its own id, and a
        // group the store refuses fails alone - the arrays it carried are
        // marked, the others are not.
        let whole_requests: Vec<GetDataArrays> = whole_groups
            .iter()
            .map(|group| {
                let asked: Vec<DataArrayIdentifier> = group.iter().map(|i| ids[*i].clone()).collect();
                GetDataArrays {
                    data_arrays: keyed(&asked),
                }
            })
            .collect();
        let whole_answers = client.call_all(&whole_requests).await;
        for (group, answer) in whole_groups.iter().zip(whole_answers) {
            let MapAnswer {
                values: mut found,
                errors,
                ended,
            } = match answer {
                Ok(deliveries) => map_answer(deliveries, |m| match m {
                    Message::GetDataArraysResponse(r) => Some(r.data_arrays),
                    _ => None,
                }),
                Err(why) => {
                    for i in group {
                        let assembly = assemblies[*i].as_mut().expect("planned");
                        assembly.failure = Some(ArrayFailure::Store(ErrorInfo {
                            message: why.to_string(),
                            code: -1,
                        }));
                    }
                    done += 1;
                    progress(ArrayProgress::Read { done, total });
                    continue;
                }
            };
            for (n, i) in group.iter().enumerate() {
                let key = (n + 1).to_string();
                let assembly = assemblies[*i].as_mut().expect("planned");
                match found.remove(&key) {
                    Some(data) => {
                        let everything = Block {
                            starts: vec![0; assembly.dimensions.len()],
                            counts: assembly.dimensions.clone(),
                        };
                        assembly.place(&everything, data);
                    }
                    None => assembly.failure = Some(ArrayFailure::Store(missing(&key, &errors, &ended))),
                }
            }
            done += 1;
            progress(ArrayProgress::Read { done, total });
        }

        let piece_requests: Vec<GetDataSubarrays> = piece_groups
            .iter()
            .map(|group| {
                let asked: Vec<GetDataSubarraysType> = group
                    .iter()
                    .map(|n| {
                        let (i, block, _, _) = &pieces[*n];
                        GetDataSubarraysType {
                            uid: ids[*i].clone(),
                            starts: block.starts.iter().map(|s| *s as i64).collect(),
                            counts: block.counts.iter().map(|c| *c as i64).collect(),
                        }
                    })
                    .collect();
                GetDataSubarrays {
                    data_subarrays: keyed(&asked),
                }
            })
            .collect();
        let piece_answers = client.call_all(&piece_requests).await;
        for (group, answer) in piece_groups.iter().zip(piece_answers) {
            let MapAnswer {
                values: mut found,
                errors,
                ended,
            } = match answer {
                Ok(deliveries) => map_answer(deliveries, |m| match m {
                    Message::GetDataSubarraysResponse(r) => Some(r.data_subarrays),
                    _ => None,
                }),
                Err(why) => {
                    for n in group {
                        let (i, _, _, _) = &pieces[*n];
                        let assembly = assemblies[*i].as_mut().expect("planned");
                        if assembly.failure.is_none() {
                            assembly.failure = Some(ArrayFailure::Store(ErrorInfo {
                                message: why.to_string(),
                                code: -1,
                            }));
                        }
                    }
                    done += 1;
                    progress(ArrayProgress::Read { done, total });
                    continue;
                }
            };
            for (k, n) in group.iter().enumerate() {
                let key = (k + 1).to_string();
                let (i, _, block, _) = &pieces[*n];
                let assembly = assemblies[*i].as_mut().expect("planned");
                match found.remove(&key) {
                    Some(data) => assembly.place(block, data),
                    None => {
                        if assembly.failure.is_none() {
                            assembly.failure = Some(ArrayFailure::Store(missing(&key, &errors, &ended)));
                        }
                    }
                }
            }
            done += 1;
            progress(ArrayProgress::Read { done, total });
        }

        Ok(results
            .into_iter()
            .zip(assemblies)
            .map(|(result, assembly)| match (result, assembly) {
                (Some(result), _) => result,
                (None, Some(assembly)) => match assembly.failure {
                    Some(failure) => Err(failure),
                    None => Ok(Array {
                        dimensions: assembly.dimensions,
                        logical: assembly.logical,
                        transport: assembly.transport,
                        values: assembly.values,
                        blocks: assembly.blocks,
                    }),
                },
                (None, None) => unreachable!("an array without a plan has a result"),
            })
            .collect())
    }
}

/// [`asynchronous::array_metadata`], on the blocking client.
pub fn array_metadata<K: Blocking>(
    client: &mut K,
    ids: &[DataArrayIdentifier],
) -> Result<Vec<Result<DataArrayMetadata, ErrorInfo>>, ClientError> {
    run(asynchronous::array_metadata(client, ids))
}

/// [`asynchronous::read_arrays`], on the blocking client.
pub fn read_arrays<K: Blocking>(
    client: &mut K,
    ids: &[DataArrayIdentifier],
    default_message: u64,
    progress: impl FnMut(ArrayProgress),
) -> Result<Vec<Result<Array, ArrayFailure>>, ClientError> {
    run(asynchronous::read_arrays(client, ids, default_message, progress))
}

/// [`asynchronous::write_arrays`], on the blocking client.
pub fn write_arrays<K: Blocking>(
    client: &mut K,
    writes: &[(DataArrayIdentifier, Array)],
    default_message: u64,
    progress: impl FnMut(ArrayProgress),
) -> Result<Vec<Result<(), ArrayFailure>>, ClientError> {
    run(asynchronous::write_arrays(
        client,
        writes,
        default_message,
        progress,
    ))
}

/// [`asynchronous::write_arrays_with`], on the blocking client.
pub fn write_arrays_with<K: Blocking>(
    client: &mut K,
    writes: &[(DataArrayIdentifier, Array)],
    default_message: u64,
    grouping: Grouping,
    progress: impl FnMut(ArrayProgress),
) -> Result<Vec<Result<(), ArrayFailure>>, ClientError> {
    run(asynchronous::write_arrays_with(
        client,
        writes,
        default_message,
        grouping,
        progress,
    ))
}

/// [`asynchronous::read_regions`], on the blocking client.
pub fn read_regions<K: Blocking>(
    client: &mut K,
    requests: &[(DataArrayIdentifier, Option<Block>)],
    default_message: u64,
    progress: impl FnMut(ArrayProgress),
) -> Result<Vec<Result<Array, ArrayFailure>>, ClientError> {
    run(asynchronous::read_regions(
        client,
        requests,
        default_message,
        progress,
    ))
}

fn missing(key: &str, errors: &BTreeMap<String, ErrorInfo>, ended: &Option<ErrorInfo>) -> ErrorInfo {
    errors
        .get(key)
        .cloned()
        .or_else(|| ended.clone())
        .unwrap_or_else(|| ErrorInfo {
            message: "the store answered nothing for this array".into(),
            code: 0,
        })
}

/// An array being assembled.
struct Assembly {
    dimensions: Vec<u64>,
    logical: AnyLogicalArrayType,
    transport: AnyArrayType,
    /// Units per element: the logical size for `bytes`, 1 otherwise.
    unit: usize,
    values: ArrayValues,
    blocks: usize,
    failure: Option<ArrayFailure>,
}

impl Assembly {
    /// Put the block `block` of `data` in place, after checking it is the
    /// block asked for.
    fn place(&mut self, block: &Block, data: DataArray) {
        if self.failure.is_some() {
            return;
        }
        let dims: Vec<u64> = data.dimensions.iter().map(|d| *d as u64).collect();
        if dims != block.counts {
            self.failure = Some(ArrayFailure::Inconsistent(format!(
                "a block of {:?} was asked, one of {dims:?} came",
                block.counts
            )));
            return;
        }
        let values = ArrayValues::from_item(data.data.item);
        if values.transport() != self.transport {
            self.failure = Some(ArrayFailure::Inconsistent(format!(
                "the metadata says {:?}, the data is {:?}",
                self.transport,
                values.transport()
            )));
            return;
        }
        let expected = block.len() as usize * self.unit;
        if values.units() != expected {
            self.failure = Some(ArrayFailure::Inconsistent(format!(
                "a block of {:?} holds {} {}, {} came",
                block.counts,
                expected,
                if self.unit > 1 { "bytes" } else { "values" },
                values.units()
            )));
            return;
        }
        let whole = &mut self.values;
        for_each_run(&self.dimensions, block, self.unit, |at_whole, at_block, len| {
            whole.copy_run(at_whole, &values, at_block, len);
        });
    }
}

/// Each contiguous run a block shares with the whole array: where it sits in
/// the whole, where in the block, and how many units long.
///
/// A run along the **last** dimension is contiguous in both, which is the
/// whole trick. Written once because reading and writing need it in opposite
/// directions - [`Assembly::place`] copies a received block in, [`carve`]
/// copies a block out to send - and two walks of the same arithmetic is
/// exactly the shape this repository keeps finding bugs in: they agree on the
/// total and disagree on where a row goes.
fn for_each_run(dimensions: &[u64], block: &Block, unit: usize, mut run_at: impl FnMut(usize, usize, usize)) {
    let rank = dimensions.len();
    if rank == 0 || block.counts.len() != rank || block.starts.len() != rank {
        return;
    }
    let run = block.counts[rank - 1] as usize * unit;
    let rows = block.len() as usize / block.counts[rank - 1].max(1) as usize;
    let mut index = vec![0u64; rank.saturating_sub(1)];
    for row in 0..rows {
        let offset = (0..rank).fold(0u64, |offset, k| {
            let at = if k + 1 == rank {
                block.starts[k]
            } else {
                block.starts[k] + index[k]
            };
            offset * dimensions[k] + at
        });
        run_at(offset as usize * unit, row * run, run);
        // The next row of the block.
        for k in (0..rank.saturating_sub(1)).rev() {
            index[k] += 1;
            if index[k] < block.counts[k] {
                break;
            }
            index[k] = 0;
        }
    }
}

/// One block of a whole array, ready to send - the inverse of
/// [`Assembly::place`], over [`for_each_run`].
fn carve(whole: &ArrayValues, dimensions: &[u64], block: &Block, unit: usize) -> ArrayValues {
    let mut out = ArrayValues::empty(whole.transport(), block.len() as usize * unit);
    for_each_run(dimensions, block, unit, |at_whole, at_block, len| {
        out.copy_run(at_block, whole, at_whole, len);
    });
    out
}
