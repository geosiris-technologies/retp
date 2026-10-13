//! Avro binary encoding for ETP: the runtime the generated ETP types stand on.
//!
//! This crate is to `retp-types` what `energyml-core` is to the
//! generated EnergyML crates - support with no schema knowledge in it. It knows
//! how Avro puts a `long` on the wire; it does not know that ETP exists.
//!
//! Two properties are deliberate and both are tested:
//!
//! * **No `Value` tree.** Decoding goes straight into the target type. Going
//!   through a generic value would allocate once per element, and a
//!   `GetDataArrays` response is measured in millions of elements - see
//!   ADR-0004.
//! * **Refusals are named.** A truncated buffer, a union branch that does not
//!   exist and a string that is not UTF-8 are three different pieces of news,
//!   never one `None`.
//!
//! The encoding itself is Avro 1.11 binary, as ETP v1.2 §3.8 requires. The
//! Avro **JSON** encoding (§3.8.1: testing and debugging only) is the [`json`]
//! module and the `to_json` / `from_json` methods of [`Avro`].

#![forbid(unsafe_code)]

// The derives write `::retp_avro::…`, and this lets them do so from
// inside the crate too - its own tests use them.
extern crate self as retp_avro;

/// The derives, under the names of the traits they implement: `Avro` is both
/// the trait and its derive, as `Serialize` is for serde.
pub use retp_derive::{Avro, EtpMessage};

pub mod json;

use std::collections::BTreeMap;
use std::fmt;

/// What can go wrong reading Avro, named rather than counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The buffer ended in the middle of a value.
    Truncated {
        /// What the reader was trying to build.
        reading: &'static str,
        /// How many more bytes it needed.
        needed: usize,
        /// How many were left.
        available: usize,
    },
    /// A variable-length integer ran past the ten bytes a 64-bit value can use.
    VarintTooLong,
    /// A length or block count that cannot be a length.
    BadLength(i64),
    /// A union branch index outside the branches the schema declares.
    NoSuchBranch {
        /// The type whose union it is.
        in_type: &'static str,
        /// The index that arrived.
        index: i64,
        /// How many branches the schema has.
        branches: usize,
    },
    /// An enum ordinal outside the symbols the schema declares.
    NoSuchSymbol {
        /// The enumeration.
        in_type: &'static str,
        /// The ordinal that arrived.
        index: i64,
        /// How many symbols the schema has.
        symbols: usize,
    },
    /// A `string` whose bytes are not UTF-8.
    NotUtf8,
    /// Bytes were left over after a complete value was read.
    TrailingBytes(usize),
    /// A `(protocol, messageType)` pair that ETP v1.2 does not define.
    ///
    /// A peer sending one is news - it is speaking something we did not
    /// negotiate - so it is a refusal and never a silently skipped message.
    NoSuchMessage {
        /// The sub-protocol from the message header.
        protocol: i32,
        /// The message type from the message header.
        message_type: i32,
    },
    /// The text is not JSON at all.
    JsonSyntax(String),
    /// A JSON value of the wrong kind where the schema wants another.
    JsonType {
        /// The record field, union or type it was read for.
        at: &'static str,
        /// What the schema wants.
        expected: &'static str,
        /// What arrived.
        found: &'static str,
    },
    /// A JSON value of the right kind that cannot be this Avro value (an `int`
    /// out of range, a `bytes` code point above 255, a `fixed` of the wrong size).
    JsonBadValue {
        /// The record field, union or type it was read for.
        at: &'static str,
        /// What is wrong with it.
        why: String,
    },
    /// A record field that is absent and has no default in the schema.
    JsonMissingField {
        /// The record.
        in_type: &'static str,
        /// The field, as the schema spells it.
        field: &'static str,
    },
    /// A key in a record's object that is not one of its fields.
    JsonUnknownField {
        /// The record.
        in_type: &'static str,
        /// The key that arrived.
        field: String,
    },
    /// An enum symbol the schema does not declare.
    JsonNoSuchSymbol {
        /// The enumeration.
        in_type: &'static str,
        /// The symbol that arrived.
        symbol: String,
    },
    /// A union value that is neither `null` nor a one-entry object naming a
    /// branch the union declares.
    JsonBadUnion {
        /// The union.
        in_type: &'static str,
        /// What arrived instead.
        found: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Truncated {
                reading,
                needed,
                available,
            } => write!(
                f,
                "truncated while reading {reading}: needed {needed} bytes, {available} left"
            ),
            Error::VarintTooLong => write!(f, "variable-length integer longer than 10 bytes"),
            Error::BadLength(n) => write!(f, "{n} is not a usable length"),
            Error::NoSuchBranch {
                in_type,
                index,
                branches,
            } => write!(f, "union branch {index} in {in_type}, which declares {branches}"),
            Error::NoSuchSymbol {
                in_type,
                index,
                symbols,
            } => write!(
                f,
                "enum ordinal {index} in {in_type}, which declares {symbols} symbols"
            ),
            Error::NotUtf8 => write!(f, "string is not valid UTF-8"),
            Error::NoSuchMessage {
                protocol,
                message_type,
            } => write!(
                f,
                "no ETP message is protocol {protocol}, message type {message_type}"
            ),
            Error::TrailingBytes(n) => write!(f, "{n} bytes left after the value"),
            Error::JsonSyntax(why) => write!(f, "not JSON: {why}"),
            Error::JsonType { at, expected, found } => {
                write!(f, "JSON at {at}: expected {expected}, found {found}")
            }
            Error::JsonBadValue { at, why } => write!(f, "JSON at {at}: {why}"),
            Error::JsonMissingField { in_type, field } => {
                write!(
                    f,
                    "JSON for {in_type}: field `{field}` is absent and the schema gives it no default"
                )
            }
            Error::JsonUnknownField { in_type, field } => {
                write!(f, "JSON for {in_type}: `{field}` is not one of its fields")
            }
            Error::JsonNoSuchSymbol { in_type, symbol } => {
                write!(f, "JSON for {in_type}: {symbol:?} is not one of its symbols")
            }
            Error::JsonBadUnion { in_type, found } => {
                write!(f, "JSON for union {in_type}: found {found}")
            }
        }
    }
}

impl std::error::Error for Error {}

/// The result of reading.
pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Appends Avro binary to a byte buffer.
#[derive(Debug, Default)]
pub struct Writer {
    out: Vec<u8>,
}

impl Writer {
    /// A writer over a fresh buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// A writer over a buffer with room already reserved.
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            out: Vec::with_capacity(bytes),
        }
    }

    /// The bytes written so far.
    pub fn as_slice(&self) -> &[u8] {
        &self.out
    }

    /// Take the buffer.
    pub fn finish(self) -> Vec<u8> {
        self.out
    }

    /// How many bytes have been written.
    pub fn len(&self) -> usize {
        self.out.len()
    }

    /// Whether nothing has been written.
    pub fn is_empty(&self) -> bool {
        self.out.is_empty()
    }

    /// `null` - zero bytes, by definition.
    pub fn null(&mut self) {}

    /// `boolean`.
    pub fn boolean(&mut self, value: bool) {
        self.out.push(u8::from(value));
    }

    /// `int`, zigzag then variable-length.
    pub fn int(&mut self, value: i32) {
        self.varint(zigzag_encode(i64::from(value)));
    }

    /// `long`, zigzag then variable-length.
    pub fn long(&mut self, value: i64) {
        self.varint(zigzag_encode(value));
    }

    /// `float` - four bytes, little-endian.
    pub fn float(&mut self, value: f32) {
        self.out.extend_from_slice(&value.to_le_bytes());
    }

    /// `double` - eight bytes, little-endian.
    pub fn double(&mut self, value: f64) {
        self.out.extend_from_slice(&value.to_le_bytes());
    }

    /// `bytes` - a length then the bytes.
    pub fn bytes(&mut self, value: &[u8]) {
        self.long(value.len() as i64);
        self.out.extend_from_slice(value);
    }

    /// `string` - a length then UTF-8.
    pub fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    /// `fixed` - the bytes, with no length, because the schema knows it.
    pub fn fixed(&mut self, value: &[u8]) {
        self.out.extend_from_slice(value);
    }

    /// An enum symbol, by its ordinal.
    pub fn enum_index(&mut self, index: usize) {
        self.int(index as i32);
    }

    /// A union branch index. The value itself follows.
    pub fn union_index(&mut self, index: usize) {
        self.long(index as i64);
    }

    /// A union of `null` and one other type.
    ///
    /// `null_at` is the branch index the schema gives `null` - read from the
    /// schema rather than assumed, because "null comes first" is a habit of
    /// these particular files and not a rule of Avro.
    pub fn optional<T, F>(&mut self, value: &Option<T>, null_at: usize, mut item: F)
    where
        F: FnMut(&mut Self, &T),
    {
        match value {
            None => self.union_index(null_at),
            Some(inner) => {
                self.union_index(1 - null_at);
                item(self, inner);
            }
        }
    }

    /// An array, written as one block: a count, the items, then a zero.
    ///
    /// One block rather than several is a choice, and the reader accepts any
    /// blocking a peer sends. ETP has no streaming array case where a second
    /// block would buy anything.
    pub fn array<T, F>(&mut self, items: &[T], mut item: F)
    where
        F: FnMut(&mut Self, &T),
    {
        if !items.is_empty() {
            self.long(items.len() as i64);
            for value in items {
                item(self, value);
            }
        }
        self.long(0);
    }

    /// A map, written as one block. Keys go out in the order given.
    pub fn map<V, F>(&mut self, entries: &BTreeMap<String, V>, mut value: F)
    where
        F: FnMut(&mut Self, &V),
    {
        if !entries.is_empty() {
            self.long(entries.len() as i64);
            for (key, item) in entries {
                self.string(key);
                value(self, item);
            }
        }
        self.long(0);
    }

    /// An `array<float>`, written as one block of contiguous little-endian
    /// bytes.
    ///
    /// See [`Reader::array_f32`] for what this pairing buys.
    pub fn array_f32(&mut self, values: &[f32]) {
        if !values.is_empty() {
            self.long(values.len() as i64);
            self.out.reserve(values.len() * 4);
            for value in values {
                self.out.extend_from_slice(&value.to_le_bytes());
            }
        }
        self.long(0);
    }

    /// An `array<double>`, written as one block of contiguous little-endian
    /// bytes.
    pub fn array_f64(&mut self, values: &[f64]) {
        if !values.is_empty() {
            self.long(values.len() as i64);
            self.out.reserve(values.len() * 8);
            for value in values {
                self.out.extend_from_slice(&value.to_le_bytes());
            }
        }
        self.long(0);
    }

    fn varint(&mut self, mut value: u64) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                self.out.push(byte);
                return;
            }
            self.out.push(byte | 0x80);
        }
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Reads Avro binary out of a byte slice.
#[derive(Debug)]
pub struct Reader<'a> {
    input: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    /// A reader positioned at the start of `input`.
    pub fn new(input: &'a [u8]) -> Self {
        Self { input, at: 0 }
    }

    /// How many bytes are left.
    pub fn remaining(&self) -> usize {
        self.input.len() - self.at
    }

    /// Whether every byte has been consumed.
    pub fn is_done(&self) -> bool {
        self.at == self.input.len()
    }

    /// Fail unless every byte has been consumed.
    ///
    /// Worth calling at the end of a message: trailing bytes mean the sender
    /// and this reader disagree about the schema, which is news.
    pub fn expect_done(&self) -> Result<()> {
        match self.remaining() {
            0 => Ok(()),
            n => Err(Error::TrailingBytes(n)),
        }
    }

    /// `null` - zero bytes, by definition.
    pub fn null(&mut self) {}

    /// `boolean`. Any non-zero byte is true, as the specification allows.
    pub fn boolean(&mut self) -> Result<bool> {
        Ok(self.take(1, "boolean")?[0] != 0)
    }

    /// `int`.
    pub fn int(&mut self) -> Result<i32> {
        Ok(self.long()? as i32)
    }

    /// `long`.
    pub fn long(&mut self) -> Result<i64> {
        Ok(zigzag_decode(self.varint()?))
    }

    /// `float`.
    pub fn float(&mut self) -> Result<f32> {
        let bytes = self.take(4, "float")?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// `double`.
    pub fn double(&mut self) -> Result<f64> {
        let bytes = self.take(8, "double")?;
        let mut eight = [0u8; 8];
        eight.copy_from_slice(bytes);
        Ok(f64::from_le_bytes(eight))
    }

    /// `bytes`.
    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        Ok(self.bytes_slice()?.to_vec())
    }

    /// `bytes`, borrowed from the input rather than copied.
    pub fn bytes_slice(&mut self) -> Result<&'a [u8]> {
        let len = self.length()?;
        self.take(len, "bytes")
    }

    /// `string`.
    pub fn string(&mut self) -> Result<String> {
        let bytes = self.bytes_slice()?;
        String::from_utf8(bytes.to_vec()).map_err(|_| Error::NotUtf8)
    }

    /// `fixed` of `N` bytes.
    pub fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self.take(N, "fixed")?;
        let mut out = [0u8; N];
        out.copy_from_slice(bytes);
        Ok(out)
    }

    /// An enum ordinal, checked against the symbol count.
    pub fn enum_index(&mut self, in_type: &'static str, symbols: usize) -> Result<usize> {
        let index = self.long()?;
        if index < 0 || index as usize >= symbols {
            return Err(Error::NoSuchSymbol {
                in_type,
                index,
                symbols,
            });
        }
        Ok(index as usize)
    }

    /// A union branch index, checked against the branch count.
    pub fn union_index(&mut self, in_type: &'static str, branches: usize) -> Result<usize> {
        let index = self.long()?;
        if index < 0 || index as usize >= branches {
            return Err(Error::NoSuchBranch {
                in_type,
                index,
                branches,
            });
        }
        Ok(index as usize)
    }

    /// A union of `null` and one other type.
    ///
    /// The pair of [`Writer::optional`]. `null_at` comes from the schema, and
    /// any other branch index is the value - the union has two branches, and
    /// [`Reader::union_index`] has already refused anything outside them.
    pub fn optional<T, F>(&mut self, in_type: &'static str, null_at: usize, mut item: F) -> Result<Option<T>>
    where
        F: FnMut(&mut Self) -> Result<T>,
    {
        if self.union_index(in_type, 2)? == null_at {
            return Ok(None);
        }
        item(self).map(Some)
    }

    /// An array, however the sender chose to block it.
    pub fn array<T, F>(&mut self, mut item: F) -> Result<Vec<T>>
    where
        F: FnMut(&mut Self) -> Result<T>,
    {
        let mut out = Vec::new();
        loop {
            let count = match self.block_count()? {
                None => return Ok(out),
                Some(count) => count,
            };
            out.reserve(count);
            for _ in 0..count {
                out.push(item(self)?);
            }
        }
    }

    /// A map, however the sender chose to block it.
    pub fn map<V, F>(&mut self, mut value: F) -> Result<BTreeMap<String, V>>
    where
        F: FnMut(&mut Self) -> Result<V>,
    {
        let mut out = BTreeMap::new();
        loop {
            let count = match self.block_count()? {
                None => return Ok(out),
                Some(count) => count,
            };
            for _ in 0..count {
                let key = self.string()?;
                out.insert(key, value(self)?);
            }
        }
    }

    /// An `array<float>`, read a whole block at a time.
    ///
    /// The generic [`Reader::array`] already decodes straight into the target
    /// `Vec` - there is no per-element allocation to save. What this saves is
    /// the per-element bounds check and length decode, on the one path where
    /// element counts run into the millions (`GetDataArrays`). The gain is
    /// small per element and it is the only place where that matters; it is to
    /// be measured, not assumed.
    pub fn array_f32(&mut self) -> Result<Vec<f32>> {
        self.array_fixed_width::<f32, 4>(|bytes| f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// An `array<double>`, read a whole block at a time.
    pub fn array_f64(&mut self) -> Result<Vec<f64>> {
        self.array_fixed_width::<f64, 8>(|bytes| {
            let mut eight = [0u8; 8];
            eight.copy_from_slice(bytes);
            f64::from_le_bytes(eight)
        })
    }

    fn array_fixed_width<T, const WIDTH: usize>(&mut self, from_le: fn(&[u8]) -> T) -> Result<Vec<T>> {
        let mut out = Vec::new();
        loop {
            let count = match self.block_count()? {
                None => return Ok(out),
                Some(count) => count,
            };
            let wanted = count.checked_mul(WIDTH).ok_or(Error::BadLength(count as i64))?;
            let block = self.take(wanted, "array block")?;
            out.reserve(count);
            out.extend(block.as_chunks::<WIDTH>().0.iter().map(|chunk| from_le(chunk)));
        }
    }

    /// The start of the next block: `None` at the terminating zero.
    ///
    /// A negative count means the sender also wrote the block's byte size, so
    /// a reader that wanted to skip the block could. This one reads it, so the
    /// size is consumed and discarded.
    fn block_count(&mut self) -> Result<Option<usize>> {
        let count = self.long()?;
        if count == 0 {
            return Ok(None);
        }
        if count < 0 {
            let count = count.checked_neg().ok_or(Error::BadLength(count))?;
            let _size = self.long()?;
            return Ok(Some(usize::try_from(count).map_err(|_| Error::BadLength(count))?));
        }
        Ok(Some(usize::try_from(count).map_err(|_| Error::BadLength(count))?))
    }

    fn length(&mut self) -> Result<usize> {
        let len = self.long()?;
        if len < 0 {
            return Err(Error::BadLength(len));
        }
        usize::try_from(len).map_err(|_| Error::BadLength(len))
    }

    fn take(&mut self, n: usize, reading: &'static str) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(Error::Truncated {
                reading,
                needed: n,
                available: self.remaining(),
            });
        }
        let out = &self.input[self.at..self.at + n];
        self.at += n;
        Ok(out)
    }

    fn varint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            if shift >= 70 {
                return Err(Error::VarintTooLong);
            }
            let byte = self.take(1, "variable-length integer")?[0];
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
    }
}

// ---------------------------------------------------------------------------
// Zigzag
// ---------------------------------------------------------------------------

/// Map a signed integer onto an unsigned one so small magnitudes stay short.
pub fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// The inverse of [`zigzag_encode`].
pub fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

// ---------------------------------------------------------------------------
// The trait the generated types implement
// ---------------------------------------------------------------------------

/// A type that knows its own Avro encoding.
///
/// Every generated ETP type implements this. Hand-written types may too, which
/// is what lets a test build a message before the generator can.
pub trait Avro: Sized {
    /// The Avro full name, used in refusals so an error names a type.
    const FULL_NAME: &'static str;

    /// Append this value.
    fn write(&self, out: &mut Writer);

    /// Read one value.
    fn read(input: &mut Reader<'_>) -> Result<Self>;

    /// Encode to a fresh buffer.
    fn to_avro(&self) -> Vec<u8> {
        let mut out = Writer::new();
        self.write(&mut out);
        out.finish()
    }

    /// The Avro JSON encoding of this value (ETP v1.2 §3.8.1: testing and
    /// debugging only). See the [`json`] module for the conventions.
    fn to_json(&self) -> json::Value;

    /// Read a value from its Avro JSON encoding.
    ///
    /// A field that is missing takes the schema's default when it has one and is
    /// a named refusal otherwise; a key the schema does not know is refused.
    fn from_json(value: &json::Value) -> Result<Self>;

    /// [`Avro::to_json`] as text.
    fn to_json_string(&self) -> String {
        self.to_json().to_string()
    }

    /// [`Avro::from_json`] from text.
    fn from_json_str(text: &str) -> Result<Self> {
        let value: json::Value =
            serde_json::from_str(text).map_err(|why| Error::JsonSyntax(why.to_string()))?;
        Self::from_json(&value)
    }

    /// Decode a complete buffer, refusing leftovers.
    fn from_avro(input: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(input);
        let value = Self::read(&mut reader)?;
        reader.expect_done()?;
        Ok(value)
    }
}

// ---------------------------------------------------------------------------
// What a message knows about itself
// ---------------------------------------------------------------------------

/// One of the six roles ETP defines.
///
/// `client` and `server` say who opened the WebSocket; the other four are the
/// roles a sub-protocol assigns, and they are **independent** of it - a client
/// may perfectly well be the `store` (ETP v1.2 §3.1.2, p.33-34). Nothing here
/// may assume which side of the wire it is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// Opened the WebSocket connection.
    Client,
    /// Accepted the WebSocket connection.
    Server,
    /// Reads from or writes to a store.
    Customer,
    /// Holds the data objects.
    Store,
    /// Makes new data available over time.
    Producer,
    /// Consumes what a producer makes available.
    Consumer,
}

impl Role {
    /// The role's name, exactly as the schemas spell it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Client => "client",
            Role::Server => "server",
            Role::Customer => "customer",
            Role::Store => "store",
            Role::Producer => "producer",
            Role::Consumer => "consumer",
        }
    }
}

/// Which roles may send a message.
///
/// Two ETP messages - `ProtocolException` and `Acknowledge`, the specification's
/// "universal" messages (§3.7.2) - declare `*` rather than a list. That is a
/// different fact from *every role*, and flattening the two would lose the only
/// thing that makes those two messages special.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Senders {
    /// Any role, in any sub-protocol.
    Any,
    /// Exactly these roles.
    These(&'static [Role]),
}

impl Senders {
    /// Whether `role` may send the message.
    pub fn allows(&self, role: Role) -> bool {
        match self {
            Senders::Any => true,
            Senders::These(roles) => roles.contains(&role),
        }
    }
}

/// What an ETP message knows about itself, from its own schema.
///
/// Every fact here is read out of the `.avsc` - none is declared by hand, which
/// is why a table of them cannot drift. What the schemas do *not* carry, and
/// which therefore is not here, is the request-to-response pairing (ADR-0001).
pub trait EtpMessage: Avro {
    /// The ETP sub-protocol number.
    const PROTOCOL: i32;
    /// The message type within that sub-protocol.
    const MESSAGE_TYPE: i32;
    /// Which roles may send this message.
    const SENDER_ROLE: Senders;
    /// The roles the sub-protocol defines.
    const PROTOCOL_ROLES: Senders;
    /// Whether the message may be split across several messages.
    ///
    /// A message whose schema says `false` **must not** be split, however
    /// convenient it would be (§3.5.5, p.58-59).
    const MULTIPART: bool;
}

/// What an ETP message says about itself, as a value rather than as constants.
///
/// [`EtpMessage`] answers at compile time for a type in hand; a receiver holding
/// only a header needs the same facts at run time, from a table the generator
/// emits (`retp_types::dispatch::info`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageInfo {
    /// The Avro full name.
    pub name: &'static str,
    /// The ETP sub-protocol number.
    pub protocol: i32,
    /// The message type within that sub-protocol.
    pub message_type: i32,
    /// Which roles may send it.
    pub sender_role: Senders,
    /// The roles its sub-protocol defines.
    pub protocol_roles: Senders,
    /// Whether it may be split across several messages.
    pub multipart: bool,
}

impl MessageInfo {
    /// The facts of `T`, read from its constants.
    pub const fn of<T: EtpMessage>() -> Self {
        Self {
            name: T::FULL_NAME,
            protocol: T::PROTOCOL,
            message_type: T::MESSAGE_TYPE,
            sender_role: T::SENDER_ROLE,
            protocol_roles: T::PROTOCOL_ROLES,
            multipart: T::MULTIPART,
        }
    }
}
