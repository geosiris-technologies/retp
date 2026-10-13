//! The Avro JSON encoding, the helpers the derive stands on.
//!
//! ETP v1.2 §3.8.1 (p.76): *"ETP also supports the use of both [binary and
//! JSON encoding], with the following caveats: … JSON encoding of ETP messages
//! is for internal testing and debugging only; JSON encoding MUST NOT be used
//! for ETP production implementations."* The caveats name the two hazards this
//! module has to settle: *"JSON has significant limitations with encoding some
//! data (for example, NaN in doubles)"* and *"Avro has certain conventions on
//! how to construct JSON messages; however, these conventions are not commonly
//! supported by Avro libraries"*. The conventions are those of the Avro 1.x
//! specification, "JSON Encoding", followed to the letter:
//!
//! * `null`, `boolean`, `int`, `long`, `float`, `double` and `string` are the
//!   JSON `null`, boolean, number and string;
//! * `bytes` and `fixed` are a JSON string whose code points 0 to 255 are the
//!   byte values ([`of_bytes`], [`as_bytes`]);
//! * an `enum` is its symbol, a string;
//! * an `array` is a JSON array and a `map` a JSON object;
//! * a `record` is a JSON object keyed by field name;
//! * a **union** is the JSON `null` for the `null` branch, and otherwise a
//!   one-entry object `{"<branch>": value}`, where `<branch>` is the primitive
//!   type name (`"string"`, `"long"`, `"array"`, `"map"`, …) or the **full
//!   name** of the named type ([`tagged`], [`union_branch`]).
//!
//! Three decisions the specification leaves open, each a refusal rather than a
//! guess:
//!
//! * **Non-finite numbers.** JSON has no `NaN`. They go out as the strings
//!   `"NaN"`, `"Infinity"` and `"-Infinity"` (the spelling the Avro
//!   specification uses for default values) and are read back from them - the
//!   alternative, a bare `null`, would turn a measurement into an absence.
//! * **A field the reader does not know** is refused by name
//!   ([`Error::JsonUnknownField`]); Avro schema resolution would drop it, and
//!   dropping data silently is what this repository does not do.
//! * **A missing field** takes the schema's default when the field has one, and
//!   is [`Error::JsonMissingField`] otherwise - never a silent zero.
//!
//! Object key order is not significant in JSON, and `serde_json::Map` keeps
//! its keys sorted; a record's fields are therefore not in schema order *in the
//! text*. No Avro JSON reader depends on that order.

pub use serde_json::{Map, Number, Value};

use std::collections::BTreeMap;

use crate::{Error, Result};

/// What kind of JSON value this is, for a refusal to name.
pub fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn wrong(at: &'static str, expected: &'static str, value: &Value) -> Error {
    Error::JsonType {
        at,
        expected,
        found: kind_of(value),
    }
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

/// A `float`, in the shortest decimal that reads back as the same `f32`.
///
/// Widening to `f64` first would write `0.10000000149011612` for `0.1f32`.
pub fn of_f32(value: f32) -> Value {
    if !value.is_finite() {
        return non_finite(f64::from(value));
    }
    // `Display` for `f32` is the shortest round-tripping decimal.
    match value.to_string().parse::<f64>().ok().and_then(Number::from_f64) {
        Some(n) => Value::Number(n),
        None => non_finite(f64::from(value)),
    }
}

/// A `double`.
pub fn of_f64(value: f64) -> Value {
    match Number::from_f64(value) {
        Some(n) => Value::Number(n),
        None => non_finite(value),
    }
}

fn non_finite(value: f64) -> Value {
    Value::String(
        if value.is_nan() {
            "NaN"
        } else if value > 0.0 {
            "Infinity"
        } else {
            "-Infinity"
        }
        .to_string(),
    )
}

/// `bytes`: a string whose code points are the byte values.
pub fn of_bytes(bytes: &[u8]) -> Value {
    Value::String(bytes.iter().map(|&b| char::from(b)).collect())
}

/// `fixed`: the same string as `bytes`; the schema knows the length.
pub fn of_fixed(bytes: &[u8]) -> Value {
    of_bytes(bytes)
}

/// A union branch that carries a value: `{"<branch>": value}`.
pub fn tagged(branch: &str, value: Value) -> Value {
    let mut entry = Map::new();
    entry.insert(branch.to_string(), value);
    Value::Object(entry)
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// `null`.
pub fn as_null(value: &Value, at: &'static str) -> Result<()> {
    match value {
        Value::Null => Ok(()),
        other => Err(wrong(at, "null", other)),
    }
}

/// `boolean`.
pub fn as_bool(value: &Value, at: &'static str) -> Result<bool> {
    match value {
        Value::Bool(b) => Ok(*b),
        other => Err(wrong(at, "boolean", other)),
    }
}

/// `long`. A number with a fraction is not one, even when it is `3.0`.
pub fn as_i64(value: &Value, at: &'static str) -> Result<i64> {
    match value {
        Value::Number(n) => n.as_i64().ok_or_else(|| {
            if n.is_u64() {
                Error::JsonBadValue {
                    at,
                    why: format!("{n} does not fit an Avro `long`"),
                }
            } else {
                wrong(at, "integer", value)
            }
        }),
        other => Err(wrong(at, "integer", other)),
    }
}

/// `int`.
pub fn as_i32(value: &Value, at: &'static str) -> Result<i32> {
    let long = as_i64(value, at)?;
    i32::try_from(long).map_err(|_| Error::JsonBadValue {
        at,
        why: format!("{long} does not fit an Avro `int`"),
    })
}

/// `double`, from a number or from one of the three non-finite spellings.
pub fn as_f64(value: &Value, at: &'static str) -> Result<f64> {
    match value {
        Value::Number(n) => n.as_f64().ok_or_else(|| Error::JsonBadValue {
            at,
            why: format!("{n} is not representable as a double"),
        }),
        Value::String(text) => match text.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            other => Err(Error::JsonBadValue {
                at,
                why: format!("the string {other:?} is not a number (only NaN, Infinity and -Infinity are)"),
            }),
        },
        other => Err(wrong(at, "number", other)),
    }
}

/// `float`.
pub fn as_f32(value: &Value, at: &'static str) -> Result<f32> {
    as_f64(value, at).map(|v| v as f32)
}

/// `string`.
pub fn as_string(value: &Value, at: &'static str) -> Result<String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        other => Err(wrong(at, "string", other)),
    }
}

/// `bytes`: every code point must be a byte value.
pub fn as_bytes(value: &Value, at: &'static str) -> Result<Vec<u8>> {
    let Value::String(text) = value else {
        return Err(wrong(at, "string", value));
    };
    text.chars()
        .map(|c| {
            u8::try_from(u32::from(c)).map_err(|_| Error::JsonBadValue {
                at,
                why: format!(
                    "U+{:04X} is not a byte: bytes are code points 0 to 255",
                    u32::from(c)
                ),
            })
        })
        .collect()
}

/// `fixed` of `N` bytes.
pub fn as_fixed<const N: usize>(value: &Value, at: &'static str) -> Result<[u8; N]> {
    let bytes = as_bytes(value, at)?;
    <[u8; N]>::try_from(bytes.as_slice()).map_err(|_| Error::JsonBadValue {
        at,
        why: format!("a fixed of {N} bytes was given {}", bytes.len()),
    })
}

/// `array`.
pub fn as_array<T, F>(value: &Value, at: &'static str, mut item: F) -> Result<Vec<T>>
where
    F: FnMut(&Value) -> Result<T>,
{
    match value {
        Value::Array(items) => items.iter().map(&mut item).collect(),
        other => Err(wrong(at, "array", other)),
    }
}

/// `map`.
pub fn as_map<T, F>(value: &Value, at: &'static str, mut item: F) -> Result<BTreeMap<String, T>>
where
    F: FnMut(&Value) -> Result<T>,
{
    match value {
        Value::Object(entries) => entries
            .iter()
            .map(|(key, v)| Ok((key.clone(), item(v)?)))
            .collect(),
        other => Err(wrong(at, "object", other)),
    }
}

/// A record's object.
pub fn as_object<'a>(value: &'a Value, in_type: &'static str) -> Result<&'a Map<String, Value>> {
    match value {
        Value::Object(entries) => Ok(entries),
        other => Err(wrong(in_type, "object", other)),
    }
}

/// Refuse any key of a record's object that the schema does not declare.
pub fn check_fields(entries: &Map<String, Value>, in_type: &'static str, known: &[&str]) -> Result<()> {
    match entries.keys().find(|key| !known.contains(&key.as_str())) {
        Some(key) => Err(Error::JsonUnknownField {
            in_type,
            field: key.clone(),
        }),
        None => Ok(()),
    }
}

/// A required field that is absent.
pub fn missing(in_type: &'static str, field: &'static str) -> Error {
    Error::JsonMissingField { in_type, field }
}

/// An enum symbol, as the string it arrived in.
pub fn as_symbol<'a>(value: &'a Value, in_type: &'static str) -> Result<&'a str> {
    match value {
        Value::String(symbol) => Ok(symbol),
        other => Err(wrong(in_type, "string", other)),
    }
}

/// A symbol the enumeration does not declare.
pub fn unknown_symbol(in_type: &'static str, symbol: &str) -> Error {
    Error::JsonNoSuchSymbol {
        in_type,
        symbol: symbol.to_string(),
    }
}

/// A union value: `None` for the JSON `null`, otherwise the branch name and
/// the value it holds. Anything else is not a union value.
pub fn union_branch<'a>(value: &'a Value, in_type: &'static str) -> Result<Option<(&'a str, &'a Value)>> {
    match value {
        Value::Null => Ok(None),
        Value::Object(entries) if entries.len() == 1 => {
            let (branch, inner) = entries.iter().next().expect("one entry");
            Ok(Some((branch.as_str(), inner)))
        }
        Value::Object(entries) => Err(Error::JsonBadUnion {
            in_type,
            found: format!("an object with {} entries instead of one", entries.len()),
        }),
        other => Err(Error::JsonBadUnion {
            in_type,
            found: format!("a {} instead of null or a one-entry object", kind_of(other)),
        }),
    }
}

/// A branch name the union does not declare.
pub fn unknown_branch(in_type: &'static str, branch: &str) -> Error {
    Error::JsonBadUnion {
        in_type,
        found: format!("the branch {branch:?}, which the union does not declare"),
    }
}

/// A union of `null` and one other type: the JSON `null`, or `{branch: value}`.
pub fn optional<T, F>(value: &Value, in_type: &'static str, branch: &str, mut item: F) -> Result<Option<T>>
where
    F: FnMut(&Value) -> Result<T>,
{
    match union_branch(value, in_type)? {
        None => Ok(None),
        Some((name, inner)) if name == branch => item(inner).map(Some),
        Some((name, _)) => Err(Error::JsonBadUnion {
            in_type,
            found: format!("the branch {name:?}, where this union holds null or {branch:?}"),
        }),
    }
}
