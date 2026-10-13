# Avro runtime (`retp-avro`) - architecture

Related decision: [ADR 0004](../../docs/adr/0004-generated-types-field-order-is-the-encoding.md) (generated types: the field order is the encoding).

## Purpose and position

Avro binary encoding for ETP: the runtime the generated ETP types stand on, as `energyml-core` is for the generated EnergyML crates. It has no schema knowledge: it knows how Avro puts a `long` on the wire, and does not know that ETP exists. It depends on `retp-derive` (the derives) and `serde_json` (for the Avro JSON encoding). It has no dependency on any EnergyML crate.

The encoding is Avro 1.11 binary, as ETP v1.2 §3.8 requires. The Avro **JSON** encoding (§3.8.1, for testing and debugging only) is the `json` module and the `to_json` / `from_json` methods of the `Avro` trait.

## Rules

- **No intermediate `Value` tree.** Decoding goes straight into the target type. A generic value would allocate once per element, and a `GetDataArrays` response is measured in millions of elements.
- **Refusals are named.** A truncated buffer, a union branch that does not exist and a string that is not UTF-8 are three different pieces of news, never one `None`.
- **The field order is the encoding.** Avro writes neither names nor tags, so the order of struct fields, enum variants and union branches is the wire format. Generated code follows schema order, and a hand-written type must too (ADR 0004).
- **`float_roundtrip` is not optional.** Without it the fast float parser can be one ulp off, and a `double` that does not come back as it went is a lost digit nobody sees.

## Tests

`tests/encoding.rs` states the encoding rules of Avro 1.11 and the named refusals; `tests/crossaudit.rs` confronts the encoder and decoder with an independent implementation (`fastavro`) on fixtures, so the encoding is not only checked against itself.
