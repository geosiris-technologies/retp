# 4. Generated types: the field order is the encoding

## Context

ETP v1.2 messages are Avro binary. Avro writes no field names: a record is its
fields' encodings one after the other, in the order the schema declares. A
decoder that reorders fields, or an encoder that skips one, silently produces
garbage on the wire. Writing around two hundred records by hand would be
error-prone, and a reflection-based codec costs time and size.

## Decision

- **The types are generated from the official Avro schemas of ETP 1.2**
  (Apache-2.0) and committed (`retp-types`).
  The generated files are never edited by hand.
- **The order of the fields in a generated struct is the order of the encoding.**
  It is the schema order, and generation preserves it.
- **The codec is generated with the type**, through derives (`retp-derive`), on the
  Avro runtime `retp-avro`:
  no field reflection at run time. Arrays of fixed-size items are coded by
  block, not item by item.
- **Message metadata** (protocol, message type) and a dispatch table are emitted
  with the types, so decoding a message is a lookup.
- **Unions are decoded strictly**, except that a neighbouring numeric width is
  accepted on read where servers are known to mix them.
- **The tests triangulate**: bytes written by `fastavro` from the same schemas
  are decoded here and re-encoded, and must come back byte for byte.

## Consequences

- Wire compatibility is checked against a second implementation that is not
  written here.
- Regenerating is a mechanical diff in a reviewable commit; a hand edit would be
  lost, hence the guard in CI.
- Changing the field order of a type is a breaking change of the wire format,
  not of the API only.
