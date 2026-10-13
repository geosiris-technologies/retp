# Derives (`retp-derive`) - architecture

Related decision: [ADR 0004](../../docs/adr/0004-generated-types-field-order-is-the-encoding.md) (generated types: the field order is the encoding).

## Purpose and position

The procedural macros behind the generated ETP types. The generator emits plain Rust with a line of attributes; these macros give each item its behaviour. The split is the one the XSD-generated crates make: **a derive for what a type knows about itself, the generator for what needs all types at once** (the dispatch table). They are re-exported by `retp-avro` under the names of their traits.

- `Avro` - the codec and a `Default` faithful to the schema. It reads a record, a fixed, an enumeration or a union (`#[avro(union)]`).
- `EtpMessage` - what a message says about itself on the wire (protocol number, message type, sender, roles).

## Rules

- **The field order is the encoding**, as the variant order is for an enumeration and a union: Avro writes neither names nor tags. A hand-written type must declare its fields in schema order.
- **Attributes carry what a type cannot say**: `name` (the Avro full name, used in refusals), `union`, `rename` (when the Avro field name is not the camelCase of the Rust one), `default` (spelled as the schema spells it), `symbol`, and a variant `default`. What the derive reads off a field's type, and what it refuses, is in `shape.rs`.
- **A type the derive cannot read is refused at compile time**, not coded around.
- **The manifest spells its licence out** instead of inheriting it: some build tools read manifests of path dependencies without resolving workspace inheritance.

## Tests

Exercised through the generated crates' test suite, which cross-audits encodings against an independent Avro implementation, and through `retp-avro`'s own tests.
