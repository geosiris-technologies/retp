# retp-derive

The derives behind the generated ETP types of `retp-types`: an Avro codec, a `Default` faithful to the schema, and the message metadata (protocol, message type, multipart).

- Used by generated code; a hand-written type rarely needs them.
- The field order of a record **is** its encoding: the derives never reorder.

## Documentation

- API reference: <https://docs.rs/retp-derive>
- Repository documentation: [`docs/`](../../docs/)

## Licence

Apache-2.0. See [`LICENSE`](../../LICENSE).
