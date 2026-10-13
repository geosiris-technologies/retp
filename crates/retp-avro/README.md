# retp-avro

Avro binary encoding for ETP v1.2: the runtime the generated types of `retp-types` stand on, as `energyml-core` is for the XSD ones.

- Encoding and decoding of the Avro primitives, unions, arrays and maps ETP uses.
- No knowledge of any ETP message: that is `retp-types`, generated from the schemas.
- Pure Rust, compiles to `wasm32-unknown-unknown`.

## Documentation

- API reference: <https://docs.rs/retp-avro>
- Repository documentation: [`docs/`](../../docs/)

## Licence

Apache-2.0. See [`LICENSE`](../../LICENSE).
