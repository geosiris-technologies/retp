# energyml-etp-store

The bridge: an ETP v1.2 dataspace behind energyml-rust's `EnergymlStorage`, so that code which reads a file reads a server.

- Keeps the `energyml` prefix because it implements a trait of energyml-rust.
- The only crate of this workspace that knows both ETP and `EnergymlStorage`.

## Documentation

- API reference: <https://docs.rs/energyml-etp-store>
- Repository documentation: [`docs/`](../../docs/), [client](../../docs/client.md)

## Licence

Apache-2.0. See [`LICENSE`](../../LICENSE).
