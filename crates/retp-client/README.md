# retp-client

An ETP v1.2 client: a transport under `retp-proto`'s connection, and what a client does with it (open a session, list dataspaces and resources, read and write objects and arrays).

- The transport is a trait; the native WebSocket (rustls over `ring`) is one implementation, and a fake one drives the tests.
- Compiles to wasm without its native transport.

## Documentation

- API reference: <https://docs.rs/retp-client>
- Repository documentation: [`docs/`](../../docs/), [client](../../docs/client.md)

## Licence

Apache-2.0. See [`LICENSE`](../../LICENSE).
