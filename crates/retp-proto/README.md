# retp-proto

The ETP v1.2 protocol for a client and a server alike: the wire (message header, body, multipart answers) and the session. No transport: bytes in, bytes out.

- Both roles share one implementation of the wire.
- Where the specification names no error code, the one used is chosen once and justified next to the code.
- Pure Rust, compiles to `wasm32-unknown-unknown`.

## Documentation

- API reference: <https://docs.rs/retp-proto>
- Repository documentation: [`docs/`](../../docs/), [client](../../docs/client.md)

## Licence

Apache-2.0. See [`LICENSE`](../../LICENSE).
