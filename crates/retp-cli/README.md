# retp-cli

The `retp` command: try an ETP v1.2 server - probe it, open a session, list, read, download a dataspace as an EPC package.

- Feature `hdf5` (off by default): write the arrays of a downloaded dataspace to HDF5. Without it, `download-epc` refuses to write arrays and says so.

## Documentation

- API reference: <https://docs.rs/retp-cli>
- Repository documentation: [`docs/`](../../docs/), [cli](../../docs/cli.md)

## Licence

Apache-2.0. See [`LICENSE`](../../LICENSE).
