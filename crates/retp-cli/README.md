# retp-cli

The `retp` command: try an ETP v1.2 server - probe it, open a session, list, read, download a dataspace as an EPC package.

- Feature `hdf5` (off by default): write the arrays of a downloaded dataspace to HDF5. Without it, `download-epc` refuses to write arrays and says so.
- Feature `mcp` (off by default): `retp mcp`, one dataspace served as read-only MCP tools over stdio, for an AI assistant. The release archives are built with it.

## Documentation

- API reference: <https://docs.rs/retp-cli>
- Repository documentation: [`docs/`](../../docs/), [cli](../../docs/cli.md), [mcp](../../docs/mcp.md)

## Licence

Apache-2.0. See [`LICENSE`](../../LICENSE).
