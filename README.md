# retp

**An Energistics Transfer Protocol (ETP) v1.2 stack in Rust.**

ETP is the WebSocket protocol the energy industry uses to exchange subsurface
data objects (RESQML, WITSML, PRODML), data arrays and streams with a server,
including OSDU's Reservoir DDMS. `retp` is its Rust implementation: encode and
decode, hold a session, talk to a server, and check one.

> **Status: early.** Version 0.1 is on crates.io; the API will still move. This
> repository replaces the proof of concept `etp-rs`. The energyml crates it
> builds on come from crates.io, like any other dependency.

## Crates

> The bridge keeps the `energyml` prefix because it implements a
> trait of energyml-rust.

| Crate | What it is |
|---|---|
| `retp-avro` | Avro binary encoding for ETP: the runtime the generated types stand on |
| `retp-derive` | the derives behind the generated types: codec, `Default`, message metadata |
| `retp-types` | the ETP v1.2 messages and data types, generated from the Avro schemas (the field order **is** the encoding) |
| `retp-proto` | the wire and the session, for a client and a server alike; no transport |
| `retp-client` | a client: a transport under the connection, the native WebSocket, reads and writes. Compiles to wasm without its native transport |
| `retp-catalog` | the catalogue of servers of the `retp` command: servers by name, their secrets by reference and never by value |
| `energyml-etp-store` | the bridge: an ETP dataspace behind energyml-rust's `EnergymlStorage`, so that code which reads a file reads a server |
| `retp-conformance` | checks a server against what the specification makes mandatory, and writes the report a provider can act on |
| `retp-cli` | the `retp` command |
| `retp-python` | the Python bridge (`pip install retp`). Not published to crates.io |
| `etptypes` 2.0 | a facade over `retp-types`, for dependents of the old crate (not published yet: crates.io keeps 1.x) |

## Command line

> `retp-cli` is being extracted from the energyml-rust command line, where the
> same commands exist today as `energyml etp <command>`.

```sh
# What the server says of itself, before any session
retp probe wss://etp.example.com/etp

# Open a session and print what was agreed
retp connect wss://etp.example.com/etp

# Dataspaces, then what one holds
retp dataspaces wss://etp.example.com/etp
retp resources wss://etp.example.com/etp --uri "eml:///dataspace('demo')" --depth 1

# Objects by URI, and a whole dataspace as an EPC package
retp get wss://etp.example.com/etp "eml:///dataspace('demo')/resqml20.obj_LocalDepth3dCrs(50fb27d8-6c4c-469d-aa88-7ccb0cea63b9)" --out objects/
retp download-epc wss://etp.example.com/etp demo --out demo.epc

# One session, many commands
retp shell wss://etp.example.com/etp

# One dataspace as MCP tools for an AI assistant (feature `mcp`)
retp mcp wss://etp.example.com/etp --dataspace demo
```

Secrets go through the environment (`ETP_TOKEN`, `ETP_USER`, `ETP_PASSWORD`,
`ETP_CLIENT_SECRET`), not the command line. See [docs/cli.md](docs/cli.md).

To judge a server rather than use it:

```sh
retp-conformance wss://etp.example.com/etp --out report.md
```

## Install

- **Pre-built binaries**: an archive per platform (`retp-vX.Y.Z-<target>.tar.gz` or `.zip`, with `SHA256SUMS`) on each [GitHub release](https://github.com/geosiris-technologies/retp/releases).
- **From source**: `cargo install retp-cli` (add `--features hdf5,mcp` for HDF5 arrays and `retp mcp`).
- **Docker**: `docker pull ghcr.io/geosiris-technologies/retp`; `retp` and `retp-conformance`, nothing to install ([Docker](docs/docker.md)).

Copy-and-run examples, natively and with the image: [`examples/cli/`](examples/cli/).

## Python

```sh
pip install retp          # the Python package, imported as `retp`
```

A Python client over the Rust stack: sessions, dataspaces, EnergyML objects as
dictionaries, arrays as numpy arrays. It carries nothing the Rust library does
not have. See [docs/python.md](docs/python.md).

## Claude Code plugin

The repository is a [Claude Code](https://code.claude.com) plugin marketplace. Its `retp`
plugin teaches Claude to work with ETP servers through the `retp` command line - probe, list,
read, download, the server catalogue, conformance, `retp mcp` - with credentials by reference
only and writes only after you agree:

```sh
claude plugin marketplace add geosiris-technologies/retp
claude plugin install retp@retp
```

See [docs/claude-plugin.md](docs/claude-plugin.md).

## Documentation

- [Library usage](docs/client.md), [command line](docs/cli.md) and [Docker](docs/docker.md)
- [The Claude Code plugin](docs/claude-plugin.md)
- [What the conformance checker judges](docs/conformance.md)
- [Architecture decisions](docs/adr/README.md)

## Related

- [energyml-rust](https://github.com/geosiris-technologies/energyml-rust):
  RESQML, WITSML and PRODML in Rust, whose storage trait ETP plugs into.
- https://www.geosiris.com

## Licence

Apache License 2.0, see [LICENSE](LICENSE) and [NOTICE](NOTICE). The ETP
schemas are Copyright Energistics Consortium, Inc., Apache-2.0.

**Not affiliated with or endorsed by Energistics or The Open Group.**
Energistics(R), WITSML(TM), PRODML(TM) and RESQML(TM) are trademarks or
registered trademarks of Energistics Consortium, Inc.
