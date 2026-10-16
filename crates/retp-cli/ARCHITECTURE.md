# Command line (`retp-cli`) - architecture

Related decision: [ADR 0001](../../docs/adr/0001-client-architecture.md).

## Purpose and position

`retp`: try an ETP v1.2 server from a terminal: probe it, open a session, list, read, download a dataspace as an EPC, shell. It is the top of the stack for this repository: `retp-client` does the work, `energyml-etp-store` packs a dataspace, `energyml-storage` writes the EPC. The same program is reachable as `energyml etp …` from the EnergyML command line, which runs `retp` when it is on `PATH` (an external subcommand, so that the EnergyML CLI never depends on ETP).

## Rules

- **Argument parsing and printing only.** `src/main.rs` parses and returns the exit code; `src/etp.rs` maps arguments to client calls and prints. The probe, the session, the requests and their refusals are the client's; a decision found here is moved down.
- **One connection choice.** Options pour into `login::Choice` of the client; secrets are read from the environment (`ETP_TOKEN`, `ETP_USER`, `ETP_PASSWORD`, `ETP_CLIENT_SECRET`), never required on the command line. A certificate check skipped by `--insecure` is stated at every use.
- **A server by name is the catalogue's** (`retp-catalog`, ADR 0007): the command line is rewritten before `clap` parses it (`retp azure`, `@azure`, `--server azure`), and `src/catalog_cli.rs` turns an entry into the same `Endpoint` and `Choice` the options give. It reads, validates and saves the catalogue through the crate; nothing about a secret is decided here, and over a catalogue server the ambient `ETP_*` variables are ignored.
- **The shell uses the one-shot commands' argument definitions** (the same `clap` structures), so an option exists in both or in neither.
- **Writing arrays needs HDF5, and says so.** The `hdf5` feature (the C library at build time) is off by default; without it `download-epc` refuses to write arrays and names the feature rather than writing an incomplete package.
- **Reads are the default; anything that writes is explicit.** The project's own tests of writes run only against a test double, never against a third-party server.
- **The ETP crates' `tracing` events go to stderr** through a layer of a few lines: warnings always, the rest of the `retp*` and `energyml*` targets with `--verbose`; only the text is printed, not the fields. The layer also installs the `log` to `tracing` bridge, for the dependencies (rustls) that still write with `log`.

## Tests

The behaviour is tested where it lives (`retp-client`, the bridge). The binary keeps only the tests of its own parsing (for example how a shell line is split). Run them through the workspace (`cargo test --workspace a_shell_line`) rather than `cargo test -p`, so feature resolution matches the workspace.
