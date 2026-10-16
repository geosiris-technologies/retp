# retp-catalog

A catalogue of ETP servers for the `retp` command line: one entry per server (a `server.toml` in a folder of its own), reached by name - `retp azure`, `retp @azure dataspaces`, `retp --server azure dataspaces`.

- An entry holds **no secret**, only a reference (`{ env = "NAME" }` or `{ file = "PATH" }`); a literal password, token or client secret is refused when the file is read.
- The login is built by the `Choice` of `retp-client`: one door, whatever the method (none, basic, bearer, OAuth 2.0 password or client credentials, Azure AD, AWS Cognito).
- Native only: `retp-client` compiles for the browser and does not depend on this crate.

## Documentation

- API reference: <https://docs.rs/retp-catalog>
- Repository documentation: [`docs/`](../../docs/), [cli](../../docs/cli.md), [ADR 0007](../../docs/adr/0007-server-catalogue.md)

## Licence

Apache-2.0. See [`LICENSE`](../../LICENSE).
