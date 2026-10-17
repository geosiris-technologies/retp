# The server catalogue (retp 0.3.0+)

The catalogue keeps servers by name, with their URL, partition and login method, and **holds
no secret: only a reference** to one, `file:PATH` or `env:NAME`. It is the preferred way to
reach a server from this skill, because nothing secret ever passes through a command line.
`retp server --help`, `retp server add --help` and `retp catalog init --help` are the authority
on options.

## Where it lives

- `retp catalog init FOLDER` chooses the folder (it must be given when not run from a
  terminal, which is the case for Claude's commands) and writes the configuration file.
- The configuration file is in the OS's configuration folder; `RETP_CONFIG` names another one.
  `RETP_CATALOG` names the catalogue folder directly.
- In the folder: `NAME/server.toml` (the definition, shareable, no secret) and `NAME/state/`
  (local: the record of `server test`, the shell's history), which a `.gitignore` written by
  `catalog init` keeps out of git.

Run `retp server list` first: the catalogue may already exist.

## Adding a server

`retp server add` with no argument opens a guided form that only works on a terminal. From
Claude's tool, give everything as options: `retp server add NAME URL --auth METHOD ...`. A
script is never asked a question; a missing field is refused by name.

1. Ask the user for the URL, the method and its non-secret fields (user, client id, tenant,
   token URL, scope, region, partition).
2. Ask them to put the secret in a file **themselves** — not through you — readable by them
   only, for example:

   ```sh
   mkdir -p ~/.config/retp && chmod 700 ~/.config/retp
   # the user writes the secret into the file with an editor, then:
   chmod 600 ~/.config/retp/my-server.pass
   ```

   On Windows: a file under `%USERPROFILE%\.config\retp\` (retp does not check Windows ACLs).
   An environment variable set outside this conversation also works: `env:NAME`.
3. Add the entry with the reference, then check it:

   ```sh
   retp server add my-server wss://etp.example.com/etp --auth basic --user demo-user \
       --password-from file:~/.config/retp/my-server.pass
   retp server show my-server        # "resolves: file exists", never the value
   retp server test my-server        # definition, references, /.well-known, token, session
   ```

| Method (`--auth`) | Options | Secret reference |
|---|---|---|
| `none` | | |
| `basic` | `--user U` | `--password-from REF` |
| `bearer` | | `--token-from REF` |
| `oauth-password` | `--token-url URL --user U [--client-id ID] [--scope S]` | `--password-from REF` [`--client-secret-from REF`] |
| `oauth-client` | `--token-url URL --client-id ID [--scope S]` | `--client-secret-from REF` |
| `azure` | `--tenant T --client-id ID [--scope S]` (`--user U` for a password grant) | `--client-secret-from REF` (or `--password-from REF`) |
| `cognito` | `--region R --client-id ID --user U` | `--password-from REF` [`--client-secret-from REF`] |

Other options: `--partition P` (OSDU data partition), `--other-url URL` and
`--transport etp|rest` (a server with both a WebSocket and a REST API), `--insecure` (a
known-bad certificate; warned at every connection), `--protected` (writes ask for
confirmation), `--description`, `--alias NAME`, `--legal-tag`, `--owner`, `--viewer` (OSDU write
defaults added to `create-dataspace`), `--force` (replace an entry). `--header` /
`--header-from` define extra headers, but 0.3.0 cannot send them yet and refuses to connect to
such an entry, by name.

A name is lowercase letters, digits, `.`, `_`, `-`, and cannot be a command of `retp`.

## Using it

```sh
retp --server my-server dataspaces    # works in every shell; -s my-server for short
retp '@my-server' dataspaces          # `@` always means a server; quoted, as PowerShell needs
retp my-server dataspaces             # the same, unless a command has that name
retp --server my-server dataspaces    # the same; -s is short
retp --server my-server shell         # the shell (retp shell my-server fails in 0.3.0)
retp-conformance --server my-server --out report.md
retp mcp --server my-server --dataspace demo
```

- **The ambient `ETP_*` variables are ignored** over a catalogue server, so that a token set
  for one server never goes to another. An entry that wants one says so:
  `--token-from env:ETP_TOKEN`. Do not "fix" a failed login by adding `--token` (it is refused
  anyway) or by switching to the URL with an `ETP_*` variable.
- **Login options are refused** with a catalogue server (`--user`, `--password`, ...), and so is
  a URL: the entry is the only source of its login.
- **The default server** (`retp server default NAME`) serves the commands that read; a command
  that writes always needs the server named on the line.
- **Protected servers** (`protected = true`): a write asks for the server's name typed at a
  terminal, which Claude's tool cannot answer. Only after the user agreed to that exact write,
  add `--yes`.
- `--save-as NAME` after a successful command (with a URL) saves the connection; a secret that
  came from an `ETP_*` variable is saved as a reference to that variable, shared with every
  server that uses it (the output says so).

## `server test` is not the last word

Its steps are `definition`, `references`, `well-known`, `token`, `session`. A server that
protects `/.well-known` with another method than the session (seen: Bearer required there,
Basic accepted for the session) fails at `well-known` and skips the rest. Try
`retp --server NAME connect` before saying the server is unreachable.

## In Docker

Mount the catalogue read-only and name it with `RETP_CATALOG`; pass an `env:` reference with
`-e NAME` (no value), mount a `file:` one at the same path:

```sh
docker run --rm -v "$HOME/retp-servers:/servers:ro" -e RETP_CATALOG=/servers \
    -e MY_SERVER_TOKEN ghcr.io/geosiris-technologies/retp:latest my-server dataspaces
```

A read-only catalogue cannot record `server test`; `server add` and `--save-as` need it
writable.

## An example `server.toml`

References only — retp refuses a literal `password`, `token`, `client_secret` or `secret`:

```toml
schema = 1
description = "an example server"

[server]
etp_url = "wss://etp.example.com/etp"
transport = "etp"
tls = "verify"
handshake = "native"
protected = true

[auth]
method = "basic"
user = "demo-user"
password = { file = "~/.config/retp/my-server.pass" }
```
