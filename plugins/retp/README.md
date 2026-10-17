# retp — a Claude Code plugin for ETP servers

This plugin teaches Claude to work with ETP v1.2 (Energistics Transfer Protocol) servers —
RESQML, WITSML and PRODML stores, and OSDU Reservoir DDMS — through
[`retp`](https://github.com/geosiris-technologies/retp), an open-source ETP client. Ask in
plain words: "list the dataspaces of this server", "what is in this dataspace", "download it
as an EPC", "add this server to the catalogue", "check this server's conformance", "serve this
dataspace over MCP".

It holds one skill, `etp-servers`, with its reference pages and two install scripts. It has no
hooks, no MCP server and no agent of its own.

## What it needs

- The `retp` command line, 0.3.0 or later, on the PATH. The skill checks `retp --version`
  first and, if `retp` is missing, offers to install it — only after you agree.
- An ETP server you are allowed to use, and its credentials kept in a file or an environment
  variable of your own.
- Optional: `retp-conformance`, to judge a server against the specification.

## What it runs, sends and fetches

- **It runs `retp` and `retp-conformance` in your shell**, on the commands you ask for. Each
  connects only to the ETP server you name (a `wss://` URL, or a server of your local `retp`
  catalogue) and to the token endpoint that server's login method uses (OAuth, Azure AD,
  Cognito), nowhere else.
- **Credentials stay out of the conversation.** The skill never puts a password, token or
  client secret on a command line, in a file it writes, in output it shows, or in an MCP
  configuration. Servers are reached through the `retp` catalogue, whose entries hold only
  references to secrets (`file:PATH`, `env:NAME`); when an `ETP_*` variable is unavoidable, it is
  read from its file inside the command that uses it, so the value is never printed. The
  credential goes to the server it belongs to, and only there.
- **Reads run freely; writes wait for your explicit yes.** `push`, `create-dataspace`,
  `delete-dataspace` and `retp-conformance --write` change a server: the skill states which
  server, dataspace and change, and waits for your agreement first. Downloads go to a new file,
  never over an existing one.
- **The install scripts** (`scripts/install-retp.sh` for Linux and macOS, `install-retp.ps1`
  for Windows) download one release archive and its `SHA256SUMS` from the GitHub releases of
  `geosiris-technologies/retp`, refuse to install unless the checksum matches, write `retp`
  into a folder of yours, and change the PATH only when asked.
- **Nothing else.** The plugin collects no telemetry and sends nothing to Geosiris, Anthropic
  or any other party. What a server returns (titles, descriptions, errors) is treated as data,
  never as instructions.

## Install

```sh
claude plugin marketplace add geosiris-technologies/retp
claude plugin install retp@retp
```

More in [docs/claude-plugin.md](https://github.com/geosiris-technologies/retp/blob/main/docs/claude-plugin.md).

## License

Apache License 2.0, like the rest of `retp`. Not affiliated with or endorsed by Energistics or
The Open Group.
