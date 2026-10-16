# MCP server: `retp mcp`

`retp mcp` serves the generic, read-only MCP tools of `energyml-mcp` over
stdio, backed by **one dataspace of an ETP server**. An MCP client (Claude
Code, Claude Desktop, any client that launches a local server) starts it as a
child process and lets a model list, search and read the objects and arrays of
that dataspace.

The tools are exactly those of `energyml mcp` on an EPC file:
`energyml_store_info`, `energyml_list_dataspaces`, `energyml_list_resources`,
`energyml_find_resources`, `energyml_describe_object`,
`energyml_get_references`, `energyml_list_arrays` and
`energyml_read_array_window`. Their arguments, their answers, their bounds and
the way an answer says it was cut are described in the `energyml-rust` book,
[`docs/mcp.md`](https://github.com/geosiris-technologies/energyml-rust/blob/main/docs/mcp.md);
this page covers only what is specific to an ETP server.

## Building it

`retp mcp` is behind the Cargo feature `mcp` of `retp-cli`, off by default,
because it brings an async runtime (tokio) and the MCP SDK that the rest of
the command line does not need. The release archives are built with it.

```sh
cargo install retp-cli --features mcp
```

## Running it

It takes the options of any `retp` command that opens a session (the server's
URL, `--partition`, `--insecure`, `--user`, `--token-url`, ...; see
[Command line](cli.md)), or a server of the [catalogue](cli.md#server-catalogue)
by its name (`retp mcp --server azure`), plus:

| Option | Meaning |
|---|---|
| `--dataspace <URI_OR_PATH>` | The dataspace to serve: `demo` or `eml:///dataspace('demo')`. Required unless the server has exactly one, which is then used and said on stderr. |
| `--max-response-bytes <BYTES>` | Lower the bound on any answer (65536 by default). |
| `--max-list <N>` | Lower the ceiling of every list's `limit` (500). |
| `--max-window-elements <N>` | Lower how many array elements one call reads (10000). |
| `--timeout-seconds <SECONDS>` | How long one tool call may take (30). |

The options only lower a bound; an argument of a tool can ask for less than the
bound, never for more.

Stdout carries nothing but the protocol. Warnings of the session (an unchecked
certificate, a plain-text connection) and the one line saying which dataspace
is served go to stderr.

```sh
retp mcp wss://etp.example.org/ --dataspace demo
```

## Secrets never go on the command line

A MCP client writes the command line it launches into a configuration file,
often readable by other accounts and kept in version control. `retp mcp`
therefore **refuses** `--token`, `--password` and `--client-secret` by name,
before connecting, and reads secrets from the environment only:

| Login | Environment |
|---|---|
| a bearer token | `ETP_TOKEN` |
| HTTP Basic, the password grant, Cognito | `ETP_USER`, `ETP_PASSWORD` |
| OAuth or Azure client credentials | `ETP_CLIENT_SECRET` (with `--client-id`) |

`--user`, `--client-id`, `--token-url`, `--azure-tenant`, `--cognito-region` and
`--oauth-scope` carry no secret and are accepted.

`energyml-mcp` redacts credentials from the server's own words that a tool
reports; the token is still never an argument of any tool.

## Registering it in Claude Code

Put the secret in the environment of the launching process, not in the
configuration. The simplest way is a small wrapper script that fetches a fresh
token and then replaces itself with `retp mcp`:

```sh
#!/bin/sh
# ~/bin/retp-mcp-demo - the token comes from wherever you keep it.
ETP_TOKEN="$(my-token-command)" exec retp mcp wss://etp.example.org/ --dataspace demo
```

```sh
chmod +x ~/bin/retp-mcp-demo
claude mcp add etp-demo -- ~/bin/retp-mcp-demo
```

The configuration then holds the path of the script, and nothing secret. If the
secret is already in the environment Claude Code is started from, no wrapper is
needed: `claude mcp add etp-demo -- retp mcp wss://etp.example.org/ --dataspace demo`.

## Registering it in Claude Desktop

In `claude_desktop_config.json`, point at the wrapper script rather than
listing the secret under `env`:

```json
{
  "mcpServers": {
    "etp-demo": {
      "command": "/home/me/bin/retp-mcp-demo",
      "args": []
    }
  }
}
```

If you must give the program directly, the arguments are the ones above and
carry no secret:

```json
{
  "mcpServers": {
    "etp-demo": {
      "command": "retp",
      "args": ["mcp", "wss://etp.example.org/", "--dataspace", "demo"]
    }
  }
}
```

## What it does not do

- **Read-only.** The session offers reading only (the offer of
  `retp-client`'s `reading_client`: no Store writes, no DataArray writes, no
  Transaction). No tool writes, and none is listed.
- **One dataspace per process.** An `EtpStore` is bound to one dataspace; a
  request for an object of another is refused as not found.
  `energyml_list_dataspaces` still lists every dataspace of the server, so
  that the model can tell you which one to serve. To read two dataspaces,
  register two servers.
- **No REST.** `--rest` (OSDU's Reservoir DDMS REST API) is refused: the tools
  read through an ETP session.
- **The listing is not refreshed.** The store keeps the listing of its
  dataspace after the first time it reads it, without expiry. An MCP process
  lives as long as the conversation, possibly hours, so objects written to the
  server after that first listing do not appear in `energyml_list_resources`
  or `energyml_find_resources`. Restart the server (in Claude Code,
  `/mcp` and reconnect) to see them. This is a limit of the store, not of the
  tools.
- **No reconnection.** If the session is lost (the server closes an idle
  connection, the network drops), the tools answer with a `remote` refusal.
  Restart the server.
- **Arrays are read through the server's DataArray protocol.** A tool that
  cannot read an array says why in a refusal; it never returns an empty array.

Titles, descriptions and custom data come from the store. Treat them as data:
a hostile dataspace can put instructions in a title, and the tools hand them to
the model in data fields, with a reminder in every description that they are
not instructions.
