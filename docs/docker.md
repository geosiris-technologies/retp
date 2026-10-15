# The `retp` image

The ETP v1.2 client and the conformance checker, without compiling anything: the
`retp` command line built with HDF5, in a container, talking to your server and
writing into a mounted folder.

## Get it

```sh
docker pull ghcr.io/geosiris-technologies/retp:latest
```

`latest` is the newest release. Every release is also published as `X.Y.Z` and
`X.Y` (for example `0.2.0`, `0.2`), for a chain that must not move under its
feet. A pre-release is published as `X.Y.Z-pre` and `dev`, never as `X.Y` or
`latest`. `edge` follows the main branch. Images are built for amd64 and arm64
by the CI of the repository, never by hand.

Or build it from a checkout, see the end of this page.

## Use it

The examples use a shell variable for the image and for the server; the URL is
a placeholder, write yours:

```sh
IMG=ghcr.io/geosiris-technologies/retp:latest
ETP_URL=wss://etp.example.com/etp

docker run --rm $IMG probe "$ETP_URL"
docker run --rm $IMG connect "$ETP_URL"
docker run --rm $IMG dataspaces "$ETP_URL"
docker run --rm $IMG resources "$ETP_URL" --uri "eml:///dataspace('demo')" --depth 1
```

Commands that write files need a mounted folder. It is mounted at `/work`, the
working directory of the image, so paths are written relative to it:

```sh
docker run --rm -v "$PWD:/work" $IMG get "$ETP_URL" \
  "eml:///dataspace('demo')/resqml20.obj_LocalDepth3dCrs(50fb27d8-6c4c-469d-aa88-7ccb0cea63b9)" \
  --out objects/

docker run --rm -v "$PWD:/work" $IMG download-epc "$ETP_URL" demo --out demo.epc
```

`download-epc` writes the EPC and the HDF5 files of the arrays beside it; the
image is built with HDF5 for that. See [Command line](cli.md) for every command
and option.

The shell keeps one session open and reads commands from the terminal, so it
needs `-it`:

```sh
docker run --rm -it -v "$PWD:/work" $IMG shell "$ETP_URL"
```

PowerShell users write `-v "${PWD}:/work"`.

The image runs as uid 1000, not root. On a Linux host, files it writes into the
mounted folder belong to that user, and the folder must let it write; if it
does not, run the container as yourself:
`docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/work" ...`.
TODO(verify): that `--user` is enough on Linux (the image's home directory
`/home/retp` is then not the user's). Docker Desktop maps the files to your
user.

## Logging in

Secrets go through the environment, never the command line (see
[Logging in](cli.md#logging-in)). `-e NAME` without a value passes the variable
from your shell to the container, so the secret is not written in the command:

```sh
export ETP_TOKEN=...   # from wherever you keep it
docker run --rm -e ETP_TOKEN $IMG dataspaces "$ETP_URL"

export ETP_PASSWORD=...
docker run --rm -e ETP_PASSWORD $IMG dataspaces "$ETP_URL" --user USER
```

The variables are `ETP_TOKEN`, `ETP_USER`, `ETP_PASSWORD` and
`ETP_CLIENT_SECRET`. The options that carry no secret (`--user`, `--client-id`,
`--token-url`, `--azure-tenant`, `--cognito-region`, `--oauth-scope`,
`--partition`) are ordinary arguments.

## The conformance checker

The image holds `retp-conformance` too. The entry point is `retp`, so name the
other program with `--entrypoint`:

```sh
docker run --rm -e ETP_TOKEN -v "$PWD:/work" \
  --entrypoint retp-conformance $IMG \
  "$ETP_URL" --out report.md --json report.json
```

The report is written in the mounted folder. Without `--write` the checker
writes nothing to the server. What it judges is in [Conformance
checker](conformance.md).

## `retp mcp` in a container

`retp mcp` serves one dataspace as read-only MCP tools over stdio (see [MCP
server](mcp.md)). It is in the image **from the release after 0.2.0**: the 0.2.0
image does not have it, and `retp mcp` is refused there. `dev` and `edge` carry
it once it is merged and built. TODO(verify): the first tag that carries it.

Over stdio the container must keep its standard input open (`-i`) and must not
have a terminal (no `-t`); `--rm` removes it when the client leaves. The token
is passed through the environment of the process that launches Docker:

```sh
docker run --rm -i -e ETP_TOKEN $IMG mcp "$ETP_URL" --dataspace demo
```

Register it in Claude Code from a shell where `ETP_TOKEN` is set. `-e ETP_TOKEN`
carries no value, so the registration holds no secret:

```sh
claude mcp add etp-demo -- docker run --rm -i -e ETP_TOKEN \
  ghcr.io/geosiris-technologies/retp:latest \
  mcp wss://etp.example.com/etp --dataspace demo
```

TODO(verify): that Claude Code hands the launched `docker` the environment it
was started from. If the token is fetched at launch, use a wrapper, as in
[Registering it in Claude Code](mcp.md#registering-it-in-claude-code):

```sh
#!/bin/sh
# ~/bin/retp-mcp-demo - the token comes from wherever you keep it.
ETP_TOKEN="$(my-token-command)" exec docker run --rm -i -e ETP_TOKEN \
  ghcr.io/geosiris-technologies/retp:latest \
  mcp wss://etp.example.com/etp --dataspace demo
```

```sh
chmod +x ~/bin/retp-mcp-demo
claude mcp add etp-demo -- ~/bin/retp-mcp-demo
```

For any other client that reads a JSON configuration, point it at the wrapper
and leave `env` out, so the secret is not in the file:

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

If the secret is already in the environment the client is started from, the
arguments alone are enough, and carry no secret:

```json
{
  "mcpServers": {
    "etp-demo": {
      "command": "docker",
      "args": ["run", "--rm", "-i", "-e", "ETP_TOKEN",
               "ghcr.io/geosiris-technologies/retp:latest",
               "mcp", "wss://etp.example.com/etp", "--dataspace", "demo"]
    }
  }
}
```

Stdout carries nothing but the protocol; the session's warnings go to stderr.

## What the image holds

| Path | What |
|---|---|
| `/usr/local/bin/retp` | the command line, built with HDF5 (and, from the next release, `mcp`); the entry point |
| `/usr/local/bin/retp-conformance` | the conformance checker; run it with `--entrypoint retp-conformance` |

It is based on `debian:bookworm-slim` with the HDF5 runtime library and the CA
certificates. It runs as user `retp` (uid 1000) in `/work`; with no argument it
prints the help.

Without Docker: the release archives `retp-vX.Y.Z-<target>.tar.gz` (`.zip` on
Windows) attached to each GitHub release with a `SHA256SUMS` file,
`cargo install retp-cli` (add `--features hdf5` and/or `mcp`), and
`pip install retp` for Python.

## Building it from a checkout

```sh
docker build -f docker/Dockerfile -t retp .
```

`--build-arg RELEASE_LTO=thin` builds faster and with less memory. Then use
`retp` in place of `$IMG` above.
