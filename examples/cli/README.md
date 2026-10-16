# Command line examples

Every example talks to the server named by `ETP_URL`, a placeholder you set:

```sh
export ETP_URL=wss://etp.example.com/etp
```

Secrets are never arguments: `ETP_TOKEN`, `ETP_USER`, `ETP_PASSWORD` and
`ETP_CLIENT_SECRET` come from the environment (see
[Logging in](../../docs/cli.md#logging-in)).

## Get the CLI

- An archive: `retp-vX.Y.Z-<target>.tar.gz` (`.zip` on Windows) attached to each
  GitHub release of `geosiris-technologies/retp`, with a `SHA256SUMS` file to
  check it against.
- Cargo: `cargo install retp-cli` (add `--features hdf5` for `download-epc`, and
  `--features mcp` for `retp mcp`).
- Docker: `docker pull ghcr.io/geosiris-technologies/retp:latest`, see
  [The `retp` image](../../docs/docker.md).

In the Docker commands below, `IMG=ghcr.io/geosiris-technologies/retp:latest`,
and `-e ETP_TOKEN` passes your token through without writing it (drop it for a
server that wants no login).

## Probe a server

What it says of itself, before any session.

```sh
retp probe "$ETP_URL"
docker run --rm $IMG probe "$ETP_URL"
```

## Open a session

Says what was agreed, pings, closes.

```sh
retp connect "$ETP_URL"
docker run --rm -e ETP_TOKEN $IMG connect "$ETP_URL"
```

## List dataspaces and resources

```sh
retp dataspaces "$ETP_URL"
retp resources "$ETP_URL" --uri "eml:///dataspace('demo')" --depth 1

docker run --rm -e ETP_TOKEN $IMG dataspaces "$ETP_URL"
docker run --rm -e ETP_TOKEN $IMG resources "$ETP_URL" --uri "eml:///dataspace('demo')" --depth 1
```

## Read objects

`--out` writes each object into a folder; with Docker, mount the current folder.

```sh
URI="eml:///dataspace('demo')/resqml20.obj_LocalDepth3dCrs(50fb27d8-6c4c-469d-aa88-7ccb0cea63b9)"
retp get "$ETP_URL" "$URI" --out objects/
docker run --rm -e ETP_TOKEN -v "$PWD:/work" $IMG get "$ETP_URL" "$URI" --out objects/
```

## Download a dataspace as an EPC

The arrays go into HDF5 files beside the package; this needs a build with HDF5
(the Docker image has it).

```sh
retp download-epc "$ETP_URL" demo --out demo.epc
docker run --rm -e ETP_TOKEN -v "$PWD:/work" $IMG download-epc "$ETP_URL" demo --out demo.epc
```

On Linux, if the container cannot write into the folder, add
`--user "$(id -u):$(id -g)"`.

## The interactive shell

One session, commands typed one per line (`help`, `dataspaces`, `exit`...).

```sh
retp shell "$ETP_URL"
docker run --rm -it -e ETP_TOKEN -v "$PWD:/work" $IMG shell "$ETP_URL"
```

## Judge a server

`retp-conformance` checks the server against what the specification makes
mandatory and writes a report. Without `--write` it writes nothing to the server.

```sh
retp-conformance "$ETP_URL" --out report.md --json report.json
docker run --rm -e ETP_TOKEN -v "$PWD:/work" --entrypoint retp-conformance $IMG \
  "$ETP_URL" --out report.md --json report.json
```

## Run `retp mcp` for an assistant

Read-only MCP tools on one dataspace, over stdio. It needs the `mcp` feature
(`cargo install retp-cli --features mcp`; the release archives have it; the
container image has it from the release after 0.2.0).

```sh
claude mcp add etp-demo -- retp mcp "$ETP_URL" --dataspace demo
claude mcp add etp-demo -- docker run --rm -i -e ETP_TOKEN $IMG mcp "$ETP_URL" --dataspace demo
```

`-i` keeps stdin open for the protocol; no `-t`. The shell expands the
variables when you register, so the configuration holds the URL, not the token.
See [MCP server](../../docs/mcp.md) and [the image](../../docs/docker.md) for
wrappers that fetch the token at launch.

## The tours

`tour.sh` and `tour-docker.sh` run a read-only tour (probe, connect, dataspaces,
resources of the first dataspace) against `$ETP_URL`, natively or through the
image:

```sh
ETP_URL=wss://etp.example.com/etp sh examples/cli/tour.sh
ETP_URL=wss://etp.example.com/etp sh examples/cli/tour-docker.sh
```
