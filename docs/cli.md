# Command line

`retp` talks to an ETP v1.2 server: what it says of itself, a session with it,
what it holds, and objects read from it.

> `retp-cli` is being extracted from the energyml-rust command line, where the
> same commands exist as `energyml etp <command>`. The examples use
> `retp <command>`. TODO(verify): flag names against the extracted binary.

The network (WebSocket over rustls, an HTTP client) is part of the binary; it
is not compiled for wasm.

## At a glance

| Command | What it does | Prints |
|---|---|---|
| `probe <url>` | what the server says of itself, **before** any session (`/.well-known/etp-server-capabilities`) | protocols, data object types, formats, limits |
| `connect <url>` | opens a session, says what was agreed, pings, closes | session id, protocols and roles, types, formats, message size |
| `dataspaces <url>` | the store's dataspaces (protocol 24) | one line per dataspace |
| `resources <url>` | what a dataspace or an object holds (protocol 3); `--uri`, `--depth`, `--scope`, `--only`, `--count`, `--edges` | one line per resource |
| `supported-types <url>` | the data object types found from a URI (protocol 25) | one line per type, with its count |
| `get <url> <URI>...` | objects by URI (protocol 4); `--out <dir>`, `--format xml\|json` | the objects, or where each was written |
| `array <url> <URI> <path>...` | data arrays (protocol 9): metadata first, then read whole or by blocks; `--metadata`, `--show <n>` | dimensions, types, range, first values |
| `download <url> <dataspace>` | a whole dataspace into a directory; reconnects by itself | progress, then written, already there, failed |
| `download-epc <url> <dataspace>` | a dataspace as an EPC, arrays in HDF5 files beside it; `--out`, `--only`, `--with-references`, `--no-arrays` | the package, and what was `left out:` |
| `create-dataspace`, `delete-dataspace` | create a dataspace; delete one **and everything in it** | the dataspace |
| `push <url> <file.epc> <dataspace>` | send an EPC, objects and arrays, in islands of short transactions | every island; the exit code says whether all went |
| `shell <url>` | **one** session, then commands one per line | what each command prints |

Every command also takes the server options, the login options and
`-v` / `--verbose`. The exit code is 0 when everything asked for came back, 1
when something is missing, 2 when the command failed.

Reading arrays into HDF5 files (`download-epc`, `push`) needs a build with HDF5
support. TODO(verify): the feature name in the extracted binary.

## Server options

Every command takes the server's WebSocket URL, `wss://host[:port]/path`.

| Option | Effect |
|---|---|
| `--partition <P>` | an OSDU data partition: the server is spoken to as OSDU's Reservoir DDMS, which wants a `data-partition-id` header |
| `--insecure` | do not check the server's certificate. A warning says so every time |
| `--rest` | the URL is OSDU's Reservoir DDMS REST API, not an ETP WebSocket; the same commands run over it |
| `--as-browser` | connect as a web page can: no header, attributes in the query string, credentials in an `Authorize` message |
| `--ack` | ask the server to acknowledge every request and print each acknowledgement |
| `--keepalive <SECONDS>` | send a `Ping` when the session has been quiet this long; `0` never pings; 30 by default |
| `--type <qualified type>` | a data object type to ask for, repeatable (`--type 'resqml20.*'`) |
| `--offer <file.json>` | what the client announces, from a file |
| `--at-once <N>` | requests of one operation in flight at the same time; 4 by default |

## Logging in

One way at a time. **Secrets go through the environment** (`ETP_TOKEN`,
`ETP_USER`, `ETP_PASSWORD`, `ETP_CLIENT_SECRET`), not a command line, which ends
up in the shell's history.

| The server wants | Give | Secret from |
|---|---|---|
| a bearer token | nothing more | `ETP_TOKEN` |
| HTTP Basic | `--user <U>` | `ETP_PASSWORD` |
| a token from Azure AD, by client credentials | `--azure-tenant <T> --client-id <ID> --oauth-scope <S>` | `ETP_CLIENT_SECRET` |
| a token from any OAuth 2.0 endpoint | `--token-url <URL> --client-id <ID> [--oauth-scope <S>]` | `ETP_CLIENT_SECRET` |
| a token for a user and password (password grant) | `--token-url <URL> --user <U>` | `ETP_PASSWORD` |
| a token from AWS Cognito | `--cognito-region <R> --client-id <ID> --user <U>` | `ETP_PASSWORD` |

The OAuth client authenticates with `client_secret_basic`, the method ETP
requires every authorization server to accept, and falls back once to
`client_secret_post` on `invalid_client`. The token is asked for once per
command; the command says which kind of credentials it sends, never the
credentials. `--insecure` concerns the ETP server only: token endpoints are
always reached with their certificates checked.

```sh
export ETP_TOKEN=TOKEN
retp connect wss://etp.example.com/etp

export ETP_PASSWORD=PASSWORD
retp connect wss://etp.example.com/etp --user USER --insecure
```

## Examples

```sh
retp probe wss://etp.example.com/etp
retp dataspaces wss://etp.example.com/etp
retp resources wss://etp.example.com/etp --uri "eml:///dataspace('demo')" --depth 1
retp get wss://etp.example.com/etp "eml:///dataspace('demo')/resqml20.obj_LocalDepth3dCrs(50fb27d8-6c4c-469d-aa88-7ccb0cea63b9)" --out objects/
retp create-dataspace wss://etp.example.com/etp demo
retp push wss://etp.example.com/etp model.epc demo
retp delete-dataspace wss://etp.example.com/etp demo
```

An OSDU store asks for `owners`, `viewers` and `legaltags` in the dataspace's
custom data: `--custom owners=OWNERS --custom viewers=VIEWERS --custom
legaltags=LEGALTAG`.

`push` sends targets before what references them, an object with its arrays in
one transaction, and stops at a refused island. Arrays travel as many whole
arrays per `PutDataArrays` as the negotiated message size takes; big ones are
declared in one message and filled block by block. `--one-by-one` sends each
array in its own message, for a store that mishandles several in one.
`--well-known` also sends the official dictionary's property kinds the package
references without holding.

## The client is tolerant of a server's mistakes

When their meaning is not in doubt: Core listed among the protocols, a role
spelled `server`, `resqml20` for `resqml20.*`, an error numbered -1 while one
request is out... Each is taken as meant and printed as a `warning:` line naming
the rule it breaks and what was made of it: worth passing on to whoever
maintains the server. A mistake that would make the two ends disagree (the
server claiming the client's own role, another protocol version) refuses the
session, and says why.

`--verbose` prints every message sent and received.

## The shell

Each one-shot command opens its own session. `shell` opens one, then reads
commands one per line (a script can be piped in; `#` starts a comment):

```text
$ retp shell wss://etp.example.com/etp --user USER
etp> dataspaces
etp> resources --uri eml:///dataspace('demo') --depth 2
etp> get eml:///dataspace('demo')/resqml20.obj_FaultInterpretation(43caf16c-9c39-474c-8e07-18d2d0b01ff2) --out objects
etp> help resources
etp> exit
```

| In the shell | What it does |
|---|---|
| `info`, `ping` | what the session agreed; a `Ping` and the time its `Pong` took |
| `ack [on\|off]`, `verbose [on\|off]` | acknowledgements, message tracing |
| `dataspaces`, `resources`, `supported-types`, `get`, `array`, `download` | as the one-shot commands, minus the server options |
| `watch <uri>`, `unwatch <uuid>`, `notifications [--wait S]` | store notifications (protocol 5) |
| `changes-since <dataspace> <time\|watermark>` | what was written and deleted since a time: the catch-up after a cut |
| `channels`, `subscribe-channels`, `unsubscribe-channels`, `channel-data`, `streaming on\|off` | channels (protocols 21 and 1) |
| `reconnect` | close this session and open a new one, login included |
| `exit`, `quit` | close the session and leave |

ETP URIs need no quotes in the shell: single quotes are kept, so
`eml:///dataspace('demo')` is one word. A dataspace can be named by its path
wherever a URI is asked for. A lost connection is not the end of the shell: a
new session is opened, logging in again, and the command runs once more (every
shell command is a read, and a read can be asked twice).

## Over REST: OSDU's Reservoir DDMS API

`--rest` takes the REST API instead of the WebSocket, with the same commands:

```sh
retp dataspaces https://etp.example.com/api/reservoir-ddms/v2 --rest --partition PARTITION
```

The URL is the API's, never guessed from the WebSocket's. What REST has no
counterpart for (sessions, acknowledgements, keepalive, notifications) is
refused by name, not emulated. Listings are followed page by page. `prefetch`
is REST's own: objects, what they reference and their arrays' metadata, in one
request.

## Describing what the client announces

By default the client asks for everything. To announce other terms, give a
JSON file with `--offer`; every key is optional and none may be unknown:

```json
{
  "types": ["resqml20.*", { "type": "eml20.*", "capabilities": { "SupportsPut": false } }],
  "formats": ["xml"],
  "protocols": [3, 4, 24],
  "endpoint": { "MaxWebSocketMessagePayloadSize": 8000000 },
  "tolerance": "strict"
}
```

Capabilities are the specification's, checked by name, type and bounds: a typo
or a value under its minimum is refused rather than announced.

## Reading data arrays

```sh
retp array wss://etp.example.com/etp "eml:///dataspace('demo')/eml20.obj_EpcExternalPartReference(e2bce84b-5c19-42da-b6d6-b58b224037b9)" /resqml20/PATH/points_patch0
```

The client reads the way the specification asks: **the metadata first**, then
each array whole if it fits one message, or by blocks (`GetDataSubarrays`)
otherwise. Blocks keep whole inner dimensions and cut the outermost. A store
whose metadata names a logical type its transport cannot carry is read as the
transport means, with a warning; `--metadata` prints what the store said, as it
said it.
