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
| `push <url> <file.epc> <dataspace>` | send an EPC, objects and arrays, in islands of short transactions; `--plan` prints the islands and sends nothing | every island; the exit code says whether all went |
| `shell <url>` | **one** session, then commands one per line | what each command prints |
| `mcp <url>` | an MCP server over stdio, on one dataspace (feature `mcp`); see [MCP server](mcp.md) | nothing on stdout but the MCP protocol; logs on stderr |
| `server add\|list\|show\|remove\|test\|default` | the [server catalogue](#server-catalogue): servers by name, secrets by reference | one line per server; a verdict per step |
| `catalog init [folder]` | choose the folder of the catalogue | where it is |

Every command also takes the server options, the login options and
`-v` / `--verbose`. The exit code is 0 when everything asked for came back, 1
when something is missing, 2 when the command failed.

Reading arrays into HDF5 files (`download-epc`, `push`) needs a build with HDF5
support. TODO(verify): the feature name in the extracted binary.

## As MCP tools for an assistant

`retp mcp <url> --dataspace <name>` serves one dataspace of the server as
read-only MCP tools, over stdio, for an MCP client such as an AI assistant.
It is built with the feature `mcp` (the release archives have it). The
tokens and passwords are never accepted as arguments: they come from the
environment. Everything else is in [MCP server](mcp.md).

## Server options

Every command takes the server's WebSocket URL, `wss://host[:port]/path`.

| Option | Effect |
|---|---|
| `--header <NAME=VALUE>`, `-H` | a header this server wants in addition, repeatable; see [Headers a server wants](#headers-a-server-wants) |
| `--insecure` | do not check the server's certificate. A warning says so every time |
| `--rest` | the URL is OSDU's Reservoir DDMS REST API, not an ETP WebSocket; the same commands run over it |
| `--as-browser` | connect as a web page can: no header, attributes in the query string, credentials in an `Authorize` message |
| `--ack` | ask the server to acknowledge every request and print each acknowledgement |
| `--keepalive <SECONDS>` | send a `Ping` when the session has been quiet this long; `0` never pings; 30 by default |
| `--type <qualified type>` | a data object type to ask for, repeatable (`--type 'resqml20.*'`) |
| `--offer <file.json>` | what the client announces, from a file |
| `--at-once <N>` | requests of one operation in flight at the same time; 4 by default |
| `--server <NAME>`, `-s` | a server of the [catalogue](#server-catalogue) instead of the URL (`--serveur` is a hidden alias) |
| `--save-as <NAME>` | after the command has succeeded, save this connection in the catalogue; add `--force` to replace one |
| `--yes` | confirm, without asking, a write to a protected catalogue server |

## Headers a server wants

Which headers a server expects is a property of the server, like its URL: a tenant, a
routing key, a data partition, an API key. `--header NAME=VALUE` gives one, and is
repeated for several:

```sh
retp dataspaces wss://host/etp --header x-tenant=acme -H x-region=eu
```

The value is everything after the first `=`, sent as given. Each header goes with **every
request** to the server: the WebSocket handshake, the probe of
`/.well-known/etp-server-capabilities`, each call of the REST API (`--rest`).

What is refused, by name, before anything is asked of the network (no login is spent
on a line that cannot be sent):

- a name that is not letters, digits, `-` and `_` (the text is not repeated: written as
  `Name: value`, it would hold the value), and a value with a control character (a line
  break in it would start another header) or a character that is not ASCII;
- a header the client writes itself: `authorization` (give credentials, see
  [Logging in](#logging-in)), `etp-encoding`, `maxwebsocketframepayloadsize`,
  `maxwebsocketmessagepayloadsize`, `host`, `connection`, `upgrade`, `accept`,
  `content-type`, `content-length`, `transfer-encoding` and every `sec-websocket-*`;
- the same header twice, whatever the case of its letters;
- with `--as-browser`, a header that carries a secret: the handshake of a front end has
  no header, so its attributes are written in the query string (ETP 1.2 section 4.3.2),
  and a secret is never written in a URL. A header carries a secret when a catalogue
  entry gives it by reference, or when its name says so (`cookie`, anything with
  `authorization`, `key`, `token`, `secret` or `password`). The probe of `/.well-known`
  is an ordinary HTTP request and keeps its headers.

A header that carries a secret is better kept in a [catalogue](#server-catalogue) entry,
by reference, than on a command line, which ends up in the shell's history. `retp mcp`,
whose command line is written in a configuration file, refuses one
([MCP server](mcp.md#secrets-never-go-on-the-command-line)).

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

## Server catalogue

A server you talk to every day can be kept under a name: `retp azure` instead of the URL,
the partition, the tenant and the way to log in. The catalogue never holds a secret, only a
reference to one.

```sh
retp catalog init ~/retp-servers          # choose the folder (asked on a terminal; required in a script)
retp server add                           # a guided form, on a terminal
retp server add azure-test wss://adme.example.com/api/reservoir-ddms-etp/v2/ \
    --auth azure --tenant 00000000-0000-0000-0000-000000000000 --client-id APP_ID \
    --scope 'api://APP_ID/.default' --client-secret-from env:RETP_AZURE_TEST_SECRET \
    --header data-partition-id=opendes --alias az

retp server test azure-test               # definition, references, /.well-known, login, session
retp azure                                # a shell on it
retp azure dataspaces                     # a command on it
retp @azure dataspaces                    # the same; @ always means the server
retp --server azure dataspaces            # the same; -s is short
retp shell azure                          # the shell, by name
```

**The folder.** `config.toml` in the OS's configuration folder (`RETP_CONFIG` names another
file) holds the catalogue folder and the default server; `RETP_CATALOG` names the folder
instead. In it, one folder per server: `server.toml`, the definition, which can be shared
(no secret in it), and `state/`, local (the record of `server test`, the history of the
shell; `retp catalog init` writes a `.gitignore` for it).

**A secret is a reference**: `env:NAME` (an environment variable) or `file:PATH` (a file;
`~` and `${HOME}` are expanded). A file loses one final line ending, is read at each
connection, and on Unix is refused beyond mode `0600`. `keyring:`, `cmd:` and `prompt` are
not available yet and are refused by name. A literal value for `password`, `token`,
`client_secret` or `secret` in `server.toml` is refused, naming the key and the line and
never the value. `retp server show` says whether each reference resolves, never what it
holds.

```toml
schema = 1
description = "ADME test tenant"
alias = ["az"]

[server]
etp_url = "wss://adme.example.com/api/reservoir-ddms-etp/v2/"
rest_url = "https://adme.example.com/api/reservoir-ddms/v2"
transport = "etp"          # which one a command uses unless told otherwise
tls = "verify"             # or "skip" (--insecure), said again at every connection
handshake = "native"       # or "browser" (--as-browser)
protected = false          # true: a command that writes asks for confirmation

[auth]
method = "azure"           # none, basic, bearer, oauth-password, oauth-client, azure, cognito
tenant = "00000000-0000-0000-0000-000000000000"
client_id = "11111111-1111-1111-1111-111111111111"
scope = "api://11111111-1111-1111-1111-111111111111/.default"
client_secret = { env = "RETP_AZURE_TEST_SECRET" }

[headers]                  # extra request headers (see below)
data-partition-id = "opendes"   # the data partition of OSDU's Reservoir DDMS
x-trace = "on"
x-api-key = { env = "RETP_AZURE_TEST_KEY" }

[defaults]                 # for a dataspace created on this server: not secrets
legal_tags = ["opendes-public-usa-dataset-1"]
owners = ["data.default.owners@opendes.example.com"]
viewers = ["data.default.viewers@opendes.example.com"]
```

The methods are the ones of [Logging in](#logging-in); the catalogue fills the same
`Choice` and the client decides. Over a catalogue server **the ambient `ETP_*` variables are
ignored**, so that the `ETP_TOKEN` set for one server never goes to another; an entry that
wants one says so (`token = { env = "ETP_TOKEN" }`). The login options (`--password`...)
are refused with a catalogue server, and so is a URL.

**Extra headers** are the [headers the server wants](#headers-a-server-wants), sent with
every request to it. One is written as a literal when it carries nothing secret, or as a
reference; a literal is refused for a name that looks secret (`cookie`, anything with
`authorization`, `key`, `token`, `secret` or `password`). They are checked when the file
is read, by the rules of `--header`: a header the client could not send is refused there,
by name, and so is `data-partition-id` beside `partition` (keep one of the two). A
`--header` on the line is added to the entry's; one the entry already has is refused, not
replaced. The file keeps no order between its headers. **Write defaults** (`legal_tags`, `owners`,
`viewers`) become `--custom legaltags=...`, `owners=...`, `viewers=...` on
`create-dataspace`, unless the line gives its own.

**Naming.** A name is lowercase letters, digits, `.`, `_` and `-` (it becomes a folder
name). Aliases share the namespace. A built-in command always wins: a server cannot be
named like one (the commands are read from the parser), and `retp @name` always means the
server. `retp dataspces` answers with the commands and servers that look like the word.

**The default server.** `retp server default azure` lets `retp dataspaces` run on it
(`server: azure (default)` is said on stderr). It is never used by a command that writes:
`push`, `create-dataspace` and `delete-dataspace` need a server named on the line.

**Protected servers.** With `protected = true`, a command that writes to the server asks
for its name typed at a terminal, or `--yes`; otherwise it is refused. In a shell only
`--yes` confirms.

**Saving what worked.** `retp connect URL --user U --token-url X --save-as azure-test`
saves the connection after the command succeeded. A secret that came from an `ETP_*`
variable is saved as a reference to that variable (shared with every other server: the
output says so); one that came from the command line is **not saved**, and the entry
points at a variable named after it, with the advice to define it. A `--header` follows
the same rule: its value is saved when its name does not say it is a secret, and one whose
name does is saved as a reference to a variable of its own
(`RETP_<SERVER>_HEADER_<NAME>`). A name already taken is refused before the command runs,
unless `--force`.

**The guided form.** `retp server add` with no argument, on a terminal, asks for the
method, the URLs, the method's fields (a secret as a reference, never a value), extra
headers, the OSDU data partition (kept as the header `data-partition-id`), the write
defaults, `protected`, then probes `/.well-known` before saving (the URL alone:
`retp server test` then asks again with the headers of the entry). With arguments the same fields come from the options, and a script is
never asked: when the input is not a terminal and a field is missing, the command refuses
and names it.

`retp-conformance --server NAME` and `retp mcp --server NAME` take a catalogue server the
same way. What `retp server test` records (`state/last.json`) and prints has the entry's
secrets removed from any answer a server repeats; this is a guard, not a proof.

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

`--plan` prints what the push would send and sends nothing: no session is
opened and the server is not reached. Each island in order, with its objects,
arrays, bytes and cycles (objects that reference each other, always sent
together), each object's URI in the package and in the dataspace, then what the
journal leaves out, what is left out and what leaves the package. The flags
that decide the plan count (`--objects`, `--bytes`, `--only`, `--well-known`,
`--json` or `--rest`, `--journal`, read and never written); `--skip-present`
needs the server and is refused. The exit code fails when something would be
left out, as a push's does.

```sh
retp push wss://etp.example.com/etp model.epc demo --plan --objects 50
```

When a push stops on a refusal whose words open-etp-server (the OSDU Reservoir
DDMS server) is documented to use (`{N} Missing reference(s)`, `{N} Orphan
reference(s)`, `{N} Missing array(s)`, `{N} Orphan array(s)`, `Missing array
container(s): ...`), a `cause:` line names it. A server that names the missing
references (open-etp-server from M27: `{N} Missing reference(s): from
<type>(<uuid>) to <type>(<uuid>) using <xpath>; ...`, 25 at most, then `(+k
more)`) gets one `missing (named by the server):` line each. For the count-only
form, `may be missing:` lines list the references leaving the island that
stopped: the client's deduction from what leaves the package, which the server
does not state, and which is wrong when a RESQML 2.0.1 reference names an object
of the package under `obj_` and the server stores it without. Other words get no
cause line.

The stop line carries the ETP error code when the store gave one: a transaction
the store would not open because it answered with an exception reads `stopped
at island 2: refused (ETP error 8): the transaction would not open: ...`. When
its commit is refused, the push rolls its own transaction back before it stops
or splits the island, since some servers (open-etp-server) keep it open and
would refuse the next one; the library itself never rolls back on a failed
commit.

A push that stopped can start again, each with its own flag, all off by default:

- `--journal FILE` records each island in `FILE` (JSON Lines) as it commits;
  the same command run again leaves out what the file records. A journal
  written for another package, another `--only` selection or another dataspace
  is refused by name;
- `--skip-present` leaves out what the dataspace holds already: an object
  listed there under its URI, each of its arrays there with the same
  dimensions. The content is not compared;
- `--split halve` sends an island the store refuses as a whole again in two
  halves, down to one object if need be; `--split single` sends it one object
  per transaction. A cycle of objects is never split; `--split-limit` bounds how
  far (8 by default). The last refusal is printed as before;
- `--reconnect N` opens a new session when the connection falls, with the
  same login, and goes on from the first island not committed, sent again
  whole; after `N` new sessions in a row that fall too, the push stops. The
  pause before the first is `--reconnect-pause` seconds (1 by default), and it
  doubles up to thirty seconds. An island the connection falls on twice in a
  row is halved. Not in the shell, which keeps its own session: there, push
  again with `--journal`;
- `--resume-session`, with `--reconnect`, first asks the server to give the
  lost session back, then opens a new session if it will not. See below.

```sh
retp push wss://etp.example.com/etp model.epc demo --journal model.push.jsonl --split halve --reconnect 3
```

### Asking the server for the lost session (`--resume-session`)

`--resume-session` is an **OSDU extension**, not ETP v1.2: OSDU's
`ResumeSession` (protocol 2400, CoreOSDU), by which a client asks a server to
give back the session whose connection fell, by its `sessionId`. It is **off by
default**, means something only with `--reconnect`, and is refused by name
without it and over `--rest`.

On each reconnection the new connection is opened the whole way (probe, login,
handshake) and its first message is `ResumeSession`. A server that gives the
session back goes on with it; one that refuses (OSDU answers `ENOT_FOUND`,
*Session not found or expired*) or does not speak protocol 2400 is asked a new
session at once, on the same connection. Servers may well not support it: then
the push goes on exactly as with `--reconnect` alone. Either way the
interrupted island is sent again whole, since a resumed session restores what
the session negotiated, not the requests that were in flight. The report says
how each reconnection came back:

```text
reconnection 1: resumed, the server gave the lost session back
reconnection 2: a new session, the lost one was not given back: ENOT_FOUND: Session not found or expired (11)
```

A caution: with OSDU's resume, nothing on the client side tells which rights
the server applies to a resumed session (OSDU issue #232). `retp` always sends
its own credentials on the new connection and relies on nothing of the lost
session's authorization.

```sh
retp push wss://etp.example.com/etp model.epc demo --journal model.push.jsonl --reconnect 3 --resume-session
```

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
| `subscribe <uri> [--to FILE\|console] [--format jsonl\|text] [--append]` | the changes of a context (StoreNotification, protocol 5) sent, as they arrive, to a file (one JSON line each) or to the console; commands go on meanwhile |
| `subscriptions`, `unsubscribe <id>` | what this shell is subscribed to, where each goes and how many arrived; end one |
| `watch <uri>`, `unwatch <uuid>`, `notifications [--wait S]` | store notifications read on demand: `notifications` collects what came |
| `changes-since <dataspace> <time\|watermark>` | what was written and deleted since a time: the catch-up after a cut |
| `channels`, `subscribe-channels`, `unsubscribe-channels`, `channel-data`, `streaming on\|off` | channels (protocols 21 and 1) |
| `reconnect` | close this session and open a new one on the same server, login included (a catalogue server's references are read again) |
| `connect <name>` | change to another server of the catalogue without leaving; the new session is opened before the old one is closed |
| `exit`, `quit` | close the session and leave |

The shell's session listens on its own task (`retp_client::live`, see
[Library usage](client.md)): what the server sends unasked - a subscription's
changes, a fault, the authorization running out, the session closed - is
printed or written as it arrives, while nothing is typed, and nothing polls.
A server that offers no StoreNotification (protocol 5) refuses `subscribe` by
name. Built with the feature `editor`, the shell has a line editor: history
(kept in `~/.retp_history`), editing, and what arrives printed above the prompt
instead of through it; without it, the shell reads plain lines.

ETP URIs need no quotes in the shell: single quotes are kept, so
`eml:///dataspace('demo')` is one word. A dataspace can be named by its path
wherever a URI is asked for. A lost connection is not the end of the shell: a
new session is opened, logging in again, and the command runs once more (every
shell command is a read, and a read can be asked twice).

## Over REST: OSDU's Reservoir DDMS API

`--rest` takes the REST API instead of the WebSocket, with the same commands:

```sh
retp dataspaces https://etp.example.com/api/reservoir-ddms/v2 --rest --header data-partition-id=PARTITION
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
