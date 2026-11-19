# Python bridge

`retp` for Python is the Rust client behind a thin pyo3 layer, built with
maturin. It carries **no functionality the Rust library lacks**: an operation is
written in Rust first and bridged second. When Python needs something Rust does
not offer, the answer is to add it to Rust.

The distribution and the import are both named `retp`. The earlier pure-Python
projects (`etpclient`, `etpproto`, `etptypes` on PyPI) are separate and remain
available.

## Install

```
pip install retp
```

`retp` is on PyPI since version 0.2.0 (2026-10-15). Wheels are `abi3` (one per
platform, Python 3.9 or later), carry HDF5, and need numpy. Development builds
go to TestPyPI (see [Releasing](releasing.md)).

## Connect

```python
import retp

with retp.connect("wss://etp.example.org/", token=token) as session:
    print(session.warnings)        # plain text, an unchecked certificate...
    print(session.ping())          # round trip, seconds
```

| Argument | Meaning |
|---|---|
| `url` | `ws://` or `wss://` URL of the ETP endpoint; with `rest=True`, the `http://` or `https://` URL of the Reservoir DDMS REST API. An HTTP URL without `rest=True` is refused (`ValueError`) |
| `token` | an OAuth 2.0 bearer token, without its scheme. It is handed to the Rust client and kept nowhere else: not in `repr`, not in `url`, not in an error |
| `partition` | an OSDU data partition, for OSDU's Reservoir DDMS: sent as the header `data-partition-id`. The other headers a server may want are not bridged yet |
| `insecure` | do not check the certificate (said in `session.warnings`) |
| `writing` | announce that this session writes (adds the Transaction protocol). A session without it refuses to write, by name |
| `types` | qualified types to ask for (`resqml22.*`); every release when empty, plus what the server announces |
| `keepalive` | ping when the session has been quiet this many seconds; 0 never |
| `at_once` | requests of one operation in flight at once (default 4; 1 sends them one after the other) |
| `rest` | talk to the Reservoir DDMS REST API instead of an ETP WebSocket (see below) |
| `user`, `password`, `token_url`, `azure_tenant`, `cognito_region`, `client_id`, `client_secret`, `oauth_scope` | the other logins of the command line, when no `token` is given (see below) |

Keep the token out of the command line and out of source files: read it from the
environment or a secret store.

The calls are blocking and release the Python GIL while they wait. From
`asyncio`, `await asyncio.to_thread(session.dataspaces)`. One session handles one
request at a time: a second thread that calls it while a request is out gets
`retp.SessionBusy` at once, and nothing is sent. Open one session per thread.
(Inside one call the library already keeps several requests in flight: see
`at_once`.)

### Renewing the token

```python
session.reauthorize(new_token)
```

Over ETP, an `Authorize` on the open session (ETP v1.2 section 3.13.5): the
session stays the same one. Do it before the token the server holds runs out; a
server that is not told may close the session when it does, and nothing brings
it back. A refusal raises `retp.AuthorizationNeeded`, and the session is still
open. Over REST there is no session to renew: every request from then on
carries the new token. When to renew is the caller's: it knows when its token
changes.

### Logins

Without a token, the session logs in as the command line does
(`retp_client::login`), and the same fields choose the same login, in this
order: Cognito (`cognito_region`, with `client_id`, `user` and `password`); a
token endpoint (`token_url`, or Azure AD's for `azure_tenant`), with the
password grant when `user` is given and client credentials (`client_id`,
`client_secret`) otherwise; HTTP Basic for `user` and `password` alone.

```python
import os

session = retp.connect(
    url,
    token_url="https://login.example.org/oauth2/token",
    client_id=os.environ["ETP_CLIENT_ID"],
    client_secret=os.environ["ETP_CLIENT_SECRET"],
)
```

The bridge reads no environment variable itself: pass what `os.environ` holds.
A login that lacks a part (a user without a password) raises `ValueError`
before any request; an authorization server that says no raises
`AuthorizationNeeded`; one that cannot be reached raises `SessionRefused`; a
step this client does not take (a Cognito challenge such as a new password)
raises `Unsupported`. No secret is kept, and none is in a message.

### The Reservoir DDMS REST API

```python
with retp.connect("https://rddms.example.org/api/reservoir-ddms/v2",
                  token=token, partition="opendes", rest=True) as session:
    print(session.dataspaces())
```

The same methods run over REST (`native::rest_client`). Nothing is opened: each
request goes on its own. What only an ETP session has is refused by name:
`keepalive` and `types` at `connect` (`ValueError`), notifications at
`subscribe` (`Unsupported`), and what the REST API has no counterpart for, as
the Rust REST client refuses it.

## Dataspaces and resources

```python
for space in session.dataspaces():
    print(space["path"], space["uri"])
    print(space["custom_data"].get("viewers"))     # an OSDU store's ACLs, as lists

listed = session.resources("demo")                 # a path, or a URI
for resource in listed["resources"]:
    print(resource["uri"], resource["name"])

# Which way, how far, which types, with edges and counts:
session.resources("demo", depth=2, scope="targets", types=["resqml22.*"],
                  edges=True, count=True)

# The types the dataspace holds, counted (scope "targets" or "sources"):
for kind in session.supported_types("demo", count=True):
    print(kind["data_object_type"], kind["object_count"])
```

`scope` is one of the five ETP names: `self`, `sources`, `targets`,
`sourcesOrSelf`, `targetsOrSelf`; `supported_types` takes `targets` or
`sources` only (ETP section 22.3.1), and refuses the others before sending
(`NotSent`). `supported_types(..., empty=True)` also lists the types the store
supports and holds none of. A listing that a server cut short with an error
raises `retp.ServerError`; it is not returned as if it were complete.

Each dataspace carries its `custom_data` as plain values: a string, a number, a
flag, a list of strings (OSDU's `owners`, `viewers`, `legaltags`, and on some deployments
`otherRelevantDataCountries`). A value of
another kind is not turned into something else: its key is named in the
dataspace's `notes`.

To create and delete dataspaces, open the session with `writing=True`:

```python
with retp.connect(url, token=token, writing=True) as session:
    created, errors = session.put_dataspaces(
        ["demo", "carl/teapot"],
        custom_data={
            "owners": ["data.default.owners@example.org"],
            "viewers": ["data.default.viewers@example.org"],
            "legaltags": ["opendes-public-usa-dataset-1"],
            "otherRelevantDataCountries": ["US"],  # only where a deployment requires it
        },
    )
    deleted, errors = session.delete_dataspaces(["eml:///dataspace('old')"])
```

Each dataspace is a path or a URI. `custom_data` goes with each (an OSDU store
reads its ACLs and legal tags there): a list of strings goes as that list; a
string is read as the command line's `--custom KEY=VALUE` reads it, a text with a
comma a list of strings and any other text one string. Any other value is
refused (`ValueError`). Each dataspace answers for itself: the ones the store took
are in the list, in the order given, the others in `errors`
(`{"kind", "code", "message"}`).

## Objects

Objects cross as **dictionaries** in the EnergyML JSON encoding: every node
carries its `$type`. There are no generated classes; the same convention as the
`energyml.rust` bridge, so an object read here can be put there.

```python
obj = session.get_object(uri)
print(obj["$type"], obj["Citation"]["Title"])

objects, errors = session.get_objects(uris)        # a URI that failed is in errors
for uri, why in errors.items():
    print(uri, why["code"], why["message"])
```

Objects are asked from the server as XML and read with the schemas the wheel
carries. A server that answers in another format is refused for that object, by
name (`errors[uri]["kind"] == "Unsupported"`).

To write, open the session with `writing=True`:

```python
with retp.connect(url, token=token, writing=True) as session:
    report = session.put_objects("demo", [obj1, obj2])   # dicts, or JSON text
    print(report["sent"], "of", len(report["islands"]), "islands committed")

    deleted, errors = session.delete_objects([uri1, uri2])
    for uri, with_it in deleted.items():
        print(uri, "took", with_it)                      # contained objects included
```

The objects are planned by the Rust library: targets before sources, in islands
of transactions, so that a connection lost halfway costs one island and not the
model. If the push did not go through in full, `retp.PushIncomplete` is raised
and its `.report` says what was sent, what was refused, what was left out and
which references leave the batch. If the server agreed no Transaction protocol,
the report says so (`without_transactions`): a refused island may be partly
there.

`report["refusals"]` names **every** object or array of the island that stopped
that the store refused, each `{"uri", "array", "code", "message"}`: `uri` the
object in the destination dataspace, `array` the array's path when the refusal
is an array's, `code` the ETP error code the store answered with (`None` when
the refusal is this side's, an array that could not be read). A
`ProtocolException` that answered the whole request is every object's answer.
`report["stopped"]` quotes the first in a sentence; read the list, not the
sentence. Each refusal also carries `"cause"`, what its words name (below).

`report["stop"]` says why the push stopped, sorted for a program to act on:
`None` when every island went through, or

```python
{
    "island": 3,              # its rank in report["islands"]
    "kind": "refused",        # "refused", "failed", "timeout", "gave_up" or "journal"
    "message": "2 Missing reference(s)",  # a refusal: the store's own words
    "code": None,             # the ETP error code, when there is one
    "cause": {"kind": "missing_reference", "count": 2, "space": None},
    "candidates": [(from_uri, to_uri), ...],
}
```

- `kind`: `timeout` is a request with no answer in time (the session may still
  stand); `failed` is a connection that fell or a session that closed;
  `gave_up`, a push that reconnected as many times as allowed; `journal`, a
  journal that would not serve;
- `message`: for a refusal, the store's words **alone**, without the hint
  `report["stopped"]` may add about the references that leave the package;
- `cause` (a refusal only, `None` otherwise) is read from those words, and only
  from the messages open-etp-server (the OSDU Reservoir DDMS server) is
  documented in its sources to use at commit or on a write outside a
  transaction: `missing_reference` (`count`, and `space` outside a
  transaction), `orphan_reference`, `missing_array`, `orphan_array` (with
  `count`), `missing_array_container` (with `containers`). Any other words are
  `{"kind": "unknown"}`: nothing is guessed for another server;
- `candidates`, for a missing reference: the `report["dangling"]` pairs whose
  `from` is in the island that stopped. **This is the client's deduction, not
  the server's statement** (the server counts the missing references and names
  none): every target the package holds is sent before what references it, so
  a reference from that island can only miss when its target leaves the
  package, and every such reference is in `dangling`, `only=` included. A target
  already in the dataspace is not missing, so there may be more candidates than
  the server counts.

`report["stopped"]` is unchanged, for the code that reads it.

### The plan, without a session

`retp.plan_push` says what `push_epc` would send, with no session and nothing
written: the package is read, written and weighed as the push does it, and
planned by the same code (`energyml_etp_store::push::dry_run`).

```python
plan = retp.plan_push("model.epc", "demo", objects_per_island=50, journal="model.push.jsonl")
for rank, island in enumerate(plan["islands"]):
    print(rank, island["objects"], island["arrays"], island["bytes"], island["cycles"])
    for source_uri, there in zip(island["uris"], island["destinations"]):
        print("   ", source_uri, "->", there)
print(plan["left_out"], plan["dangling"], plan["skipped"])
```

Its keywords are `push_epc`'s, with the same defaults, for what decides the
plan: `objects_per_island`, `bytes_per_island`, `only`, `well_known`, `json`,
`journal`. The dataspace is required: the destination URIs are in it, and a
journal is kept for one dataspace. A journal is read, never created nor
repaired; what it records is left out, in `plan["skipped"]`. `skip_present`
needs the server and raises `ValueError`.

Each island is a row of `report["islands"]` (`"state"` is `"planned"`), with
`"destinations"`: its objects' URIs in the dataspace, in the order of `"uris"`
(the package's). The rows of a push's report and of a plan both carry
`"objects"`, `"units"` (one object, or one cycle: a unit of more than one
object is a cycle) and `"cycles"` (the URIs of each cycle). A cycle is always
sent whole, whatever the budget. A push follows the plan unless the store
refuses an island and `split=` cuts it, or the connection keeps falling on one.

`put_objects` and `push_epc` take the command line's `push-epc` options:

| Keyword | Meaning |
|---|---|
| `objects_per_island`, `bytes_per_island` | at most this many objects, this many bytes, in one island (`--objects`, `--bytes`); a cycle larger than that still goes whole |
| `only` | these objects alone, each by URI (of the package, or of the same object in another dataspace) or by uuid (`--only`); a name the source does not hold is refused |
| `well_known` | also push the official dictionary's `PropertyKind`s the objects reference without holding (`--well-known`) |
| `one_by_one` | the arrays one per message, one after the other (`--one-by-one`) |
| `json` | send JSON Energistics, what the Reservoir DDMS REST API takes (`--json`); XML otherwise |
| `skip_present` | leave out what the dataspace holds already: an object listed there under its URI, each of its arrays there with the same dimensions (`--skip-present`); the content is not compared |
| `journal` | a file (a `str` or a path) where each island is recorded as it commits; a push given the same file leaves out what it records (`--journal`) |
| `split` | `"halve"` or `"single"`: an island the store refuses as a whole is sent again in two halves, or one object per transaction (`--split`); `None`, the default, stops at it |
| `split_limit` | how many times the pieces of one island may be split again, 8 by default (`--split-limit`) |
| `reconnect` | when the connection falls, open a new session and go on from the first island not committed, at most this many times in a row (`--reconnect`); 0, the default, stops at the fall |
| `reconnect_pause` | seconds before the first new session, 1 by default, doubling after each fall in a row up to 30 (`--reconnect-pause`) |
| `resume_session` | with `reconnect`: on each new connection, first ask the server to give the lost session back (OSDU's `ResumeSession`, an OSDU extension a server may not support), then open a new session if it will not (`--resume-session`); `False` by default, refused without `reconnect` and over REST |

The client does not filter the objects by the types the server announced: what
the server will not take, it refuses, and the refusal is in the report.

To name an object, `retp.uri` builds its canonical URI (ETP v1.2 section
25.3.7), a quote in the dataspace written twice:

```python
retp.uri("osdu/project", "resqml20.obj_HorizonInterpretation", uuid)
# "eml:///dataspace('osdu/project')/resqml20.obj_HorizonInterpretation(<uuid>)"
```

`delete_objects` answers per URI: `deleted` maps each URI the store deleted to
the URIs it says went with it, `errors` the ones it did not delete.
`prune_contained=True` asks the store to drop the contained objects the
deletion would orphan; a store that refuses to orphan them answers that URI with
its error (`ENOCASCADE_DELETE`, 4003).

### Starting again where a push stopped

The last seven keywords of the table are off by default: a push goes as it
always went. A loading job that may be interrupted, or that pushes into a store
that refuses large transactions, can ask for each:

```python
report = session.push_epc(
    "model.epc", "demo",
    journal="model.push.jsonl",   # what committed is recorded, and left out next time
    skip_present=True,            # what the dataspace holds already is left out
    split="halve",                # a refused island is sent again in halves
    reconnect=3,                  # a fallen connection is reopened, three times in a row at most
)
for skipped in report["skipped"]:
    print(skipped["uri"], skipped["why"])          # "present" or "journal"
for split in report["splits"]:
    print(split["island"], split["into"], split["why"], split["refusals"])
print(report["reconnections"], "new session(s)")
for came in report["reopened"]:
    print("resumed" if came["resumed"] else "new session", came["refused"])
```

- **What is already there** (`skip_present`) is judged by the URI and the
  arrays' dimensions, never by the content: a store rewrites what it is given,
  so two writings of one object do not compare, and an object changed in the
  package since it was sent goes unseen. A listing the store cuts short stops
  the push by name before anything is written.
- **The journal** is JSON Lines: a first line naming the dataspace and a
  fingerprint of what is sent, then one line per committed island with its
  objects' URIs in the destination (the format is in the Rust documentation
  of `energyml_etp_store::push::journal`). A journal written for another
  dataspace, another package, or another selection of it (`only`) is refused
  by name: use a new file. A last line cut while it was written is dropped,
  and its island sent again.
- **A split** never cuts a cycle of objects that reference each other. The
  refusal that caused it is in `report["splits"]`; the one the push stopped
  on, if any, in `report["refusals"]` as before.
- **A reconnection** opens the session again the whole way (probe,
  handshake, authorization) with the credentials `connect` made, or the last
  token `reauthorize` accepted, and sends the interrupted island again whole:
  writing an object again replaces it. An island the connection falls on twice
  in a row is halved. When the connection still falls after `reconnect` new
  sessions in a row, the push stops and says so (`report["stopped"]`); the
  island's outcome is then unknown, and a next run with the journal sends that
  island again and nothing committed before it. The new session replaces the
  old one on the `Session`:
  subscriptions of the old session end with it, and a session opened by a
  login at a token endpoint reuses the token obtained then - renew it with
  `reauthorize` before it runs out.
- **Asking for the lost session** (`resume_session=True`, with `reconnect`)
  is an **OSDU extension**, OSDU's `ResumeSession` (protocol 2400), off by
  default. Each new connection is opened the whole way, with the same
  credentials, and first asks the server to give the lost session back by its
  `sessionId`. A server that refuses (`ENOT_FOUND`, *Session not found or
  expired*) or does not speak 2400 is asked a new session at once, and the push
  goes on as with `reconnect` alone; many servers do not support it. The
  interrupted island is sent again whole either way: a resumed session
  restores what was negotiated, not the requests in flight.
  `report["reopened"]` has one `{"resumed", "refused"}` per new session:
  `resumed` is `True` when the server gave the session back, and `refused`
  holds why it did not, when it was asked (`None` otherwise). A caution: with
  OSDU's resume, nothing on the client side tells which rights the server
  applies to a resumed session (OSDU issue #232); the client always sends its
  own credentials on the new connection.

### Transactions

`put_objects` and `push_epc` open their own transactions. For other writes, a
transaction can be held by hand:

```python
with retp.connect(url, token=token, writing=True) as session:
    transaction = session.start_transaction(["demo"], message="clean-up")
    try:
        session.delete_objects(uris)
    except BaseException:
        session.rollback_transaction(transaction)
        raise
    session.commit_transaction(transaction)
```

`start_transaction` returns a `retp.Transaction` (`uuid`, `dataspaces`,
`read_only`); only `commit_transaction` or `rollback_transaction` ends it. A
store that will not open one raises `ServerError` with its reason; a store
without transactions raises `Unsupported`. A refused commit raises
`ServerError`, and then the writes of the transaction are **not** in the store;
a commit whose connection fell raises `OutcomeUnknown`.

### From `energyml.rust` to a dataspace

```python
from energyml.rust.epc import Epc
import retp

epc = Epc.open("model.epc")
# ... edits ...
epc.save_as("model.epc")

with retp.connect(url, token=token, writing=True) as session:
    session.push_epc("model.epc", "demo")      # objects and arrays, planned in Rust
```

The two wheels are separate native modules and cannot share a Rust object, so
the hand-off is the file. To send some objects only, without arrays:
`session.put_objects("demo", [epc.get(uri) for uri in uris])`.

### From a dataspace to `energyml.rust`

```python
objects, errors = session.get_objects(uris)
for obj in objects.values():
    epc.put(obj)
```

Objects only. Arrays follow by reading each with `read_array` and writing it with
`Epc.write_array`.

### A dataspace as an EPC

```python
packed = session.download_epc("demo")              # types=, uris=, arrays=, references=
with open("demo.epc", "wb") as out:
    out.write(packed["epc"])
for array in packed["arrays"]:                     # what the package's HDF5 files hold
    print(array["file"], array["path"], array["dimensions"], array["values"].dtype)
for said in packed["left_out"]:
    print("left out:", said)
```

`download_epc` is `energyml_etp_store::pack::pack`: the dataspace's objects in
one package, in memory. `types` and `uris` narrow what is taken; `references`
also takes what the kept objects reference, while the dataspace holds it;
`arrays=False` reads no array. The arrays come back beside the package, each with
the HDF5 file the package names for it, its path in that file, its dimensions
and its values (a flat numpy array): writing them is the caller's (h5py, or
`energyml.rust`), as the command line's `download-epc` writes them with its own
HDF5 backend. `left_out` says what could not be taken (an object that does not
read, an array the store refused): a package thinner than the dataspace is said,
never silent.

## Arrays

An object does not contain its numbers: it names arrays, by a path inside the
resource. Take the path from the object.

```python
info, errors = session.array_metadata(uri, [path])
print(info[0]["dimensions"], info[0]["logical"])

values, dimensions = session.read_array(uri, path)
points = values.reshape(dimensions)            # flat, row-major, in the logical type

# One block (ETP's subarray): one entry per dimension
block, dims = session.read_array(uri, path, start=[0, 0], count=[10, 3])
```

`values` is a numpy array **copied** out of the Rust buffer, in the array's
logical type (`float64`, `int32`, `uint8`...), flat. Why flat and why copied is in
the crate's `ARCHITECTURE.md`. String arrays, and bytes of a type without a fixed
element size, are refused by name (`retp.Unsupported`); they are not returned as
an empty array.

Writing arrays from numpy is not bridged yet: the one conversion from numbers to
what the wire carries (`energyml_etp_store::push::to_wire`, where the logical
and transport types are chosen) is not public in Rust.

## Channels

```python
records, errors = session.channel_metadata([log_uri])
for uri, record in records.items():
    print(record["id"], record["channelName"], record["uom"])

items = session.get_ranges([{
    "channelIds": [record["id"]],
    "interval": {"startIndex": {"item": {"long": 0}},
                 "endIndex": {"item": {"long": 1000}},
                 "uom": "m", "depthDatum": ""},
    "secondaryIntervals": [],
}])
for item in items:
    print(item["channelId"], item["indexes"], item["value"])
```

`channel_metadata` asks ChannelSubscribe for the channels at some URIs (a log, a
channel set, a channel); `get_ranges` for their values over index intervals.
Records, ranges and data items cross in the ETP Avro JSON encoding the Rust
library writes (`retp_avro`, ETP section 3.8.1): camel-case field names, and a
union as a one-entry object naming its branch (`{"long": 0}`). A range that does
not read as a `ChannelRangeInfo` raises `ValueError` naming it.

Subscribing to channels, and ChannelStreaming, are not bridged yet (see below).

## Failures

All exceptions derive from `retp.RetpError`, apart from `ValueError` for a wrong
argument.

| Exception | Meaning | What to do |
|---|---|---|
| `Unsupported` | the case is not supported; the message names it | use another route |
| `SessionRefused` | the session could not be opened (also a refused WebSocket upgrade: the message gives its HTTP status) | change the offer, the credential or the server |
| `AuthorizationNeeded` | the server wants credentials | log in again |
| `ConnectionLost` | the connection is gone | open a new session |
| `OutcomeUnknown` | the connection fell while a request was out (a `ConnectionLost`) | check before retrying a write |
| `NotSent` | not sent: the library would see the server refuse it | fix the request; the session is untouched |
| `SessionBusy` | not sent: another thread is using the session (a `NotSent`) | one session per thread, or wait |
| `ServerError` | the server answered with an error (`.code`, when it sent one) | depends on the code |
| `TimedOut` | no answer in time | retry or give up |
| `AnswerRefused` | the answer could not be taken | report it |
| `LocalError` | an object or a package could not be read or written locally | fix the data |
| `PushIncomplete` | a push did not go through in full (`.report`) | read the report |

A call never returns an empty result to mean "not supported" or "not found":
plural reads, deletions and dataspace writes return a pair, what went through
and `errors`, so that nothing is silently missing; a push raises
`PushIncomplete` when it did not go through in full.

## Notifications

`Session.subscribe(uri, ...)` subscribes to the changes of a context
(StoreNotification) and sends them, as they arrive, where `to` says: a path (one
JSON line each), a callable (called with a dict, on a thread of its own), or
nothing - then iterate the subscription. The session goes on answering requests
meanwhile, and nothing polls: it is driven by a task of its own.

```python
with retp.connect(url, token=token) as session:
    with session.subscribe("eml:///dataspace('demo')") as changes:
        for change in changes:          # or changes.get(timeout=5.0)
            print(change["message"], change["body"])

    session.subscribe("eml:///dataspace('demo')", to="changes.jsonl")
    session.subscribe("eml:///dataspace('demo')", to=lambda change: print(change["message"]))
```

A server that offers no StoreNotification refuses by name (`NotSent`). Each
change is the JSON object the Rust library writes (`retp_client::live::as_json`):
`received`, `protocol`, `message`, `subscription`, `body`. A REST session sends
no notifications: `subscribe` raises `Unsupported`.

## What is not there yet

Each of these waits for the Rust library first; the crate's `DESIGN.md`
(section 9) says what Rust must grow:

- writing arrays from numpy (the conversion to the wire is not public);
- subscribing to channels and ChannelStreaming (Rust has no call that sets the
  route and subscribes in one step, as `LiveSession::subscribe_to` does for
  notifications);
- writing a downloaded dataspace to disk with its HDF5 files in one call (the
  command line does it itself; the library returns the package in memory).

Listening to the session's own events (the server's warning that the token is
expiring among them) is not bridged yet: `Session.subscribe` carries store
notifications only, so a Python caller renews on its own clock
(`session.reauthorize`). Nor is a credential source that the library calls
when it renews: the Rust library has both (see [Library usage](client.md)).
