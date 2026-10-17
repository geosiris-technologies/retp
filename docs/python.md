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
| `partition` | an OSDU data partition: talks to the server as OSDU's Reservoir DDMS |
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
request at a time.

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

To create and delete dataspaces, open the session with `writing=True`:

```python
with retp.connect(url, token=token, writing=True) as session:
    created, errors = session.put_dataspaces(
        ["demo", "carl/teapot"],
        custom_data={"owners": "a@example.org,b@example.org", "legaltags": "public"},
    )
    deleted, errors = session.delete_dataspaces(["eml:///dataspace('old')"])
```

Each dataspace is a path or a URI. `custom_data` goes with each, every value read
as the command line's `--custom KEY=VALUE` reads it: a text with a comma is a
list of strings, any other text one string (an OSDU store reads its ACLs and
legal tags there). Each dataspace answers for itself: the ones the store took
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

`delete_objects` answers per URI: `deleted` maps each URI the store deleted to
the URIs it says went with it, `errors` the ones it did not delete.
`prune_contained=True` asks the store to drop the contained objects the
deletion would orphan; a store that refuses to orphan them answers that URI with
its error (`ENOCASCADE_DELETE`, 4003).

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
| `SessionRefused` | the session could not be opened | change the offer or the server |
| `AuthorizationNeeded` | the server wants credentials | log in again |
| `ConnectionLost` | the connection is gone | open a new session |
| `OutcomeUnknown` | the connection fell while a request was out (a `ConnectionLost`) | check before retrying a write |
| `NotSent` | not sent: the library would see the server refuse it | fix the request; the session is untouched |
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
