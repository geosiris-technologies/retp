# Python bridge

> Status: alpha. The page describes what the bridge binds; it has not been run
> against a build yet, and `TODO(verify)` marks what the first build and test
> pass must confirm.

`retp` for Python is the Rust client behind a thin pyo3 layer, built with
maturin. It carries **no functionality the Rust library lacks**: an operation is
written in Rust first and bridged second. When Python needs something Rust does
not offer, the answer is to add it to Rust.

The distribution and the import are both named `retp`. The earlier pure-Python
projects (`etpclient`, `etpproto`, `etptypes` on PyPI) are separate and remain
available.

## Install

TODO(verify): not published to PyPI yet; development builds go to TestPyPI
(see [Releasing](releasing.md)).

```
pip install retp
```

Wheels are `abi3` (one per platform, Python 3.9 or later) and need numpy.

## Connect

```python
import retp

with retp.connect("wss://etp.example.org/", token=token) as session:
    print(session.warnings)        # plain text, an unchecked certificate...
    print(session.ping())          # round trip, seconds
```

| Argument | Meaning |
|---|---|
| `url` | `ws://` or `wss://` URL of the ETP endpoint. A REST URL is refused by name (`retp.Unsupported`); the REST transport is not bridged yet |
| `token` | an OAuth 2.0 bearer token, without its scheme. It is handed to the Rust client and kept nowhere else: not in `repr`, not in `url`, not in an error |
| `partition` | an OSDU data partition: talks to the server as OSDU's Reservoir DDMS |
| `insecure` | do not check the certificate (said in `session.warnings`) |
| `writing` | announce that this session writes (adds the Transaction protocol). A session without it refuses to write, by name |
| `types` | qualified types to ask for (`resqml22.*`); every release when empty, plus what the server announces |
| `keepalive` | ping when the session has been quiet this many seconds; 0 never |
| `at_once` | requests of one operation in flight at once (default 4; 1 sends them one after the other) |

Keep the token out of the command line and out of source files: read it from the
environment or a secret store.

The calls are blocking and release the Python GIL while they wait. From
`asyncio`, `await asyncio.to_thread(session.dataspaces)`. One session handles one
request at a time.

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
```

`scope` is one of the five ETP names: `self`, `sources`, `targets`,
`sourcesOrSelf`, `targetsOrSelf`. A listing that a server cut short with an error
raises `retp.ServerError`; it is not returned as if it were complete.

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
```

The objects are planned by the Rust library: targets before sources, in islands
of transactions, so that a connection lost halfway costs one island and not the
model. If the push did not go through in full, `retp.PushIncomplete` is raised
and its `.report` says what was sent, what was refused, what was left out and
which references leave the batch. If the server agreed no Transaction protocol,
the report says so (`without_transactions`): a refused island may be partly
there.

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
`Epc.write_array`. Downloading a whole dataspace as an EPC is available in the
Rust command line (`download-epc`) and not bridged yet.

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

Writing arrays from numpy is not bridged yet: Rust has no constructor from typed
numbers, and the choice of the logical and transport types belongs there.

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
| `ServerError` | the server answered with an error (`.code`) | depends on the code |
| `TimedOut` | no answer in time | retry or give up |
| `AnswerRefused` | the answer could not be taken | report it |
| `LocalError` | an object or a package could not be read or written locally | fix the data |
| `PushIncomplete` | a push did not go through in full (`.report`) | read the report |

A call never returns an empty result to mean "not supported" or "not found":
plural reads return `(found, errors)` so that nothing is silently missing.

## What is not there yet

Deleting objects and dataspaces, creating dataspaces, explicit transactions,
supported-type listing, notifications and channels, the logins of the command
line (the bridge takes a token), the REST transport, downloading a dataspace as
an EPC, and writing arrays. The Rust library has most of them; each is a small
binding. The list of what Rust must grow first is in the crate's `DESIGN.md`.
