# retp (Python)

An [ETP v1.2](https://www.energistics.org/) client for Python, over the Rust
`retp` stack: open a session with an ETP server (or the Reservoir DDMS REST
API), list its dataspaces, resources and supported types, read, write and
delete EnergyML objects, create and delete dataspaces, hold transactions, read
data arrays and channel values into Python, subscribe to notifications, and take
a dataspace as an EPC.

```python
import retp

with retp.connect("wss://etp.example.org/", token=token) as session:
    for space in session.dataspaces():
        print(space["path"], space["uri"])

    listed = session.resources("demo")
    objects, errors = session.get_objects([r["uri"] for r in listed["resources"]])

    values, dimensions = session.read_array(uri, "/RESQML/.../points")
    points = values.reshape(dimensions)
```

**It is only a bridge.** Every operation is a call into the Rust library
(`retp-client`, `energyml-etp-store`); the Python layer converts arguments and
results and carries no feature the Rust library does not have. What Python
needs and Rust lacks is added in Rust first.

**Objects are dictionaries**, in the EnergyML JSON encoding: every node carries
its `$type`. No generated classes. This is the same convention as
`energyml-rust`'s Python bridge (`energyml.rust`), so an object read here can be
put there, and the other way round.

**Arrays are numpy arrays**, copied out of the Rust buffer, flat and row-major,
with the dimensions beside them.

**A failure is named.** Exceptions derive from `retp.RetpError` and tell apart
what a caller does differently: reconnect (`ConnectionLost`), log in again
(`AuthorizationNeeded`), check before retrying a write (`OutcomeUnknown`), give
up on a feature (`Unsupported`). A call never returns an empty result to mean
"not supported".

**No credential is stored**: the token, or the password and client secret of a
login, is handed to the Rust client and is in no `repr`, no attribute and no
error message.

See `DESIGN.md` for the scope, the decisions still open, and what the Rust API
must grow before the bridge can offer more. See `ARCHITECTURE.md` for why the
code is shaped as it is, and `docs/python.md` (at the root of the repository)
for the user guide.

## Install

```
pip install retp
```

On PyPI since version 0.2.0. From a checkout of the repository (needs the HDF5
C library, or add `hdf5-static` to the features for CMake and a C compiler
instead):

```
pip install maturin
cd crates/retp-python
maturin develop
```

## License

Apache-2.0.
