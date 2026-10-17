# Architecture of retp-python

Why each choice is what it is. It mirrors `energyml-python` (the bridge of
`energyml-rust`) wherever the same question had an answer there.

## The rule

The Python bridge carries no feature that Rust does not have. It is only a
bridge: an operation is written in Rust first, then bridged. The test applied
while writing: if this crate were removed, would anything be lost, apart from
the conversion? The answer must be no.

## Three layers

```
Python         python/retp/*.py          names modules, documents; no code
pyo3 dressing  src/bindings.rs           Python objects <-> session types
plain Rust     src/session.rs            arguments, failures, results; one call
                                         into the Rust library per operation
Rust library   retp-client, energyml-etp-store, energyml-core/schemas/storage
```

- **`session.rs` is plain Rust and holds everything that can be wrong at the
  boundary.** `cargo test` runs it with no interpreter and no server. A pyo3
  binding only runs after a `maturin develop`; code written there is code the
  suite never sees, so it carries none.
- **`bindings.rs` converts and nothing else**, behind the `python` feature. Off
  by default: a workspace member that needs an interpreter would impose one on
  every other member through feature unification.
- **`python/retp/*.py` are thin**: they re-export from `._core` and carry the
  docstrings a user reads. The arrangement lives in Python because that is where
  it can be read; registering submodules from Rust would mean writing to
  `sys.modules` by hand, in code no test reaches.

## Blocking, not async

`retp-client` writes each operation once, as an `async fn` over a `Caller`, and
runs it on the blocking `Client` through `retp_client::blocking`. The bridge
binds that blocking form: no async runtime, no thread of its own. The GIL is
released around each call (`Python::detach`), so other Python threads run while
a request is out. See `DESIGN.md` section 3 for what this costs and what an
asynchronous face would need.

## Objects cross as dictionaries

An EnergyML object has hundreds of fields and a type hierarchy. Exposing it as
Python classes would mean generating a module per release, redoing in Python what
the generated crates do in Rust. A dictionary carrying `$type` is read, edited
and written back without a line knowing a type, and it is the form
`energyml-rust`'s bridge already uses, so the two wheels exchange objects.

The JSON crosses as text and is parsed by Python's own `json`: rebuilding it key
by key through pyo3 would be a second implementation of the conversion.

## Arrays cross by copy, flat

A view would share memory whose lifetime is not Python's: an array kept after
the session closes would read freed memory, and the defect would show weeks
later as wrong numbers. The copy costs one pass; a view would cost the trust. If
it is ever needed on very large arrays it will arrive as a method that says so in
its name.

Flat, with the dimensions beside it, because the dimensions are the server's
statement and `reshape` is one line in numpy.

## Failures are classified once, from the library's own error

`ClientError` is the Rust client's error. `session::Failure::from` maps each
variant to a `Kind`, and `bindings.rs` maps each `Kind` to an exception class.
The library's own words are kept as the message: they are written to be read.
`tests/errors.rs` checks the classification against a second source, the
library's own `is_connection_lost`.

A case the bridge does not support raises `Unsupported` and names it. A list a
server cut short with an error is refused, not returned as if it were whole.

## The token

`ConnectOptions::endpoint` turns the token, or the login the other fields ask
for (`retp_client::login::Choice`, the command line's own, given no environment
to read), into `retp_proto::credentials::Credentials` (whose `Debug` shows the
scheme and hides the rest) and keeps nothing else. `ConnectOptions` has no
`Debug`; `Session` holds no copy; a refusal of a token or a login names the rule
and never echoes the value.

## Dependencies are an allow-list

`tests/scope.rs` reads `Cargo.toml` and goes red if a dependency outside the list
appears: geometry, export, translation and the other EnergyML workshops belong to
`energyml-rust`'s wheel, not to this one. A dependency added by distraction would
bring them into the wheel, the compile time and the public surface.

## The library name

The Rust library is `retp_python`, not `retp`: `retp-cli` ships a binary of that
name, and two targets writing the same `target/debug/deps/retp.pdb` is a Windows
link error (`LNK1201`). Maturin renames the artefact after `module-name`
(`retp._core`), so `import` never sees the Rust name.

## Tests

One binary (`tests/main.rs`, `autotests = false`), as in the rest of the
workspace. None needs a server or an interpreter. The operations added after the
first scope are free functions over any blocking client, so `tests/operations.rs`
runs them on a client whose other end is a server connection in the same process
(`retp_client::memory`, the in-memory transport the client's own tests use).
`tests/live.rs` talks to a real server, is compiled only with the `live-server`
feature and each test is `#[ignore]`.
