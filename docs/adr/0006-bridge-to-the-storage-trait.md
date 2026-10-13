# 6. The bridge: an ETP dataspace behind the storage trait

> `energyml-etp-store` keeps the `energyml` prefix while the other crates are
> `retp-*`, because it is the one crate that names both worlds.

## Context

The `energyml-rust` library reads EnergyML data through one trait,
`EnergymlStorage`, drawn after ETP v1.2 (list resources, read objects, read
arrays). An EPC file implements it today. Everything above the trait (geometry,
export, a viewer) is written for it and never learns where the data lives.

An ETP client that offers its own, different API would force every consumer to
be written twice: once for files, once for servers. The test of the client is
therefore not that it can fetch bytes, but that code written for files runs over
a server without a line changing.

## Decision

- **A bridge crate, `energyml-etp-store`, implements `EtpStore:
  EnergymlStorage` over the client.** It is the acceptance criterion of the
  client: a viewer or a geometry reader written for files must run unchanged
  over a server. It is the only crate that names both the EnergyML storage layer
  and the ETP client; neither of them depends on the other.
- **One trait method, one client exchange.** Listing resources and headers is
  Discovery (a header's title is the resource name; no object is read); reading
  objects is Store followed by the schema registry; arrays are DataArray,
  metadata first, then the whole array or sub-arrays.
- **Locating an object's arrays is not ETP.** The storage crate does it
  (`array::locate_with`); the bridge only translates an array location into an
  ETP identifier. The `file` of a location becomes the URI of whatever carries
  the array: the proxy object for HDF5 datasets, the object itself for an
  external data array part, whose file name means nothing to a server.
- **A failure is said, never an empty answer.** A wholly refused request is a
  remote error, an absent object its own not-found error, a truncated list a
  diagnostic. A server that lacks a protocol gives an error, not an empty list.
- **The client is blocking, the trait asynchronous**: each method holds the
  session for the length of its exchange. A browser brings its own transport.
- **Moving a whole model is written once, for any `Caller`.** `pack` turns a
  dataspace into an EPC and `push` sends any `EnergymlStorage` into a dataspace,
  in islands of transactions; both work through the blocking or the asynchronous
  caller and give the same result. Arrays are not written by the bridge (neither
  this crate nor a browser writes HDF5): they come back beside the archive and
  the caller places them.
- **A push never overwrites what changed behind it** and never separates an
  object from its arrays; the details are in the crate's `ARCHITECTURE.md`.

## Consequences

- The same geometry reader, exporter and viewer run over a file and over a
  server; a test asks exactly that question (prefetch, then geometry, code
  written for files).
- Fixing a rule of the storage trait fixes it for both worlds; there is no
  second place where the rule can drift.
- The price is an adapter: some ETP capabilities that have no counterpart in
  the trait are reached through the client directly, not through the bridge.
- The bridge is tested against a real server `Connection` over an in-memory
  transport, so its tests need no network.
