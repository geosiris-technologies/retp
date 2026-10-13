# etptypes

**This crate has moved to [`retp-types`](https://crates.io/crates/retp-types).**
Version 2.0 only re-exports it, so that existing dependents keep building.

`etptypes` 1.x held the Energistics Transfer Protocol (ETP) v1.2 types of an
early proof of concept (`etp-rs`). That repository is now
[`retp`](https://github.com/geosiris-technologies/retp), a complete ETP v1.2
stack: Avro runtime, generated types, protocol and session, client, a bridge to
the [energyml-rust](https://github.com/geosiris-technologies/energyml-rust)
storage trait, and a conformance checker.

Migrating:

```toml
# before
etptypes = "1"
# after
retp-types = "0.1"
```

The types of 2.0 are not those of 1.x: the 1.x line stays available on
crates.io for code that cannot move yet.

Licence: Apache-2.0. The ETP schemas are Copyright Energistics Consortium,
Inc., Apache-2.0; see `NOTICE`. Not affiliated with or endorsed by Energistics
or The Open Group. https://www.geosiris.com
