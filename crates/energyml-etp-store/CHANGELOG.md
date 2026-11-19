# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0-alpha.2](https://github.com/geosiris-technologies/retp/compare/energyml-etp-store-v0.4.0-alpha.1...energyml-etp-store-v0.4.0-alpha.2) - 2026-11-19

### Added

- *(etp-store)* [**breaking**] plan a push without a session, and sort why it stopped
- *(etp-store)* the push report says how each new session came back
- *(etp-store)* push::reconnecting, the Retry of "reconnect N times"
- *(etp-store)* [**breaking**] a push that can start again where it stopped
- *(etp-store)* [**breaking**] name every object a stopped island could not put, with the store's code

### Fixed

- *(push)* [**breaking**] roll back a refused commit, keep the code of a refused start, name the missing references
- *(etp-store)* objects held by value, as read from a server

### Other

- say how push islands are numbered, and that OSDU may need otherRelevantDataCountries
- *(etp-store)* an array written again by a resumed push is unverified
- *(deps)* [**breaking**] move to energyml 0.4.0
- *(etp-store)* the comment of the discovery module is back on it
- *(etp-store)* how an object held by value is found, among the rules of the crate

## [0.3.0](https://github.com/geosiris-technologies/retp/compare/energyml-etp-store-v0.2.0...energyml-etp-store-v0.3.0) - 2026-10-16

### Added

- *(etp-store)* [**breaking**] the store times unknown, and pruning apart from listing contents
- *(etp-store)* the conversions between ETP and the storage traits, both ways, in one module

### Fixed

- *(etp-store)* keep the edges with either end among the resources found

### Other

- *(deps)* energyml 0.3.0

## [0.2.0](https://github.com/geosiris-technologies/retp/compare/5248f7e...energyml-etp-store-v0.2.0) - 2026-10-15

### Added

- *(cli)* retp mcp, one dataspace of an ETP server as MCP tools
- *(etp-store)* blocks, declarations, JSON objects, custom data, and the storage contract
- *(etp-store)* [**breaking**] the storage interface of energyml 0.2 on an ETP dataspace
- *(python)* the retp Python bridge, and the release flow of energyml-rust

### Fixed

- *(etp-store)* name the PropertyKind uuid for what it is, and ignore the one false finding left in history

### Other

- *(deps)* energyml 0.2.0
