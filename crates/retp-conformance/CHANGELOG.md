# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0-alpha.2](https://github.com/geosiris-technologies/retp/compare/retp-conformance-v0.4.0-alpha.1...retp-conformance-v0.4.0-alpha.2) - 2026-11-19

### Added

- *(catalog)* [**breaking**] add otherRelevantDataCountries to the write defaults
- *(client)* [**breaking**] ask the server for the lost session, opt-in (native::Resuming)
- *(client)* [**breaking**] the data partition is a header and nothing else
- *(client)* [**breaking**] the headers a server wants in addition, given once with the endpoint

### Fixed

- *(push)* [**breaking**] roll back a refused commit, keep the code of a refused start, name the missing references
- *(client)* an extra header cannot say its value in an error, a Debug or to another host

### Other

- Merge remote-tracking branch 'origin/dev' into feat/catalog-other-relevant-data-countries
- *(deps)* [**breaking**] move to energyml 0.4.0

## [0.3.0](https://github.com/geosiris-technologies/retp/compare/retp-conformance-v0.2.0...retp-conformance-v0.3.0) - 2026-10-16

### Added

- *(cli)* a catalogue of servers, named, their secrets by reference

### Other

- *(deps)* energyml 0.3.0

## [0.2.0](https://github.com/geosiris-technologies/retp/compare/5248f7e...retp-conformance-v0.2.0) - 2026-10-15

### Added

- *(cli)* retp mcp, one dataspace of an ETP server as MCP tools
- *(etp-store)* [**breaking**] the storage interface of energyml 0.2 on an ETP dataspace
- *(python)* the retp Python bridge, and the release flow of energyml-rust

### Other

- *(deps)* energyml 0.2.0
