# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
