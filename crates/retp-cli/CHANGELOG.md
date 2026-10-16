# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0](https://github.com/geosiris-technologies/retp/compare/retp-cli-v0.2.0...retp-cli-v0.3.0) - 2026-10-16

### Added

- *(cli)* a catalogue of servers, named, their secrets by reference
- *(proto,client,cli)* emit tracing events rather than log records
- *(cli,python)* subscribe to notifications that arrive as they come

### Other

- *(deps)* energyml 0.3.0
- the server catalogue in the crate list, conformance, MCP, Docker, the architecture and the publication order

## [0.2.0](https://github.com/geosiris-technologies/retp/compare/5248f7e...retp-cli-v0.2.0) - 2026-10-15

### Added

- *(cli)* retp mcp, one dataspace of an ETP server as MCP tools
- *(etp-store)* [**breaking**] the storage interface of energyml 0.2 on an ETP dataspace
- *(python)* the retp Python bridge, and the release flow of energyml-rust

### Other

- *(deps)* energyml 0.2.0
