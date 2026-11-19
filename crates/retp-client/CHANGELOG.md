# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0-alpha.2](https://github.com/geosiris-technologies/retp/compare/retp-client-v0.4.0-alpha.1...retp-client-v0.4.0-alpha.2) - 2026-11-19

### Added

- *(client)* read_regions_with takes the metadata the caller already holds
- *(client)* re-export retp-types, retp-proto and retp-avro
- *(catalog)* [**breaking**] add otherRelevantDataCountries to the write defaults
- *(client)* [**breaking**] ask the server for the lost session, opt-in (native::Resuming)
- *(client)* customData as plain JSON both ways, and a renewal for every transport
- *(client)* [**breaking**] the data partition is a header and nothing else
- *(client)* [**breaking**] the headers a server wants in addition, given once with the endpoint
- *(client)* [**breaking**] Event and ClientError are non-exhaustive
- *(client)* renew the authorization of an open session, credentials from a source, and a typed handshake refusal

### Fixed

- *(client)* bound the HTTP body and the WebSocket message a server can send
- *(push)* [**breaking**] roll back a refused commit, keep the code of a refused start, name the missing references
- *(client)* a quote in a dataspace path is written twice in its URI
- *(client)* an extra header cannot say its value in an error, a Debug or to another host

### Other

- Merge pull request #39 from geosiris-technologies/feat/catalog-other-relevant-data-countries
- Merge pull request #42 from geosiris-technologies/fix/bounded-allocations
- Merge remote-tracking branch 'origin/dev' into feat/catalog-other-relevant-data-countries
- Merge pull request #41 from geosiris-technologies/feat/read-regions-known-metadata
- Merge pull request #40 from geosiris-technologies/feat/client-reexports
- *(client)* do not link a private item from public documentation
- *(deps)* bump tokio-tungstenite from 0.27.0 to 0.30.0
- *(deps)* [**breaking**] move to energyml 0.4.0
- *(client)* a request that carries a secret header follows no redirection
- *(client)* what a credential source promises is said on the trait
- *(client)* renewing at the server's warning with a source of your own, and an example

## [0.3.0](https://github.com/geosiris-technologies/retp/compare/retp-client-v0.2.0...retp-client-v0.3.0) - 2026-10-16

### Added

- *(proto,client,cli)* emit tracing events rather than log records
- *(cli,python)* subscribe to notifications that arrive as they come
- *(client)* a live session that pushes what arrives unasked, without polling

### Other

- *(deps)* energyml 0.3.0

## [0.2.0](https://github.com/geosiris-technologies/retp/compare/5248f7e...retp-client-v0.2.0) - 2026-10-15

### Added

- *(cli)* retp mcp, one dataspace of an ETP server as MCP tools
- *(etp-store)* blocks, declarations, JSON objects, custom data, and the storage contract
- *(etp-store)* [**breaking**] the storage interface of energyml 0.2 on an ETP dataspace
- *(python)* the retp Python bridge, and the release flow of energyml-rust

### Other

- *(deps)* energyml 0.2.0
