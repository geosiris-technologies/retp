# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0](https://github.com/geosiris-technologies/retp/compare/retp-catalog-v0.3.0...retp-catalog-v0.4.0) - 2026-11-19

### Added

- *(catalog)* [**breaking**] add otherRelevantDataCountries to the write defaults
- *(client)* [**breaking**] the data partition is a header and nothing else
- *(client)* [**breaking**] the headers a server wants in addition, given once with the endpoint
- *(client)* renew the authorization of an open session, credentials from a source, and a typed handshake refusal

### Fixed

- *(client)* an extra header cannot say its value in an error, a Debug or to another host

## [0.3.0](https://github.com/geosiris-technologies/retp/compare/retp-catalog-v0.0.1...retp-catalog-v0.3.0) - 2026-10-16

### Added

- *(cli)* a catalogue of servers, named, their secrets by reference

### Fixed

- *(catalog)* a doc comment clippy reads as a list, on Unix only

### Other

- *(catalog)* rustfmt after the clippy fixes
