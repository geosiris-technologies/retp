# The ETP v1.2 Avro schemas

212 `.avsc` files, the official ones from Energistics' `etpv1` repository (tag
`v1.2`, commit `f94c3f9e`), under `Energistics/Etp/v12/{Datatypes,Protocol,PrivateProtocols}/…`.
Apache-2.0: see `License.txt`. Where they come from, the archive hash and what
was taken: `PROVENANCE.md`.

They are the **source** the ETP type generator (`generate --etp`) reads, as
`rc/energyml-xsd/` is for the EnergyML types. Nothing here is written by hand
and nothing should be: to update, take a new tag of `etpv1`, replace the
files, and update `PROVENANCE.md`. The generator reads every `.avsc` under this
directory and ignores the paths.

| kind | count |
|---|---|
| `record` (59 data types, 138 messages carrying `protocol` and `messageType`) | 197 |
| `enum` | 14 |
| `fixed` (`Uuid`) | 1 |

Four of the enumerations (`Protocol`, `EndpointCapabilityKind`,
`ProtocolCapabilityKind`, `DataObjectCapabilityKind`) are referenced by no
record: they are the registries of protocol numbers and of capability keys.

The previous copy, extracted from `etp-rs`, was compared with these and found
identical on the 208 types it held; see `doc/suivi/etp-avsc-comparaison.md`.

## OSDU's extensions

`OSDU/` holds two more `.avsc`, not Energistics': OSDU's extension of Core,
`CoreOSDU` (protocol 2400, `ResumeSession` and its response), taken from the
`etp-1.2.avpr` of OSDU's `open-etp-server` (Apache-2.0: `OSDU/LICENSE`). Where
they come from, the file hash and how they were written out:
`OSDU/PROVENANCE.md`. The generator reads them as it reads the others (they
become `protocol::core_osdu`); the counts above are Energistics' alone.

## What the schemas do not carry

Three files are ours. They come from the ETP v1.2 specification, not from a
schema, and their licence is not established (a question for Energistics).

- [`error-codes.json`](error-codes.json): `ProtocolException` carries a bare
  integer, so the 37 error codes are in no `.avsc`. Taken from chapter 24 of the
  specification (pp. 482-486). It is a declared table and can be wrong; the
  test `error_enum_matches_the_declared_table` guards it. Since v1.2 any code
  may appear in any protocol (§24.1).
- [`correlation.json`](correlation.json): which message answers which. Taken
  from the *Correlation Id Usage* row of each of the 124 messages the
  specification describes (private protocol 2100 is not in it), and checked
  against the schemas by `retp-tests/tests/correlation.rs`. Three
  message names the specification uses do not exist; they are listed under
  `errata`.
- [`capabilities.json`](capabilities.json): type, bounds and default of each
  capability, from §3.3, §4.3 and chapter 23. It can now be cross-checked
  against `EndpointCapabilityKind`, `ProtocolCapabilityKind` and
  `DataObjectCapabilityKind`, which are in the schemas.
