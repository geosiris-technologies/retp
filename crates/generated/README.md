# Generated crates

The code in this directory is **generated** by an internal Geosiris tool from
the ETP v1.2 Avro schemas (`rc/etp-1.2/`, Energistics, Apache-2.0). It is
committed so that the library builds without that tool.

- **Do not edit it.** Changes are overwritten at the next synchronisation, and
  CI refuses a pull request that touches this directory.
- In the generated types, **the order of the fields is the encoding**: Avro
  binary has no field names on the wire. See
  [ADR 4](../../docs/adr/0004-generated-types-field-order-is-the-encoding.md).
- Found a problem? **Open an issue** describing the message or field and what
  you expected. It is fixed in the generator and arrives here as a
  synchronised commit.

See the repository `NOTICE` for the provenance of the schemas.
