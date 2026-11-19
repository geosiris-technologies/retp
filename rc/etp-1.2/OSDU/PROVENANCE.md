# Provenance of the OSDU ETP extension schemas

| | |
|---|---|
| Source repository | <https://community.opengroup.org/osdu/platform/domain-data-mgmt-services/reservoir/open-etp-server> (OSDU, public) |
| File | `src/lib/oes/common/etp12/etp-1.2.avpr` |
| Commit | `e7a5183b5e5e069e50b5c27ce51a085a4240b2cf` (2026-10-22, the head of `main`) |
| File SHA-256 | `686c9f5c9b04fc1982f519af948a6ee4a249eb60b413cbc65e64e34a47b4f54c` (165 024 bytes) |
| Retrieved | 2026-10-22, through the GitLab API (`/projects/828/repository/files/…/raw?ref=<commit>`) |
| Licence | Apache License 2.0, the `LICENSE` of that repository, copied verbatim next to this file (SHA-256 `2d5b05ac90d4462bdbe5ec758a174fffe06bb74904e27d8e7c2a32e84a5d32e7`) |

## What was taken

The two records of the namespace `Energistics.Etp.v12.Protocol.CoreOSDU`
(protocol 2400, OSDU's extension of Core, numbered in the range §3.6.1 of
ETP 1.2 leaves to private protocols):

- `CoreOSDU/ResumeSession.avsc` (message 1, sent by the client);
- `CoreOSDU/ResumeSessionResponse.avsc` (message 2, sent by the server).

The `.avpr` holds them inline in its `types` array; each was written out as
its own file, the JSON object unchanged (its keys, their order, the field
descriptions, and the `fullName` and `depends` keys the `.avpr` adds, which the
generator ignores), indented with tabs.

Not taken: the rest of the `.avpr`. Its Energistics types are those of
`../Energistics/` (taken from `etpv1`); its other OSDU protocols (`StoreOSDU`
2404, `DataspaceOSDU` 2424) are not used yet.

## Why

A client that lost its connection asks the server to take back the session,
by its id, before opening a new one (`ResumeSession`). The generator reads
every `.avsc` under `rc/etp-1.2/`, so these become `protocol::core_osdu` in the
generated crate; the field order, which is the Avro encoding, is OSDU's.
