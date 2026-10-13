# Provenance of the ETP v1.2 Avro schemas

| | |
|---|---|
| Source repository | <https://bitbucket.org/energistics/etpv1> (Energistics, public) |
| Version | tag `v1.2` (the head of `master`) |
| Commit | `f94c3f9e96df005eb90600eec0ac7a3541597b3b` (2021-09-27) |
| Archive | `https://bitbucket.org/energistics/etpv1/get/f94c3f9e96df005eb90600eec0ac7a3541597b3b.zip` |
| Archive SHA-256 | `2dad002f25ce98100ccea7d353bc1fc4589e2a647e8b069baad89a0083435117` |
| Tree digest | `44921f15d38480d3508157d83b0f8548d9c67c48dd47072978ba9b54b4f3f530` (212 `.avsc`, method below) |
| Retrieved | 2026-10-13 |
| Licence | Apache License 2.0, the `License.txt` of that repository, copied verbatim next to this file (SHA-256 `a6cba85bc92e0cff7a450b1d873c0eaa2e9fc96bf472df0247a26bec77bf3ff9`) |

The `etpv1` repository names no copyright holder (no filled-in appendix, no
`NOTICE` file, no header in the schemas). None is claimed here.

## What was taken

- the 212 `.avsc` files of `src/Schemas/Energistics/` (197 records, 14
  enumerations, 1 fixed), byte for byte, under `Energistics/Etp/v12/` (the
  layout this repository already used; the generator reads the JSON, not the
  path);
- `License.txt`.

Not taken: `etp.avpr`, the specification PDFs, the DevKits, `build/`
(`Avro.dll`, `avrogen.exe`, `node_modules`).

`capabilities.json`, `error-codes.json` and `correlation.json` are ours, not
part of the archive (see `README.md`).

## Tree digest: the method

1. Every `*.avsc` of the tree, its path made upstream-relative
   (`Energistics/<rest>`: the `Etp/v12/` of this layout is removed), `/`
   separators.
2. For each, the SHA-256 of its bytes **after replacing CRLF by LF** (git
   normalises line endings on commit, so a Windows checkout and a Linux one
   differ in 29 of these files and must still agree).
3. One line per file, `<64 hex digits><two spaces><path>` and a newline, sorted
   by path bytewise.
4. The digest is the SHA-256 of that text.

In a shell, from `rc/etp-1.2/Energistics/Etp/v12`:

```sh
find . -name '*.avsc' | sed 's#^\./##' | LC_ALL=C sort | while read p; do
  echo "$(tr -d '\r' < "$p" | sha256sum | cut -d' ' -f1)  Energistics/$p"
done | sha256sum
```

`python scripts/sync-etp-schemas.py --check` does the same and compares with the
row above; `python scripts/sync-etp-schemas.py fetch --dry-run` downloads the
archive, verifies its SHA-256 and reports the digest of what it would lay out.
The archive digest covers the raw zip; the tree digest covers the schemas
alone, whatever the line endings.

## Branch warning

The `etpv1.2` branch of the repository is **not** the final version, despite
the upstream README: it stops at commit `54712d50` (2021-07-15). Use the tag
`v1.2`.

## Comparison with the previous copy

The previous copy (extracted from `etp-rs`) was compared on 2026-10-13: all
208 named types it held are identical to the official ones (names, namespaces,
field order, types, defaults, message keys, enum symbols). The official tree
adds four enumerations nothing references (`Protocol`, `EndpointCapabilityKind`,
`ProtocolCapabilityKind`, `DataObjectCapabilityKind`). Three file names differ
(`PutUnitializedDataArrayType.avsc`, `OpenChannelResponse.avsc`,
`PutUninitializedDataArray.avsc` upstream); the types inside are the same.
Report: `doc/suivi/etp-avsc-comparaison.md`.
