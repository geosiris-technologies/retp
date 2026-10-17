# Checking a server: `retp-conformance`

`retp-conformance` judges an ETP v1.2 server against what the specification makes
**mandatory** and writes a report its provider can act on. It is not in the release archives:
`cargo install retp-conformance`, or the Docker image with `--entrypoint retp-conformance`.
`retp-conformance --help` is the authority on options.

## Reading only (the default): run it freely

```sh
retp-conformance --server my-server --out report.md --json report.json  # catalogue (0.3.0+)
retp-conformance wss://etp.example.com/etp --out report.md            # a URL
ETP_TOKEN="$(cat ~/.config/retp/demo.token)" \
    retp-conformance wss://etp.example.com/etp --out report.md         # a URL with a token
```

Pick report paths that do not exist yet: `--out` and `--json` overwrite. Without `--out` the
Markdown report goes to stdout. Useful options: `--dataspace PATH` (where to read; the first
listed otherwise), `--type 'resqml20.*'`, `--partition P` (OSDU), `--rest` (OSDU's REST API),
`--insecure` (no certificate check: `tls.chain` is then *not checked*), `--tls-fallback`
(check the certificate, report `tls.chain` failed if it is bad, and run the rest without it).
Logins are the CLI's: the catalogue, or the `ETP_*` variables read from files
([authentication.md](authentication.md)).

## Verdicts

Every finding has an id, the section of the specification, the rule in its own words, a
verdict and what was observed:

| Verdict | Meaning |
|---|---|
| `Pass` | the rule was observed to hold |
| `Fail` | the rule was observed to be broken |
| `Tolerated` | a deviation the client accepts, taken as meant |
| `NotApplicable` | the server does not announce the protocol |
| `NotChecked` | nothing could be observed (never a silent pass) |

When summarising, lead with the `Fail` rows (id, rule, observation), then `Tolerated`, and say
how many were `NotChecked` and why (an unreachable `/.well-known`, a protocol not agreed, an
empty dataspace). The exit code is `1` when anything failed — and also when the run could not
start; look at the report or the error. A `login.token` note means no token could be obtained:
the ETP server itself was not judged.

## `--write`: a change to the server — ask first

`--write` adds the checks that write. Before running it, tell the user exactly what it does on
**which server**, and wait for an explicit yes:

- it creates a dataspace of its own (`example/etp-conformance-<seconds>`, or the path of
  `--write-dataspace PATH`), puts sample objects and arrays into it, reads them back, deletes
  them, and deletes the dataspace, even when a step in between fails. A dataspace the server
  already lists is never used as the suite's own;
- **if the server refuses to let it create a dataspace** (or Dataspace, protocol 24, is not
  agreed), it writes its samples into an **existing** dataspace instead: the one named by
  `--write-fallback-dataspace PATH`, else the first listed dataspace whose name contains
  "test", else the default dataspace `eml:///`. It deletes only the objects it put there (their
  uuids share a prefix) and never that dataspace. `docs/conformance.md` does not say this:
  mention it, and prefer to name the fallback explicitly;
- an OSDU store wants custom data on a new dataspace: `--dataspace-custom legaltags=...`,
  `--dataspace-custom owners=...`, `--dataspace-custom viewers=...`;
- unlike `retp`, `retp-conformance` does not ask for confirmation on a catalogue server marked
  `protected` (0.3.0): your own confirmation with the user is the only guard.

On a shared or production server, prefer reading only, or a `--write-dataspace` path the user
chose.

## Passing the report on

The report quotes what the server answered. Before sharing it, skim the `said` column for
anything that should not leave the organisation (internal names, tokens a server echoed);
retp removes the catalogue entry's own secrets from what it records, but that is a guard, not
a proof.
