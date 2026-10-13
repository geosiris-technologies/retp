# Conformance checker

`retp-conformance` judges an ETP v1.2 server against what the specification
makes **mandatory**, and writes the report a provider can act on. It is a
separate crate from the client, and nothing else in the workspace depends on
it: what a server is judged on must never leak into what the client tolerates.

```sh
retp-conformance wss://etp.example.com/etp --out report.md --json report.json
```

## One check, one cited rule, one verdict

Every check yields a finding: an id, the section of the specification, **the
rule in its own words**, the protocol, a verdict, and *what was observed*. A
provider reading the failures table has the rule to read and the observation to
fix.

Five verdicts, and the difference between them is the report:

| Verdict | Meaning |
|---|---|
| `Pass` | the rule was observed to hold |
| `Fail` | the rule was observed to be broken |
| `Tolerated` | the fault was taken as meant by the client (a deviation the negotiation accepts) |
| `NotApplicable` | the server does not announce the protocol; the specification only makes a protocol mandatory when announced |
| `NotChecked` | no check exists, or there was nothing to observe |

A check that could not observe answers `NotChecked`, **never** `Pass`: a green
report on an unreachable server would be the worst possible result.

## Order and scope

The order is a client's: session, dataspaces, the content of a dataspace, an
object; each step passes what it found to the next, and the next says
`NotChecked` when there is nothing. Wire rules (odd, increasing message ids,
correlation, final-message flag) are verified on everything the run received,
not on a request chosen to pass.

**Writes only happen with `--write`**, in a dataspace named for the suite,
created and then deleted whatever happens in between. A dataspace the server
already lists is never written into.

## Options worth knowing

| Option | Effect |
|---|---|
| `--rest` | check an OSDU Reservoir DDMS REST API; wire-only rules answer `NotApplicable` |
| `--insecure` | do not verify the certificate |
| `--tls-fallback` | verify it; if it fails, report `tls.chain` as failed and run the rest unverified |
| `--write`, `--write-dataspace`, `--dataspace-custom KEY=VALUE`, `--write-fallback-dataspace` | the checks that write, and what a store needs to create a dataspace |
| `--dataspace`, `--type` | where to read, and which types to ask for |
| `--out`, `--json` | Markdown report (or JSON by extension), and a JSON copy beside it |

Logins and secrets are the CLI's (`ETP_TOKEN`, ...): see [Command line](cli.md).

## The checker is checked against a fake

The tests run the checks against an in-memory store with a **button per rule**
a store can break without the session refusing. Each test turns one button and
reads the verdict that must turn red. Adding a check means adding the button
that makes it fail; otherwise it may be looking at nothing.
