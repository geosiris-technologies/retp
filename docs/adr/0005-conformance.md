# 5. Conformance is judged apart from the client

## Context

Providers want to know whether their ETP server conforms to the specification.
A client that tolerates deviations cannot be the judge: it would pass what it
forgives. Conversely, what a server is judged on must not leak into what the
client tolerates.

## Decision

- **The checker is a crate of its own**, which nothing else in the repository
  depends on.
- **One check, one cited rule, one verdict.** A finding names the section of the
  specification, quotes the rule in its words, and records what was observed.
- **Five verdicts**: `Pass`, `Fail`, `Tolerated`, `NotApplicable`,
  `NotChecked`. A check that could not observe says `NotChecked`, never `Pass`.
- **A protocol is only mandatory when the server announces it.** Otherwise the
  checks for it are `NotApplicable`.
- **The order is a client order**: session, dataspaces, content, an object. Every
  request goes through one function that keeps the headers received, so wire
  rules are verified on everything seen.
- **Writes only on request**, in a dataspace of the suite own, created and
  deleted whatever happens between.
- **A fake store with a button per rule** tests the checker: each test breaks
  one rule and expects the matching verdict to turn red.

## Consequences

- A report is actionable: the rule and the observation are in it.
- A green report on an unreachable server is impossible by construction.
- Adding a check obliges adding the button that makes it fail.
