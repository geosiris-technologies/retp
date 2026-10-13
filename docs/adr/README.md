# Architecture decision records

Short records of the decisions that shape `retp`: the context, the decision,
and what follows from it.

| # | Decision |
|---|---|
| [1](0001-client-architecture.md) | One protocol core for client and server; no socket and no clock inside it |
| [2](0002-decisions-by-tier.md) | The client was built in tiers, and each tier settled its own questions |
| [3](0003-session-and-resumption.md) | Resumption is a first-class path, replays only what is idempotent, and is proposed rather than assumed |
| [4](0004-generated-types-field-order-is-the-encoding.md) | Generated types: the field order is the encoding |
| [5](0005-conformance.md) | Conformance is judged apart from the client: one check, one cited rule, one verdict |
| [6](0006-bridge-to-the-storage-trait.md) | The bridge: an ETP dataspace behind the library's storage trait, so what reads a file reads a server |
