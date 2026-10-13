# 2. Decisions by tier

## Context

The client was built in tiers: the wire first, then the session, then a first
client against real servers, and so on. Real servers differ from each other and
from the specification, and a decision taken at one tier often only becomes
visible at the next.

## Decision

Each tier records what it settled, in the order it was learned. The ones that
shaped the public behaviour:

- **Tolerate the non-critical deviations of an external peer, refuse the
  critical ones.** A deviation whose meaning is not in doubt (a role spelled
  `server`, `resqml20` for `resqml20.*`) is taken as meant and logged as a
  warning naming the rule it breaks. A deviation that would make the two ends
  disagree (the server claiming the client role, another protocol version)
  refuses the session.
- **The client yields by default to everything the server allows**: it asks for
  every release it knows, and every type the server announces before the
  session.
- **A server has habits**: a named server profile (`standard`, an OSDU one)
  carries what a family of servers needs, in one place.
- **A client never assumes a server is a machine.** A transaction answered with
  a 404 is not blindly replayable; an interrupted request has an unknown
  outcome and says so.
- **The same API, with failure modes that show.** Over a WebSocket and over a
  REST API the operations are the same; what REST cannot do is refused by name,
  never emulated.

## Consequences

- A user can tell whether a problem is in the client, in the server, or in the
  specification, because deviations are reported rather than hidden.
- Tolerance is a setting: a strict mode refuses what the default tolerates.
- Server-specific habits are isolated in profiles and do not leak into the
  protocol crate.
