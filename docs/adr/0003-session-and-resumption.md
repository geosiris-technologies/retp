# 3. Session and resumption

## Context

Long operations (downloading a dataspace, pushing a package) outlive
connections. Servers close idle sessions, proxies drop connections, tokens
expire. ETP v1.2 has no resumption in its core protocol; OSDU Reservoir DDMS
offers a mechanism of its own.

## Decision

- **Resumption is a first-class path, never a derivation of the normal path.**
  It restarts from the probing state, with no shortcut.
- **Resumption is an extension, declared as such.** It is attempted, never
  awaited: when the server does not support it, the client reconnects and
  starts again.
- **Only what is idempotent is replayed.** A read can be asked twice; a write
  is replayed only when it can be shown to be harmless.
- **The watermark is the only thing a client keeps from one session to the
  next.** A catch-up asks from before the watermark, not after it.
- **A long operation is a plan of steps and a journal**, driven by a runner:
  progress is observable and a failure resumes at a step.
- **Keepalive is a `Ping` sent after a period of quiet**, driven by the same
  periodic call as the other deadlines. A client keeps its session alive; a
  server does not unless told to.

## Consequences

- A cut connection during a download costs one retry, not the whole transfer.
- A write interrupted in flight reports an unknown outcome instead of a
  guessed one.
- Everything is testable without a clock, since time is an argument.
