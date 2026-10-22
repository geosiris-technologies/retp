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

### OSDU's `ResumeSession`

The extension the context names is OSDU's `ResumeSession` (protocol 2400,
`CoreOSDU`, its schemas taken from OSDU's `open-etp-server`). It is spoken by
the connection itself, as Core is, and only before a session opens.

- **A client resumes from what the session was negotiated from**: the
  `RequestSession` it sent and the `OpenSession` it received (`Resumable`).
  Both ends negotiate the resumed session again from the same `RequestSession`,
  so they agree on what it speaks without the negotiation crossing the wire a
  second time.
- **A server does not decide alone**: the question goes to the application
  (`Event::ResumeAsked`), which alone knows whether the session still exists,
  where its state lives, and who may have it back.
- **The new connection is authorized for itself.** Nothing of the lost
  session's authorization is carried over: carrying it over is the
  authorization bypass of OSDU's issue #232, and a client that leant on it would
  hold rights nobody granted it.
- **Refused, the client opens a new session at once**, on the same WebSocket,
  and says so: a resume is attempted, never awaited.

## Consequences

- A cut connection during a download costs one retry, not the whole transfer.
- A write interrupted in flight reports an unknown outcome instead of a
  guessed one.
- Everything is testable without a clock, since time is an argument.
