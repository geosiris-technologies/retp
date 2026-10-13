# Protocol core (`retp-proto`) - architecture

Related decisions: [ADR 0001](../../docs/adr/0001-client-architecture.md) (one protocol core for client and server; no socket and no clock inside it), [ADR 0002](../../docs/adr/0002-decisions-by-tier.md), [ADR 0003](../../docs/adr/0003-session-and-resumption.md).

> The bridge crate is `energyml-etp-store`, not `retp-*`: it implements a trait of energyml-rust, and keeps the `energyml` prefix.

## Purpose and position

The ETP v1.2 protocol for a client and a server alike: the wire and the session. It sits on the generated message types and on the Avro runtime (`retp-avro`), and under the client, the bridge and the conformance suite. **No transport and no `async`.**

## The wire

- **No sentence assumes which side of the wire we are on.** The side is a parameter (`Side`); a test fails the day server identifiers stop being odd. A `Wire` receives the bytes of one WebSocket message and returns what to send back and what arrived; to emit, it returns bytes. This makes everything testable without a network and lets each face choose its socket.
- **Every message leaves through `Wire::emit`**: identifier, FIN flag and size check cannot be bypassed by any path. A new kind of message goes through it, never through a hand-built `MessageHeader`.
- **Reception follows the order of the specification (ETP v1.2 §3.5.4.2)**, written at the head of `receive.rs`: header, **`Acknowledge` before everything**, identifier, compression, extension, route, correlation, body, BLOBs. Swapping two steps changes what the peer receives (a refusal sent before the acknowledgement is the tested example).
- **Two roles, two types.** `Side` is for the connection, `Role` for the session (negotiated per protocol). What a peer may send is read from the session role.
- **An exception is never answered by an exception.** What is wrong in a received `ProtocolException` goes into `Received::fault`.
- **Correlation is decided before the body and applied after** (`Commit`): an unreadable body does not close an action.
- **Error codes the specification does not name are chosen once and documented**: `EINVALID_STATE` for a correlation that is not open or an identifier out of rule, `EINVALID_MESSAGE` for an unreadable body, `ECOMPRESSION_NOTSUPPORTED` for a non-negotiated compression. A new case takes one of these or extends the list with its reason, never a code chosen on the spot.
- **The error registry cannot be replaced by a caller**, the one registry where a caller's additions are not tried first: the meaning of an Energistics code is not the caller's to change. `EtpError` is held against the published error-code table by a test.

## The session

- **A connection announces its handlers.** The protocol list of the offer is rebuilt from the `Handler`s it is given; a protocol is implemented entirely or not announced (§3.1.1.2 ¶3). A test fails otherwise.
- **No call to the clock.** Everything takes `now` (UTC microseconds); a test reads the crate's sources to make sure none calls `SystemTime::now`.
- **A capability is read through `capabilities::Capabilities`**, never in the raw `DataValue` map: it is the one place that knows the types, bounds and defaults, and its definitions are held against the published capabilities table.
- **Resumption restarts from `Probing`**: no shortcut skips authorisation.
- **`Credentials` are the whole value of an `Authorization` header**, scheme included (§4.1.4 ¶2; §5.3.4: "the content of the equivalent HTTP Authorization header (NOT just a token)"). Nothing prepends `Bearer`.
- **Tolerant toward the peer, explicit otherwise.** `Tolerance` is a field of the `Offer` and of the wire `Config`, `Tolerant` by default. A fault whose meaning is not in doubt is taken as meant and recorded as a `Deviation` in `Negotiated::deviations` and in the log; a fault that would make the two ends diverge is refused, naming all faults at once. Tolerated is not invisible: a test fails if a deviation does not reach the log.
- **An offer is described in code or in JSON, and both say the same thing** (`describe.rs`). Everything goes through the capability registry: what we would refuse from a peer we do not announce (unknown capability, wrong type, out of bounds, an unqualified type, a format ETP does not have, an unknown JSON key).
- **A `Ping` waits like any request**: a `Pong` that never returns correlated gives `ETIMED_OUT` after the `ResponseTimeoutPeriod`, not an endless wait. A client keeps its session alive (a ping after 30 s of silence, one at a time; its `Pong` produces no application event); a server pings only when told to. Anything passing in either direction postpones the next ping.
- **Retained types are an intersection; a type not asked for is a tolerated deviation** (§5.2.2 row 6.1), refused in `Strict`. `Offer::widened_to` adds to an offer the types of a server it does not cover.
- **A catch-up asks from *before* the watermark.** `Continuity::window` subtracts the server's `ChangePropagationPeriod` from the watermark before asking what changed, because a change made just before the cut may only have become visible just after (§26.6.3 ¶3.a). Asking too early costs a few objects read again (a read is idempotent); asking too late loses a change silently. The same shifted time decides whether it is too late: a watermark the delay pushes out of the retention window is out, and the server would answer `ERETENTION_PERIOD_EXCEEDED` (5001). `Continuity` holds no session field: what survives a session is a watermark, never a handle that reopens a context.
- **A refused answer ends its request** (`Received::refused_answer`, `Event::AnswerRefused`), or the caller waits to its deadline for an answer already received.

## Tests

`tests/wire.rs` holds the named tier-1 tests and `tests/etp_session.rs` those of the session (names differ from other test binaries of the workspace that are called `session`). A peer that breaks a rule is written **by hand** (`raw` in `wire.rs`), since a well-behaved `Wire` refuses to emit what must be tested. A refusal test verifies the **reason**, not only the code: a sabotage once found an extension test that went red for another reason than its own. Run with the workspace: `cargo test --workspace --test wire`.
