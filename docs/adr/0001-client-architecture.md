# 1. Client architecture

## Context

ETP v1.2 is symmetric enough that a client and a server follow the same wire
rules: message numbering, correlation, multipart messages, negotiation, error
registry. An earlier proof of concept mixed protocol and client logic, which
made both hard to test without a socket and without real time.

## Decision

- **The protocol is a crate of its own** (`retp-proto`): a single connection
  type, instantiated with its role, shared by a client and a server. The client
  does not contain the protocol; it drives it.
- **No socket and no clock in the protocol.** A `Transport` and a `Clock` are
  traits; every call that needs the time takes it as an argument. Tests drive
  deadlines and keepalives without sleeping.
- **The connection holds two roles**: the role of the connection and the role
  of the session, which are not always the same.
- **Errors go through a registry**: a code plus a detail. Capabilities are read
  through a typed registry, never by looking into a raw map.
- **A blocking client and an asynchronous one share one engine**: bytes in,
  bytes out, answers gathered. Every operation is written once, as an `async fn`
  over a `Caller`; the blocking client runs it to completion.
- **Native networking stays out of wasm**: the WebSocket, TLS and HTTP probe are
  compiled only for native targets, so the client crate checks for
  `wasm32-unknown-unknown` and a page brings its own transport.

## Consequences

- Every rule of the wire is exercised in memory, with a transport that talks to
  a server connection.
- A server can be built on the same crate and cannot disagree with the client
  on the wire.
- The price is a layer: the client crate is thin, and the protocol crate alone
  does not open a socket.
