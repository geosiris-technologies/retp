# Library usage

> Snippets below describe the shape of the API; check the
> rustdoc of the version you use. TODO(verify): turn them into doctests once
> the API settles.

## The layers

| Layer | Crate | Knows |
|---|---|---|
| Avro binary | `retp-avro` | how to encode and decode; nothing of ETP |
| Messages | `retp-types` | every ETP v1.2 message and data type, with its codec |
| Wire and session | `retp-proto` | numbering, correlation, multipart, negotiation, handlers; **no socket, no clock** |
| Client | `retp-client` | a `Transport` and a `Clock` under the connection, and the operations |
| Bridge | `energyml-etp-store` | an ETP dataspace as an `EnergymlStorage` |

`retp-proto` is written for a client and a server alike: one connection type,
instantiated with its role. A client built on it, and a server one day, agree on
every rule of the wire because they run the same code.

## Opening a session

A client is a `Client<T, C>`: a `Transport` (the WebSocket on native targets, a
fake in memory in tests) and a `Clock` (the system clock, or one you give).
`Client::open` sends `RequestSession`, authorizes if the server asks and you
gave credentials, and returns once `OpenSession` has arrived.

```rust,ignore
let mut client = retp_client::native::connect(Connect {
    endpoint,           // URL, credentials, certificate policy, server profile
    offer,              // what this client asks for: types, formats, protocols
    handlers,           // the protocols it implements
    max_message: None,  // taken from the server's capabilities when probed
})?;
```

Operations are plain functions over the client:

```rust,ignore
let spaces = retp_client::list_dataspaces(&mut client)?;
```

The same operations exist asynchronously (`AsyncClient`), fed by whoever holds
the socket; that is the form a browser uses.

## What you announce

By default the client asks for everything: every release the build knows, XML
and JSON, and Discovery, Store, DataArray, Dataspace and SupportedTypes. The
session keeps the intersection of both ends' types. Describe other terms in
code (`Offer::with_types`, `with_formats`, `with_capability`,
`with_tolerance`) or in a JSON file (`retp --offer file.json`, see
[Command line](cli.md)).

## Tolerance

A server's mistakes whose meaning is not in doubt are taken as meant and logged
as warnings through the `log` facade (the application chooses where they go).
A mistake that would make the two ends disagree refuses the session and says
why. A strict tolerance refuses both.

## Time

Nothing in the protocol crate reads the clock: every call that needs the time
takes it as an argument. Tests drive timeouts, keepalives and resumption
without sleeping.

## A live session: what arrives unasked, pushed as it arrives

The blocking client reads the WebSocket only while a call runs: between two calls, a
notification the server sends waits in the socket. With the feature `live`,
`retp_client::live::LiveSession` drives the session from a task of its own, which sleeps
until a message arrives, a request is to be sent, or the next deadline (the keepalive
`Ping`, a response timeout) is due - nothing polls. Requests go from any thread through
`session.client()`, an `AsyncClient` on which every operation of the crate runs
(`session.block_on(...)` from synchronous code); what arrives unasked goes to the
destinations routed for it, on a thread of their own, so a slow destination never delays an
answer:

```rust,no_run
use std::sync::Arc;
use retp_client::live::{Filter, JsonLines, LiveSession, LiveSettings};

# fn run(connect: retp_client::native::Connect) -> Result<(), Box<dyn std::error::Error>> {
let session = LiveSession::connect(connect, LiveSettings::default())?;
// The notifications of protocol 5 go to a JSON Lines file, one line each.
session.route(Filter::Protocol(5), Arc::new(JsonLines::create("notifications.jsonl")?));
// ... subscribe, then keep sending requests: the notifications are written as they come.
session.close("done")?;
# Ok(())
# }
```

The destinations are `JsonLines` (one JSON object per line), `Console` (one line in words),
`Callback` (a function) and `Queue` (a bounded channel); a `Destination` of your own is a
trait to implement. `Filter` routes a protocol, one subscription, or the session's own
events (a fault, the authorization running out, the session closed). A full queue drops and
counts (`LiveSession::dropped`), never blocks the session.

## wasm

`cargo check --target wasm32-unknown-unknown` on the client is part of CI. The
native WebSocket, TLS, HTTP probe and logins are not compiled for wasm; the
page brings its own transport (the browser's `WebSocket` and `fetch`).

## As a storage

`energyml-etp-store` implements energyml-rust's `EnergymlStorage` on top of the
client, so code that lists and reads objects from an EPC file lists and reads
them from a server. See that crate's rustdoc.
