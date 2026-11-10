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
as `tracing` warnings (the application installs the subscriber and chooses where
they go). Each event keeps its text and adds structured fields - `peer`,
`etp_message`, `message_id`, `correlation_id`, `code`, `error`, `bytes` - so a
server's journal can be filtered and joined without parsing prose. No field ever
carries a credential or the body of an object or an array.
A mistake that would make the two ends disagree refuses the session and says
why. A strict tolerance refuses both.

## Time

Nothing in the protocol crate reads the clock: every call that needs the time
takes it as an argument. Tests drive timeouts, keepalives and resumption
without sleeping.

## Asking a lost session back

ETP 1.2 has no session survivability; OSDU's servers add `ResumeSession`
(protocol 2400, `CoreOSDU`), which asks the server, on a new WebSocket and in
place of `RequestSession`, to give back a session whose connection was lost.
The client keeps what it needs from an open session, `client.resumable()`, and
opens the next connection with `native::connect_resuming(connect, lost)`. If the
server gives the session back, it is the same session - its id, its protocols,
and on the server side whatever it kept for it, such as an open transaction;
`client.resumed()` is then true. If it does not (`ENOT_FOUND`, or a server that
does not speak protocol 2400), the refusal is logged and a new session opens on
the same WebSocket: the resume is attempted, never awaited (ADR-0003).

Either way the new connection authorizes itself from the start: no right of the
lost session is carried over. A server takes `ResumeSession` once built with
`Connection::accepting_resume()`: each request reaches the application as
`Event::ResumeAsked`, which answers with `Connection::resume(lost_request)` (the
`RequestSession` the lost session was opened with, `Connection::requested()`)
or `Connection::refuse_resume(incident)`.

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

## A token that runs out: renewing an open session

A session lasts longer than a token. ETP 1.2 (section 3.13.5) wants an endpoint to
re-authorize *before* its authorization expires, with an `Authorize` sent at any time; a
server that is not told sends `EAUTHORIZATION_EXPIRED` at the end, then `CloseSession`, and
nothing brings the session back. The warning `EAUTHORIZATION_EXPIRING` is optional, so the
client does not wait for it.

`retp` offers the door and leaves the timing to the application, which knows when its token
changes:

- `Client::reauthorize(&credentials)`, `AsyncClient::reauthorize(&credentials)` and
  `LiveSession::reauthorize(&credentials)` send the `Authorize` and wait for its answer.
  The session is the same one afterwards. A refusal is `ClientError::AuthorizationNeeded`
  with the server's challenges, and the session is still open until the authorization it
  holds ends: another renewal may be in time. Both answers are also events of the session
  (`Event::Reauthorized`, `Event::AuthorizationRefused { challenges }`, for
  `Filter::Session`); an event never carries a credential.
- A `CredentialSource` is what an application hands over when one fixed `Credentials` is not
  enough: `credentials()` gives credentials for one opening, one REST request or one
  renewal, and `refused(witness, why)` is told when the server refused them. Each `Fresh` it
  gives carries a `Witness` - whatever the application puts in it - and gets it back on the
  refusal of **those** credentials. The source keeps no state per connection, so an ETP
  session and REST calls to the same server can share one source without the refusal of one
  being taken for the refusal of the other. A source may block (it may fetch a token): it is
  never called on the thread or task that runs a session.
  `LiveSession::connect_with_source` and `RestClient::with_source` take one; a fixed
  `Credentials` is a source too (`impl CredentialSource for Credentials`), and the fixed
  forms keep working.
- The 401 rule is applied once: a 401 on the WebSocket upgrade or on a REST request goes
  back to the source, the credentials it gives next are tried, and a second 401 is the error.
  A 403 is not retried. `LiveSession::renew()` (and `reauthorize_with(&source)` on the other
  clients) renews from the source with the same rule on an `AuthorizeResponse` that failed.

```rust,ignore
// Every few minutes, or when the token changes - the application's rule, not the library's:
session.renew()?; // asks the source, sends Authorize, goes back to the source once if refused
```

### Renewing at the server's warning, with a source of your own

An application that has its own way of getting tokens needs nothing more than the two
sections above: a destination routed on `Filter::Session`, and `renew`. The destination only
passes the session's events on; a thread that is not the session's reads them and renews.

```rust,ignore
let source: Arc<dyn CredentialSource> = Arc::new(MySource::new()); // fetches a token in `credentials()`
let session = LiveSession::connect_with_source(source, || my_connect(), LiveSettings::default())?;
let (queue, events) = Queue::bounded(64);
session.route(Filter::Session, Arc::new(queue));

let mut due: Option<Instant> = None; // or from the end the source knows for the opening credentials
loop {
    let wait = due.map_or(Duration::from_secs(3600), |at| at.saturating_duration_since(Instant::now()));
    let renew = match events.recv_timeout(wait) {
        Ok(Event::AuthorizationExpiring) => true,          // the server's warning (optional)
        Ok(Event::Closed { .. }) => break,                 // the session is over
        Ok(_) => false,                                    // `Event` is non-exhaustive
        Err(RecvTimeoutError::Timeout) => due.is_some_and(|at| Instant::now() >= at), // our own clock
        Err(RecvTimeoutError::Disconnected) => break,
    };
    if renew {
        match session.renew() {
            // `Some(end)`: when the accepted credentials end, if the source said.
            Ok(end) => due = end.map(|end| instant_before(end, MARGIN)),
            // Refused: the session stays open until the authorization it holds ends.
            Err(ClientError::AuthorizationNeeded { challenges }) => { report(challenges); break; }
            Err(other) => { report_error(other); break; }
        }
    }
}
```

Here the main thread reads the queue and renews, so the session is not shared with another
thread. Two traps:

- **Do not renew from inside a destination.** `deliver` runs on the dispatcher thread of the
  session, and `reauthorize` and `renew` wait for the server's answer: that thread would wait
  for the session, and the destinations behind it with it. Use a `Queue` (or a `Callback` that
  only passes a signal on a channel) and a thread of your own that calls `renew`.
- **Do not send again credentials the server already has.** A server may accept an `Authorize`
  that does not move the end of the authorization and warn again at once; an application that
  answers every warning with the same token then sends several `Authorize` per second, all
  granted, with the end unchanged. Renew only with credentials newer than the ones last
  accepted. With a source, that is the source's job: `credentials()` must give a fresh token
  each time it is asked. The loop can protect itself too: if the end that `renew()` returns is
  not later than the previous one, do not answer the next warning at once.

The warning is optional, so the loop does not rely on it alone: it also renews on its own
clock, some margin before the end that `renew()` returned (`Receiver::recv_timeout` gives both
in one wait). `connect_with_source` does not return the end of the credentials it opened with:
a source that knows it keeps it, and the loop asks the source for its first deadline. The
complete program is `crates/retp-client/examples/renew_on_warning.rs`.

`retp_proto::connection::Event` and `ClientError` are `#[non_exhaustive]`: a later version may
add a case (the renewal added some) without a breaking change. A `match` over either, written
in another crate, needs a catch-all arm; treat an unknown event as "not for me" and an unknown
error as a failure, never as a success, and do not print it with `{:?}`. The library's own
destinations do the same: `JsonLines` writes `{"received", "event": "unknown"}`, and the
console says "an event this version does not know"; the Python bridge raises the base
`RetpError` with the error's own text.

`Credentials` is wiped from memory when dropped (`zeroize`), every clone with it; a copy made
by whoever reads `header()` is theirs to wipe.

## wasm

`cargo check --target wasm32-unknown-unknown` on the client is part of CI. The
native WebSocket, TLS, HTTP probe and logins are not compiled for wasm; the
page brings its own transport (the browser's `WebSocket` and `fetch`).

## As a storage

`energyml-etp-store` implements energyml-rust's `EnergymlStorage` on top of the
client, so code that lists and reads objects from an EPC file lists and reads
them from a server. See that crate's rustdoc.
