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

One dependency is enough: `retp-client` re-exports the crates whose types its
signatures use, whole, as `retp_client::types` (`retp-types`),
`retp_client::proto` (`retp-proto`) and `retp_client::avro` (`retp-avro`).
Write `retp_client::types::datatypes::ErrorInfo`, not a second dependency on
`retp-types` that must be kept at the same version.

## Opening a session

A client is a `Client<T, C>`: a `Transport` (the WebSocket on native targets, a
fake in memory in tests) and a `Clock` (the system clock, or one you give).
`Client::open` sends `RequestSession`, authorizes if the server asks and you
gave credentials, and returns once `OpenSession` has arrived.

```rust,ignore
let mut client = retp_client::native::connect(Connect {
    endpoint,           // URL, credentials, certificate policy, server profile, extra headers
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

## Headers a server wants

Which headers a server expects in addition to the protocol's own is a property of
the server, like its URL: a tenant, a routing key, a data partition, an API key.
They are given once, with the endpoint, and go with every request to it: the
WebSocket handshake, the probe of `/.well-known/etp-server-capabilities`, each
call of the REST API.

```rust,ignore
use retp_client::ExtraHeaders;
use retp_client::native::Endpoint;

let mut endpoint = Endpoint::new("wss://host/etp");
endpoint.headers = ExtraHeaders::new()
    .with("data-partition-id", "opendes")?    // the data partition of OSDU's Reservoir DDMS
    .with("x-tenant", "acme")?
    .with_secret("x-routing", routing_key)?;  // a secret whatever its name
```

`ExtraHeaders` is the one door they come through. Each header is checked when it
is added, and what cannot be sent is refused there, by name (`HeaderRefusal`,
which never carries a value): a name that is not letters, digits, `-` and `_`; a
value with a control character or a character that is not ASCII; a header the
client writes itself
(`authorization`, `etp-encoding`, `sec-websocket-*`...); the same name twice,
whatever the case. The data partition of OSDU's Reservoir DDMS is one of these
headers, `data-partition-id`, and nothing else: the client has no other way to
give it.

A value may be a secret. One added with `with_secret`, or whose name says so
(`sensitive_header`: `cookie`, anything with `authorization`, `key`, `token`,
`secret` or `password`), is wiped from memory when the list is dropped and is in
no `Debug` of this crate: the list and a REST `HttpRequest` show the names, and a
`Handshake` shows the value of ETP's own attributes only. A request that carries
one follows no redirection, so that it goes to no other host. It is never written
in a URL either. The handshake of a
front end (`Profile::Browser`) has no header, its attributes go in the query
string (ETP 1.2 section 4.3.2), so a header that carries a secret is refused
there, before the socket is opened (`HeaderRefusal::InTheUrl`). The copy a
transport makes to send a header is the transport's to wipe.

`Endpoint::sendable()` says what a WebSocket connection would refuse about the
headers with nothing sent: ask it before a login, which costs a request.

A transport of your own (the browser's `WebSocket`, which takes a URL) gets the
handshake to send from `ExtraHeaders::onto(handshake)`.

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

## Size limits on what a server sends

A message from a server is read only up to a limit, so that one that lies about its size
does not fill the memory. A message above it is refused by name, before it is buffered,
as a transport error that says the size and how to raise the limit.

- **WebSocket messages**: the `MaxWebSocketMessagePayloadSize` the client announces in its
  handshake (`Connect::max_message`), or, when none is announced,
  `native::DEFAULT_MAX_WEBSOCKET_MESSAGE` (64 MiB). To accept more, announce a larger
  size, or open the socket yourself with
  `WebSocketTransport::connect_limited(&handshake, certificates, max_message)`, whatever
  the handshake says. A `LiveSession` applies the same rule to the size it announces.
- **HTTP bodies** (the REST API): `native::DEFAULT_MAX_HTTP_BODY` (256 MiB), changed with
  `UreqHttp::with_max_body(bytes)`; a larger body is refused and not buffered beyond the
  limit.
- **Avro counts**: an array or a map whose block announces more items than the input has
  bytes left (or an endless run of items with no content) is refused as
  `retp_avro::Error::TooManyItems`, before anything is allocated for it. Nothing to
  configure.

Array reads are cut into messages of 16 MiB at most by default, so the WebSocket default
leaves room for the other messages.

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

## Reading regions with metadata you already hold

`retp_client::arrays::read_regions` reads arrays whole, or a region of each (a `Block`).
It first asks the server for the arrays' metadata (`GetDataArrayMetadata`) to plan the
blocks. An application that already asked, to show shapes for instance, passes what it
holds to `read_regions_with`, and no round trip is spent on it again. It exists on the
async client (`retp_client::asynchronous::read_regions_with`) and on the blocking one
(`retp_client::arrays::read_regions_with`).

```rust,ignore
let requests = vec![(id_a, Some(block)), (id_b, None)]; // a region of `a`, all of `b`
let known = vec![Some(metadata_of_a), None];            // `known[i]` goes with `requests[i]`
let read = retp_client::arrays::read_regions_with(
    &mut client, &requests, &known, default_message, |_progress| {},
)?;
```

`known[i]` is the metadata of request `i`, or `None` to have it asked; entries past the
end of a shorter `known` count as `None`. Only the unknown ones are asked, together in one
`GetDataArrayMetadata`, and none is sent when every array is known. Given or asked,
metadata goes through the same planning and the same checks: a region that does not lie
inside the dimensions is refused by name, not clipped.

Metadata you give is used as given, and **its staleness is your risk**. When the store's
data disagrees with it (a whole array whose dimensions changed, another data type, a block
of another element count), that array's result is `ArrayFailure::Inconsistent`. A region
cannot show that the array grew or shrank: the store refuses the region, or sends data of
another type. Ask for the metadata again when in doubt.

## wasm

`cargo check --target wasm32-unknown-unknown` on the client is part of CI. The
native WebSocket, TLS, HTTP probe and logins are not compiled for wasm; the
page brings its own transport (the browser's `WebSocket` and `fetch`).

## As a storage

`energyml-etp-store` implements energyml-rust's `EnergymlStorage` on top of the
client, so code that lists and reads objects from an EPC file lists and reads
them from a server. See that crate's rustdoc.

## Pushing a package by islands

The module `energyml_etp_store::push` sends everything an `EnergymlStorage` holds (an EPC,
another dataspace) into a dataspace, in **islands**: one transaction each, so that a
connection lost halfway costs an island and not the model. The command line's `retp push`
and the Python bridge's `push_epc` are this module; their options are described in
[Command line](cli.md#examples) and [Python](python.md#the-plan-without-a-session).

An island follows two rules, the first winning over the second:

1. **Targets before sources.** An object goes after everything it references, so that no
   island asks a store to hold a reference to something it has not seen. A cycle (two
   objects naming each other) is never split: it stays whole in one island.
2. **What carries no array goes first**, in islands of its own, as far as the first rule
   allows. An object and its arrays are never apart.

A budget cuts the rest: `Budget { objects, bytes }`, 100 objects or 32 MiB by default
(a cycle larger than that still goes whole).

```rust,ignore
use energyml_etp_store::push::{self, Options};

let options = Options { skip_present: true, ..Options::default() };
let report = retp_client::blocking(push::push(
    &mut client, &source, "demo", &options, |progress| println!("{progress:?}"),
))?;
```

`push::push` goes over one session: a connection that falls stops the push there.
`push::push_resumable(connector, &mut session, &source, dataspace, &options, &retry,
on_event)` opens a new session instead and goes on from the first island not committed,
which is sent again whole; `push::reconnecting(times, first_pause)` builds its `Retry`.
What starts a push again, or narrows a refusal, is chosen in `Options`, and is off by
default:

| Field | Meaning |
|---|---|
| `journal` | a file where each island is recorded as it commits; a push started again with it leaves out what it records. A journal written for another package or dataspace is refused |
| `skip_present` | leave out an object the dataspace holds under the same URI, its arrays answering metadata of the same dimensions. The content is not compared |
| `split` | what becomes of an island the store refuses: `Split::Off` (stop, the default), `Halve`, or `Single` (object by object); a cycle is never split |
| `split_limit` | how many times the pieces of one island may be split again (8) |
| `well_known` | `WellKnown::Push(registry)` also sends the official dictionary's `PropertyKind`s the source references and does not hold |
| `only` | only these objects, by URI |
| `budget` | the `Budget` above |

`Options` also holds `format` (XML or JSON), `grouping`, `expected` and `overwrite`.

### The plan, without a session

`push::dry_run(&source, dataspace, &options).await` does the same work up to the point
where it would ask the destination anything, and returns a `Plan`: the islands with their
cycles, each object's URI in the destination, what is left out and what leaves the
source. It is the same code path as the push, so the plan is the one the push follows. An
option only the destination can honour (`skip_present`, `expected`) is refused by name,
as `NoPlan::NeedsDestination`.

### Reading a result

A push returns a `Report`; an `Err` is for a source that could not be read, not for a
store that refused. Its fields: `islands` (the plan), `sent` (how many committed: the
first `sent` of `islands`), `stopped` (where it stopped, `None` when all went through),
`refusals` (every object or array of the stopped island the store would not take, each
with its code and words), `skipped` (left out by `skip_present` or the journal), `splits`
(islands cut and sent as pieces), `reconnections` and `reopened` (how each new session
came back, resumed or fresh).

`Report::diagnosis()` sorts the stop for a program that acts on it, and is `None` when all
went through:

```rust,ignore
if let Some(why) = report.diagnosis() {
    // `island` is 0-based; `kind` is a StopKind: Refused, Failed, Timeout, GaveUp, Journal.
    eprintln!("island {} {}: {} ({:?})", why.island, why.kind.name(), why.message, why.code);
    if let Some(Cause::MissingReference { count, named, more, .. }) = &why.cause {
        // `named` is the server's statement; `more` it counted and did not name.
        eprintln!("{count} missing, {} named, {more} more", named.len());
    }
}
```

- `message` and `code` are the store's own words for a refusal, apart from any hint this
  side adds; for another stop, what stopped it.
- `cause` is `Some` for a refusal only. `Cause` reads the words of open-etp-server (the
  OSDU Reservoir DDMS server) and nothing else: any other words are `Cause::Unknown`,
  never a guess. `Cause::MissingReference` carries the `named` references (from, to,
  xpath) when the server names them, and `more` it counted without naming.
- `candidates` is the client's **deduction**, not the server's statement: for a missing
  reference the server does not name, the references that leave the source from the
  stopped island. It is empty when the server named them, since those come first, and it
  may be wrong on a server that counts a reference missing although its target is there.

Inside the push only, when a commit is refused the island's transaction is rolled back,
because some servers keep it open and refuse the next one. The library's own
`commit_transaction` never rolls back by itself: a caller that commits its own transaction
and is refused ends it with `rollback_transaction`.
