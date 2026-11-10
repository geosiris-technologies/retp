# 8. The headers a server wants are a property of the server

> One list on the endpoint, one door to build it, and every request to the
> server carries it. The data partition of a Reservoir DDMS is one of them.

## Context

Some servers want a header that ETP does not define. OSDU's Reservoir DDMS wants
`data-partition-id`; a gateway in front of a server wants a tenant, a routing
key, an API key. The client knew one of them, by name: the profile
`ServerProfile::Rddms` added the partition. Any other could be written in a
catalogue entry (`[headers]`), but the client had no way to send it, so a
connection to such an entry was refused by name (ADR 7).

ETP 1.2 says nothing of a header that is a server's own. What it says of its own
handshake attributes is followed for these too: they are header properties
(section 4.3, step 4), and a browser-based client, which cannot add request
headers, "MUST specify them as query string parameters", URL-encoded (section
4.3.2).

A header is also a place where a secret travels, and a way to break a request: a
line break in a value starts another header, and a second `authorization` or
`sec-websocket-protocol` replaces what the client meant to send.

## Decision

- **A list on the endpoint.** `Endpoint::headers` is an `ExtraHeaders`. Which
  headers a server expects is a property of the server, like its URL: not of a
  login, and not of one request.
- **Every request to the server carries them**: the WebSocket handshake, of a
  blocking session and of a live one, the probe of
  `/.well-known/etp-server-capabilities`, each call of the REST API. A header
  that only some requests carried would make a server answer the probe and the
  session differently.
- **One door, which checks at the door.** An `ExtraHeaders` cannot hold a header
  that cannot be sent. Refused when it is added, by name (`HeaderRefusal`, which
  never carries a value): a name that is not letters, digits, `-` and `_`; a
  value with a control character; a header the client writes itself
  (`etp-encoding`, `host`, `sec-websocket-*`...); the same name twice, whatever
  the case. The catalogue checks an entry through the same door when the file is
  read.
- **The partition is a header like any other.** `ServerProfile::Rddms` comes down
  to `data-partition-id` and is merged into the same list
  (`Endpoint::additions`); `--partition P` is `--header data-partition-id=P`.
  Given both ways, it is refused rather than sent twice.
- **`authorization` is not an extra header.** Credentials have a path of their
  own (`Credentials`, a login, the renewal of an open session). An
  `authorization` given as a header would go around all of it, so it is refused,
  with the advice to give credentials.
- **A secret is never written in a URL.** The value of a header is a secret when
  it is added as one (`with_secret`: the catalogue does for a reference) or when
  its name says so. It is then wiped from memory when dropped, absent from every
  `Debug`, and refused for the handshake of a front end, whose attributes go in
  the query string: a URL is kept by proxies, servers and histories. The probe is
  an ordinary HTTP request, which a front end's `fetch` can give headers to, so
  it sends headers under either profile.
- **Refused before the login.** What an endpoint would refuse about its headers
  is known with nothing sent (`Endpoint::sendable`). The command line, the
  catalogue and `retp-conformance` ask before they log in: a login costs a
  request, and is not spent on a line that cannot be sent.
- **The command line adds, it does not replace.** `--header NAME=VALUE` is
  repeatable. With a catalogue server its headers are added to the entry's, and
  one the entry already has is refused: two values for one header is a question
  for the person, not a rule of precedence to remember.
- **`--save-as` keeps the headers**, by the catalogue's rule for a literal: the
  value when the name does not say it is a secret, a reference to a variable of
  its own when it does (the value was on the command line, and is not saved).
- **`retp mcp` refuses a header that carries a secret by its name**, as it
  refuses `--token`: an MCP client writes the command line it launches into a
  configuration file. Such a header is kept in a catalogue entry, by reference.

## Consequences

- The test that matters reads what a bare TCP listener received
  (`crates/retp-client/tests/extra_headers.rs`): the same three headers on the
  WebSocket handshake, the probe, a REST call and a live session; nothing of
  them in a URL; no socket opened when a secret would go in a query string. The
  rules that guard a secret were each broken on purpose once, to see the test
  fail.
- The `Debug` of a `Handshake` shows the value of ETP's own attributes only. It
  printed every header, `authorization` among them.
- `native::rest_client` can refuse (it returns a `Result`), and the probe has a
  refusal of its own (`ProbeError::Header`).
- The name of a header is written in lower case in a WebSocket handshake, as ETP
  asks of its own (section 4.3, step 4.c), and as given elsewhere; HTTP does not
  tell the two apart. A value is always sent as given.
- A catalogue file keeps no order between its headers (a TOML table): they are
  sent in the order of their names.
- Not in this version: the Python bridge takes the partition and no other
  header; the guided form of `retp server add` probes the URL alone, before the
  entry exists (`retp server test` then asks with the entry's headers).
