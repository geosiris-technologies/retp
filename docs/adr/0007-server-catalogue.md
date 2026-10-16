# 7. The server catalogue: servers by name, secrets by reference

> `retp-catalog` is a crate of its own, native only. The client does not depend
> on it; it depends on the client.

## Context

A person who talks to the same few servers every day types, each time, a URL, a
partition, a tenant, a client id and the way to log in, and keeps the secrets in
environment variables or, worse, on the command line (which ends up in the
shell's history). `retp azure` should be enough.

Two things make this delicate. A catalogue is a file full of the things an
attacker wants, so the first rule is that it never holds a secret. And the
client already knows how to log in (`Choice`, `Login`): a second, parallel way
to decide the method would drift from the first.

## Decision

- **A crate of its own, `retp-catalog`, native only.** `retp-client` compiles for
  the browser and must stay free of files, `directories` and `toml`. The CLI and
  `retp-conformance` call the catalogue and hold no logic of their own; a later
  Python bridge will call it too. Texts (`show`, `list`, `test`) are returned as
  strings, so a test reads exactly what a person would see.
- **One folder per server.** A configuration file (`RETP_CONFIG`, else the OS's
  configuration folder through `directories`) holds only the catalogue folder
  and the default server; `RETP_CATALOG` overrides the folder. In the folder,
  `NAME/server.toml` is the definition, shareable (no secret), and `NAME/state/`
  is local: the record of the last `server test`, the history of the shell.
  `retp catalog init` chooses the folder; nothing is created before.
- **A name is a folder name.** `[a-z0-9][a-z0-9._-]{0,62}`, no `..`, no Windows
  device name. Aliases share the namespace of names. A name or alias that is a
  command of `retp` is refused at the add, the list of commands being **read
  from the parser**, never copied.
- **An entry holds references, never values.** A secret is `{ env = "NAME" }` or
  `{ file = "PATH" }` in `server.toml`, `env:NAME` or `file:PATH` on a command
  line. The keyring, a command and a prompt are named in the design and refused
  by name ("not available yet") until they exist. A literal value for
  `password`, `token`, `client_secret` or `secret` is refused when the file is
  read, naming the key and the line, **never the value**: the messages of the
  TOML parser quote the source, so they are never shown as they are (a line
  number and a message with every quoted piece blanked). A URL with credentials
  is refused for the same reason: URLs appear in error messages. A text given
  where a reference was asked is not repeated, since it may be the secret.
- **A file secret** loses one final line ending and not two, is refused beyond
  mode `0600` on Unix, and is read at each connection, so a renewed file is
  taken into account by `reconnect`. TODO(verify): the equivalent check on
  Windows.
- **A `Secret` of our own, not `secrecy`.** The value lives in a `Secret`: no
  `Display`, no `Clone`, a `Debug` that shows nothing, memory overwritten when it
  is dropped. The overwrite is safe Rust with `std::hint::black_box`, a best
  effort and not the guarantee of a volatile write; it cannot reach the `String`s
  of `Choice` and `Login` in `retp-client` or the buffers of the system. The
  value leaves its `Secret` at **one** call site, which a test counts in the
  source.
- **One door to the login.** An entry fills the `Choice` of `retp-client` and
  `Choice::login` decides, as it does for the command line. The method an entry
  declares (`none`, `basic`, `bearer`, `oauth-password`, `oauth-client`, `azure`,
  `cognito`) must be the one `Choice::login` gives: two sources of one fact.
  `Choice::login` is called with `|_| None`: **the ambient `ETP_*` variables are
  ignored** for a catalogue server, or the `ETP_TOKEN` set for one server would
  go to another. An entry that wants one says so, with `{ env = "ETP_TOKEN" }`.
  Credentials go to the entry's own URLs only.
- **The command line is rewritten before it is parsed.** `retp azure COMMAND`,
  `retp @azure COMMAND` and `retp --server azure COMMAND` (`-s`, and a hidden
  `--serveur`) become the line `retp` already parses: the entry's URL as the
  first positional, a hidden `--catalog-entry NAME`, and the entry's non-secret
  options that the line did not give (the line wins). No secret passes through
  the argument vector. A built-in command always wins over a server of the same
  name; `@name` always means the server and survives the day a command of that
  name is added. A first word that is neither gets the commands and servers
  that look like it. A login option (`--password`...) given with a catalogue
  server is refused: the entry is the only source of its login.
- **A default server never receives a write.** `retp server default NAME` serves
  the commands that read; `push`, `create-dataspace` and `delete-dataspace` need
  a server named on the line. Every command that took the default says so on
  stderr.
- **`protected = true`.** A command that writes to a protected server asks for the
  server's name typed at a terminal, or `--yes`; refused by name otherwise. In a
  shell, whose input is the command stream, only `--yes` confirms.
- **`--save-as NAME`** saves the connection **after** the command succeeded, with
  its non-secret options and a reference for each secret. A value that came from
  an `ETP_*` variable becomes a reference to that variable (said to be shared
  with every other server); a value that came from the line is not saved at all
  and the entry points at a variable of its own name, with the advice to define
  it. A name already taken is refused before the command runs.
- **The shell connects by name.** `retp shell azure`, `retp @azure shell`;
  `reconnect` opens a new session on the same server and reads the references
  again; `connect NAME` changes server without leaving, the new session being
  opened before the old one is closed.
- **Extra headers and write defaults are fields of `server.toml`.** `[headers]`
  holds extra request headers: a literal for a header that carries nothing
  secret, a reference for one that does; a literal is refused for a name that
  looks secret (`authorization`, `cookie`, anything with `key`, `token`,
  `secret` or `password`). `[defaults]` holds `legal_tags`, `owners` and
  `viewers`, not secrets, added as `--custom` entries to `create-dataspace` unless
  the line gives its own. TODO(verify): `retp-client` has no way to send extra
  headers yet. Until it has, a connection to an entry that defines some is
  **refused by name** rather than made without them.
- **A guided form.** `retp server add` with no argument, on a terminal, asks for
  each field with plain prompts (no new dependency); a secret is asked for as a
  reference, and what is typed is checked as one. It probes `/.well-known` before
  saving. With arguments, the same fields from the options, and a script is
  never asked anything: a missing field is refused by name when the input is not
  a terminal.
- **The redaction is a guard, not a defence.** What a server answers may repeat
  what it was sent (`error_description` echoing a form). The errors the catalogue
  shows and records have the literal occurrences of the entry's secrets removed.
  This catches the echo; it is not a proof, and no component is asked to vouch
  for itself: the leak test below looks from outside.

## Consequences

- The test that matters is the leak test (`crates/retp-catalog/tests/leak.rs`): a
  made-up canary goes through every operation, and neither a string a person
  would see, nor a log line, nor a byte of any file of the catalogue contains it.
  It shows that it can see (the canary is found where it is meant to be) and it
  is broken on purpose by hand before it is trusted.
- Logging is `tracing`, like the rest of retp. What the catalogue logs names a
  reference and never a value, in its text and in its fields; the leak test
  captures every event with all its fields.
- `retp-conformance --server NAME` and `retp mcp --server NAME` use the same
  expansion, and the same resolution; the ambient variables are ignored there
  too.
- Not in this version, and named when asked for: the OS keyring, commands and
  prompts as secret sources; `server edit`, `rename`, `login`, `exec`, `secret`;
  caching of tokens; flows that need a person (code with PKCE, device code); the
  Python bridge. The `Choice` and `Login` of `retp-client` still hold ordinary
  `String`s; moving them to `Secret` is a change of the client.
