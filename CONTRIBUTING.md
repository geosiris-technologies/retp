# Contributing to retp

Thanks for your interest. A few things first.

- **Issues are welcome**: bugs, questions, server behaviours the client should
  tolerate or refuse, missing protocol features. Use the templates.
- **Pull requests**: open an issue first for anything beyond a small fix.
- **`crates/generated/` is never edited by hand.** It is produced by an
  internal Geosiris tool from the ETP v1.2 Avro schemas, and a pull request
  that touches it is refused by CI. Report a problem with generated types as
  an issue; it is fixed in the generator and arrives as a synchronised commit.

## Conventions

This project follows the conventions of
[energyml-rust](https://github.com/geosiris-technologies/energyml-rust), the
sibling library it builds on:

- refuse rather than guess: where the specification is ambiguous, the answer is
  a named refusal, not a plausible value;
- tests follow the specification, not a particular server: a test states a
  rule and cites the document, section and paragraph in a comment;
- a test that cannot fail proves nothing: when adding one, break the code on
  purpose to see it turn red;
- registries, not `match`, where the list will grow;
- code, comments and messages are in English.

## Commit messages

Commits follow [Conventional Commits 1.0](https://www.conventionalcommits.org/en/v1.0.0/), the
convention release-plz reads to choose the next version and write the CHANGELOG:

```
<type>(<scope>): <summary in the imperative, lower case, no final full stop>

<why, wrapped at 100 columns>

BREAKING CHANGE: <what a caller must change>   (only for a breaking change)
```

- **type**: `feat` (a new capability, a minor version), `fix` (a bug fix, a patch version),
  `perf`, `refactor`, `docs`, `test`, `build`, `ci`, `chore`, `revert`.
- **scope**: the crate without its `energyml-` prefix (`storage`, `build`, `cli`, `schemas`), or
  a broader word when several crates move together (`deps`, `ci`).
- **breaking change**: a `!` after the scope (`feat(storage)!: ...`) and a `BREAKING CHANGE:` line
  in the body. Before 1.0, a breaking change raises the minor version; `cargo-semver-checks` also
  runs in CI and flags an API break the message did not announce.

## Before you push

```sh
cargo fmt -p <the hand-written crate you touched>   # never `cargo fmt --all`
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace -j 4 -- --test-threads=4
cargo check --target wasm32-unknown-unknown -p retp-client
```

`cargo fmt --all` would rewrite the generated crates. Format the hand-written
crates by name.

By contributing you agree that your contribution is licensed under the
Apache License 2.0, as the project is.
