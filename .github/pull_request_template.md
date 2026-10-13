## What and why

<!-- One or two sentences. Link the issue. -->

## Checklist

- [ ] `crates/generated/` and `rc/etp-1.2/` are untouched (they are synchronised, never edited by hand)
- [ ] hand-written crates formatted by name (never `cargo fmt --all`)
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` is clean
- [ ] `cargo test --workspace -j 4 -- --test-threads=4` passes
- [ ] a new check or rule cites the specification (document, section, paragraph) in a comment
- [ ] a new test was seen to fail when the code it covers is broken
- [ ] code, comments and messages are in English
