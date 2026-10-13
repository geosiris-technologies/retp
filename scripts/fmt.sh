#!/usr/bin/env bash
# Format every hand-written crate with rustfmt, crate by crate; `--check` only reports
# (what CI runs). Never `cargo fmt --all`: it would also rewrite `crates/generated/`, whose
# bytes are the generator's. The list is read from the workspace, so a new crate is
# formatted the day it exists.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

packages=()
for manifest in crates/*/Cargo.toml; do
  name=$(sed -n 's/^name *= *"\(.*\)"/\1/p' "$manifest" | head -n 1)
  [ -n "$name" ] && packages+=(-p "$name")
done

if [ "${1:-}" = "--check" ]; then
  cargo fmt "${packages[@]}" -- --check
else
  cargo fmt "${packages[@]}"
fi
