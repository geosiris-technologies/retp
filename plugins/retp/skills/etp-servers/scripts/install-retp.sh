#!/bin/sh
# Install the `retp` command line from a GitHub release of geosiris-technologies/retp,
# on Linux or macOS.
#
# The archive is checked against the release's SHA256SUMS before anything is installed:
# a missing entry or a different checksum stops the script, and nothing is written outside
# a temporary folder. `retp` goes into a folder of the user (~/.local/bin by default); the
# PATH is only changed with --add-to-path.
#
#   sh install-retp.sh                      # the latest release, into ~/.local/bin
#   sh install-retp.sh --version 0.3.0      # a given release
#   sh install-retp.sh --dir ~/bin --add-to-path
#
# The release archives hold `retp` only. `retp-conformance` comes with
# `cargo install retp-conformance`, or the image ghcr.io/geosiris-technologies/retp
# (`docker run --rm --entrypoint retp-conformance ...`).
set -eu

repo="geosiris-technologies/retp"
version=""
dir="${HOME}/.local/bin"
target=""
add_to_path=0
force=0

usage() {
  cat <<'EOF'
Usage: install-retp.sh [options]

  --version X.Y.Z   the release to install (default: the latest)
  --dir DIR         where to put `retp` (default: ~/.local/bin)
  --target TRIPLE   the archive's target, e.g. x86_64-unknown-linux-musl (default: detected)
  --add-to-path     add DIR to the PATH in ~/.profile (never done otherwise)
  --force           replace an existing `retp` in DIR
  -h, --help        this help
EOF
}

fail() {
  echo "install-retp.sh: $*" >&2
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version) [ $# -ge 2 ] || fail "--version needs a value"; version=${2#v}; shift 2 ;;
    --dir) [ $# -ge 2 ] || fail "--dir needs a value"; dir=$2; shift 2 ;;
    --target) [ $# -ge 2 ] || fail "--target needs a value"; target=$2; shift 2 ;;
    --add-to-path) add_to_path=1; shift ;;
    --force) force=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; fail "unknown option: $1" ;;
  esac
done

command -v curl >/dev/null 2>&1 || fail "curl is needed"
command -v tar >/dev/null 2>&1 || fail "tar is needed"
if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  sha256() { shasum -a 256 "$1" | awk '{print $1}'; }
else
  fail "sha256sum or shasum is needed to check the archive"
fi

# The target triple of this machine, as the release names its archives.
if [ -z "$target" ]; then
  case "$(uname -m)" in
    x86_64|amd64) arch=x86_64 ;;
    aarch64|arm64) arch=aarch64 ;;
    *) fail "no release archive for the architecture $(uname -m): try \`cargo install retp-cli --features mcp\`" ;;
  esac
  case "$(uname -s)" in
    Linux)
      # The musl archives are for systems without glibc (Alpine...); they are built
      # without HDF5, so `download-epc` there cannot write arrays.
      if (ldd --version 2>&1 || true) | grep -qi musl; then
        target="$arch-unknown-linux-musl"
      else
        target="$arch-unknown-linux-gnu"
      fi
      ;;
    Darwin) target="$arch-apple-darwin" ;;
    *) fail "no release archive for $(uname -s): on Windows use install-retp.ps1" ;;
  esac
fi

# The latest release: where github.com/<repo>/releases/latest redirects to.
if [ -z "$version" ]; then
  latest=$(curl -sSfLI -o /dev/null -w '%{url_effective}' "https://github.com/$repo/releases/latest") ||
    fail "could not reach GitHub to find the latest release"
  version=${latest##*/tag/v}
  case "$version" in
    ''|*/*) fail "could not read the latest release from $latest: give --version" ;;
  esac
fi

# Refused before downloading anything.
if [ -e "$dir/retp" ] && [ "$force" -ne 1 ]; then
  fail "$dir/retp already exists (version: $("$dir/retp" --version 2>/dev/null || echo unknown)): add --force to replace it"
fi

archive="retp-v$version-$target.tar.gz"
base="https://github.com/$repo/releases/download/v$version"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT INT TERM

echo "downloading $archive (release v$version)" >&2
curl -sSfL -o "$work/$archive" "$base/$archive" || fail "no archive $archive in release v$version"
curl -sSfL -o "$work/SHA256SUMS" "$base/SHA256SUMS" || fail "release v$version has no SHA256SUMS: not installing"

# The line of SHA256SUMS that names this archive exactly (`hash  name` or `hash *name`).
expected=$(awk -v name="$archive" '{ file = $2; sub(/^\*/, "", file); if (file == name) print $1 }' "$work/SHA256SUMS")
[ -n "$expected" ] || fail "SHA256SUMS of v$version does not list $archive: not installing"
actual=$(sha256 "$work/$archive")
if [ "$expected" != "$actual" ]; then
  fail "checksum mismatch for $archive (expected $expected, got $actual): not installing"
fi
echo "checksum verified: $actual" >&2

tar -xzf "$work/$archive" -C "$work"
binary="$work/retp-v$version-$target/retp"
[ -f "$binary" ] || fail "the archive does not hold retp-v$version-$target/retp"

mkdir -p "$dir"
cp "$binary" "$dir/retp.new"
chmod 0755 "$dir/retp.new"
mv -f "$dir/retp.new" "$dir/retp"
echo "installed $("$dir/retp" --version) into $dir" >&2

case ":$PATH:" in
  *":$dir:"*) ;;
  *)
    if [ "$add_to_path" -eq 1 ]; then
      line="export PATH=\"$dir:\$PATH\""
      if ! grep -qsF "$line" "$HOME/.profile"; then
        printf '\n# added by install-retp.sh\n%s\n' "$line" >>"$HOME/.profile"
      fi
      echo "added $dir to the PATH in ~/.profile: open a new shell, or run: $line" >&2
    else
      echo "$dir is not on the PATH: call $dir/retp, or rerun with --add-to-path" >&2
    fi
    ;;
esac
