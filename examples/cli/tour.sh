#!/bin/sh
# A read-only tour of an ETP v1.2 server with the native `retp` command:
# probe, connect, dataspaces, resources of the first dataspace.
# Nothing is written to the server.
#
# Needs: `retp` on the PATH, and ETP_URL (e.g. wss://etp.example.com/etp).
# If the server wants a login, export ETP_TOKEN (or ETP_PASSWORD and set
# ETP_LOGIN_ARGS="--user USER") first; secrets are never arguments.
set -eu

if [ -z "${ETP_URL:-}" ]; then
  echo "tour.sh: ETP_URL is not set (the server's WebSocket URL, wss://host/path)" >&2
  exit 2
fi
# Options that carry no secret, if any (word-split on purpose).
login=${ETP_LOGIN_ARGS:-}

echo "== probe"
retp probe "$ETP_URL" $login || echo "(the server refused the probe: some ask for another login there; going on)"
echo "== connect"
retp connect "$ETP_URL" $login
echo "== dataspaces"
list=$(retp dataspaces "$ETP_URL" $login)
printf '%s\n' "$list"

# `dataspaces` prints one line per dataspace, its URI first then its path: the first
# word of the first line is the URI that --uri takes.
first=$(printf '%s\n' "$list" | sed -n '1p' | awk '{print $1}')
if [ -z "$first" ]; then
  echo "tour.sh: the server lists no dataspace" >&2
  exit 1
fi
echo "== resources of $first"
retp resources "$ETP_URL" $login --uri "$first" --depth 1
