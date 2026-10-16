#!/bin/sh
# The same read-only tour as tour.sh, through the Docker image: probe, connect,
# dataspaces, resources of the first dataspace. Nothing is written to the server.
#
# Needs: docker, and ETP_URL (e.g. wss://etp.example.com/etp).
# RETP_IMAGE overrides the image (default ghcr.io/geosiris-technologies/retp:latest).
# If the server wants a login, export ETP_TOKEN (or ETP_PASSWORD and set
# ETP_LOGIN_ARGS="--user USER") first: `-e NAME` passes the variable through
# without writing its value; secrets are never arguments.
set -eu

if [ -z "${ETP_URL:-}" ]; then
  echo "tour-docker.sh: ETP_URL is not set (the server's WebSocket URL, wss://host/path)" >&2
  exit 2
fi
img=${RETP_IMAGE:-ghcr.io/geosiris-technologies/retp:latest}
login=${ETP_LOGIN_ARGS:-}

# Pass through only the secrets that are set.
envs=""
for v in ETP_TOKEN ETP_USER ETP_PASSWORD ETP_CLIENT_SECRET; do
  if [ -n "$(printenv "$v" 2>/dev/null || true)" ]; then envs="$envs -e $v"; fi
done

retp() {
  # shellcheck disable=SC2086
  docker run --rm $envs "$img" "$@"
}

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
  echo "tour-docker.sh: the server lists no dataspace" >&2
  exit 1
fi
echo "== resources of $first"
retp resources "$ETP_URL" $login --uri "$first" --depth 1
