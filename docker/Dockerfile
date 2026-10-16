# syntax=docker/dockerfile:1
#
# `retp` and `retp-conformance` in a container: probe and talk to an ETP v1.2
# server, or check it against the specification.
#
#   docker run --rm ghcr.io/geosiris-technologies/retp probe wss://etp.example.com
#   docker run --rm -e ETP_TOKEN -v "$PWD:/work" --entrypoint retp-conformance \
#     ghcr.io/geosiris-technologies/retp wss://etp.example.com --out report.md
#
# `retp` is built with `hdf5` and `mcp` (`retp mcp` needs `docker run -i`). See docs/docker.md.
#
# Built by `.github/workflows/docker.yml` only.
#
# Build context: this repository; the energyml crates come from crates.io.

FROM rust:1-bookworm AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends libhdf5-dev pkg-config \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src/retp
COPY . .
ARG RELEASE_LTO=fat
ENV CARGO_PROFILE_RELEASE_LTO=${RELEASE_LTO}
RUN --mount=type=cache,target=/usr/local/cargo/registry/index,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/registry/cache,sharing=locked \
    set -eu; \
    cargo build --release --locked -p retp-cli -p retp-conformance \
      --features retp-cli/hdf5,retp-cli/mcp; \
    mkdir /out; \
    cp target/release/retp target/release/retp-conformance /out/

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends libhdf5-103-1 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
ARG REVISION=unknown
ARG VERSION=dev
LABEL org.opencontainers.image.title="retp" \
      org.opencontainers.image.description="An ETP v1.2 client and conformance checker, in Rust" \
      org.opencontainers.image.source="https://github.com/geosiris-technologies/retp" \
      org.opencontainers.image.url="https://www.geosiris.com" \
      org.opencontainers.image.vendor="Geosiris" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION"
ENV LANG=C.UTF-8
RUN useradd --uid 1000 --create-home retp
USER retp
WORKDIR /work
ENTRYPOINT ["retp"]
CMD ["--help"]
