# Releasing

Publication runs from the CI of this repository, on its default branch, with
credentials only protected workflows can read (`release-plz`, trusted
publishing). The first publication of each crate is done once, in order, by a
maintainer; every later release goes through the workflow.

## Trusted publishing

No registry token is stored. The `release` job of `release-plz.yml` has
`id-token: write` and release-plz exchanges the workflow's OIDC token for a
short-lived crates.io token. This needs, for **each** crate, a trusted publisher
configured on crates.io (crate settings, *Trusted Publishing*): owner
`geosiris-technologies`, repository `retp`, workflow file `release-plz.yml`.
crates.io requires the crate to exist already, so the first publication of a
crate is made by hand with a short-lived token; trusted publishing is configured
right after, and only then is the repository variable `RELEASE_PLZ_PUBLISH` set
to `true`.

Tags `retp-cli-v*` are created by release-plz, which then starts
`release-cli.yml` and `docker.yml` on the tag itself (a tag pushed with the
workflow token starts no workflow on its own).

## Publication order

A crate is published only when everything it depends on is on crates.io. Order
from the manifests (normal, build and dev dependencies):

| Level | Crates |
|---|---|
| 0 | in the [`energyml-rust`](https://github.com/geosiris-technologies/energyml-rust) repository, first: `energyml-core`, `energyml-storage`, `energyml-schemas` and what they need (see its [releasing page](https://github.com/geosiris-technologies/energyml-rust/blob/main/docs/releasing.md)) |
| 1 | `retp-derive` |
| 2 | `retp-avro` |
| 3 | `retp-types` |
| 4 | `retp-proto`, `etptypes` |
| 5 | `retp-client` |
| 6 | `energyml-etp-store` |
| 7 | `retp-cli`, `retp-conformance` |

`retp-tests` is never published.

The energyml crates are dependencies from crates.io, not by path: a version of
retp that needs an unreleased change of energyml-rust waits for that release.
