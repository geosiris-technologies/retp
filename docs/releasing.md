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
`etptypes` is not published for now: crates.io keeps its 1.x versions for the
projects that depend on them, and the facade waits.

## Publishing from a GitHub release

A published GitHub release publishes too. Its tag must name a version of the
manifests, `vX.Y.Z` for the workspace version or `<crate>-vX.Y.Z`, and its commit
must be on `main` or `dev`; otherwise the job stops before uploading anything.
The release pull request is still what prepares the versions and the changelogs:
merge it, then create the release on the merge commit.

The repository variable `RELEASE_PLZ_PUBLISH` chooses the trigger: `true`
publishes on a merge to `main` or `dev` and on a release, `release` on a release
only, and nothing is published while it is unset. A GitHub release marked as a
pre-release publishes to crates.io but starts neither the CLI binaries nor the
image.


The energyml crates are dependencies from crates.io, not by path: a version of
retp that needs an unreleased change of energyml-rust waits for that release.
