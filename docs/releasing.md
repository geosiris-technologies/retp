# Releasing

How the crates, the CLI archives, the image and the Python wheels of this
repository are published. Publication runs from the CI of this repository, on its
default branch, with credentials only protected workflows can read (`release-plz`,
trusted publishing). The first publication of each crate is done once, in order, by a
maintainer; every later release goes through the workflows.

## Making a release

**A published GitHub release is what publishes**, everything at once:

1. **Merge the release pull request.** `release-plz` keeps one open (opened by the
   release bot, a GitHub App): it bumps the versions, the requirements between crates
   and every `CHANGELOG.md`. Merging it publishes nothing: the repository variable
   `RELEASE_PLZ_PUBLISH` is `release`.
2. **Create a GitHub release** (Releases > Draft a new release): a new tag `vX.Y.Z`, the
   workspace version the merge left in `Cargo.toml`, on `main`; title `vX.Y.Z`; notes
   ("Generate release notes" gives a start). Publish it.
3. Publishing it starts, each from the release:

| What | Where | Workflow |
|---|---|---|
| the crates (see the order below) | crates.io | `release-plz.yml`, which also tags each crate `<crate>-vX.Y.Z` |
| the `retp` archives, one per target, with `SHA256SUMS` | attached to this release | `release-cli.yml` |
| the `retp` image, tags `X.Y.Z`, `X.Y` and `latest` | `ghcr.io/geosiris-technologies/retp` | `docker.yml`, once `IMAGE_PUBLISH` is `true` |
| the Python wheels and sdist | PyPI | `python.yml`, once `PYPI_PUBLISH` is `true` |
| not published | | `retp-tests`, `etptypes` (for now), `retp-python` as a crate (it ships as wheels) |

Each workflow first checks that the tag names the version of the manifests, and stops
before uploading anything otherwise: a release created on a commit that is still at the
previous version would publish the old code under the new name. A release marked as a
pre-release publishes the same way but gives the image no `latest` and no `X.Y` tag; it
does not change the version: a pre-release version (`0.2.0-alpha.1`) is set in
`Cargo.toml` first.

Between releases, every push to `main` that touches the crates publishes a development
build of the wheels to TestPyPI (`X.Y.(Z+1).devN`, once `TESTPYPI_PUBLISH` is `true`),
and the image `edge` (once `IMAGE_PUBLISH` is `true`).

The repository variables and secrets this relies on:

| Name | Kind | Role |
|---|---|---|
| `RELEASE_PLZ_PUBLISH` | variable | `release`: publish crates on a release only; `true`: also on a merge to `main` or `dev`; unset: nothing |
| `IMAGE_PUBLISH` | variable | `true` pushes the image |
| `PYPI_PUBLISH`, `TESTPYPI_PUBLISH` | variables | `true` uploads the wheels to PyPI, to TestPyPI |
| `RELEASE_BOT_CLIENT_ID`, `RELEASE_BOT_PRIVATE_KEY` | variable, secret | the GitHub App that opens the release pull request, so that its checks run |

## Trusted publishing

No registry token is stored. The `release` job of `release-plz.yml` has
`id-token: write` and release-plz exchanges the workflow's OIDC token for a
short-lived crates.io token. This needs, for **each** crate, a trusted publisher
configured on crates.io (crate settings, *Trusted Publishing*): owner
`geosiris-technologies`, repository `retp`, workflow file `release-plz.yml`,
environment `crates-io`. crates.io requires the crate to exist already, so the first
publication of a crate is made by hand with a short-lived token; trusted publishing is
configured right after, and only then is the repository variable `RELEASE_PLZ_PUBLISH`
set.

The wheels go through PyPI trusted publishing too: a publisher on PyPI (environment
`pypi`) and one on TestPyPI (environment `testpypi`), both for this repository and the
workflow file `python.yml`.

## Publication order

A crate is published only when everything it depends on is on crates.io. Order
from the manifests (normal and build dependencies decide it). A dev-dependency on a
crate of this repository carries **no** `version` (`{ path = "../retp-avro" }`):
`cargo publish` drops it from the published manifest, so it does not constrain the
order. A dev-dependency with a `version` is resolved against the registry, and stopped a
release of the sibling repository (`energyml-rust`) at a crate whose tests use crates
not yet published. Do not add a `version` to one.

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

The energyml crates are dependencies from crates.io, not by path: a version of
retp that needs an unreleased change of energyml-rust waits for that release.

## The publication switch

The repository variable `RELEASE_PLZ_PUBLISH` chooses when crates.io is published
to: `release` on a published GitHub release only; `true` also on every merge to
`main` or `dev`; nothing while it is unset. A release's tag must name a version of the
manifests, `vX.Y.Z` for the workspace version or `<crate>-vX.Y.Z`, and its commit must
be on `main` or `dev`.

With `true`, a merge publishes the crates and tags them with the workflow token, and a
tag pushed with that token starts no workflow on its own: the `release` job of
`release-plz.yml` then starts `release-cli.yml` and `docker.yml` itself
(`workflow_dispatch` on the new `retp-cli-v*` tag). On a GitHub release it does not,
since those workflows listen for the release themselves and would otherwise run twice.

## What the builds contain

The CLI archives and the wheels are built with the `dist` profile of the workspace
(`Cargo.toml`: thin LTO, 16 codegen units): the release profile's fat LTO with one
codegen unit ran GitHub runners out of memory while linking, and took more than half an
hour per wheel. They are built with `hdf5-static`: HDF5 is compiled from its sources and
linked in (CMake and a C compiler at build time; CMake 3.26 or newer, which the musllinux
build images do not carry and the workflow installs). They then run on a machine without
HDF5. The musl archives are built without it and refuse to write an array by name
(`FeatureNotEnabled`). The container image links the system HDF5 of its Debian base.

## The image on GHCR

A package created by a workflow starts private. The organisation allows public packages:
after the first push, make `ghcr.io/geosiris-technologies/retp` public in the package
settings (and check that it is linked to this repository). Pushes after that keep the
setting.

## Checks that run before a release

- `ci.yml` runs `cargo deny check` on every push and pull request; `deny.yml` runs it
  every day, so that a new advisory shows before a release does.
- The release pull request's semver check builds the documentation of every changed
  crate with all its features, `hdf5` included; the workflow installs `libhdf5-dev` for it.
- `generated-guard.yml` accepts the release pull request's edits of `crates/generated/`
  (changelogs, and `version` values in a `Cargo.toml`) and refuses any other hand edit.

## Python wheels

The metadata of the wheels is in `crates/retp-python/pyproject.toml` (the distribution and
the import are both `retp`). A GitHub release `vX.Y.Z` builds the wheels for eight
targets and the sdist, with HDF5 compiled in, and uploads them by PyPI trusted
publishing (environment `pypi`). The tag `python-vX.Y.Z` does the same for the Python
package alone. Either tag must equal the version of the crate; the `version` job fails
otherwise. Development builds go to TestPyPI (environment `testpypi`) from every push to
`main` or `dev`.

When a command line gives maturin `--features`, it replaces the list of
`pyproject.toml` instead of adding to it: the workflow passes the whole list. The licence
is declared with PEP 639 (`license` and `license-files`): left to find the files, maturin
recorded a `License-File` that was not in the sdist, and the index refused it; the sdist
job checks it.
