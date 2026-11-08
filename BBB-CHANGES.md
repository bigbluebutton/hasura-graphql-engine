# BigBlueButton changes to Hasura GraphQL Engine

This repository is a fork of [hasura/graphql-engine](https://github.com/hasura/graphql-engine)
maintained by the [BigBlueButton](https://bigbluebutton.org) project. This file records what the
fork changes relative to upstream, as required by section 4(b) of the Apache License 2.0.

The fork and every modification listed here are distributed under the same
[Apache License 2.0](./LICENSE) as the upstream project. The upstream `LICENSE` and
`LICENSE-community` files are kept intact.

## Base version

- Branch `v2.50-bbb` is based on the upstream tag `v2.50.7`
  (commit `6ac41be02e7295f417006fe38173bfdefc80e518`).
- Release tags on this fork follow the pattern `v<upstream>-bbb.<N>` (for example
  `v2.50.7-bbb.1`), where `N` increments when the patch set, toolchain or build
  configuration changes on top of the same upstream version.
- The binary reports its version (via `/v1/version` and the logs) as
  `v<upstream>-bbb<N>`, for example `v2.50.7-bbb1`, so that a BigBlueButton build is
  always distinguishable from an official Hasura build.

## Build and release scripts (files added by this fork)

- `scripts/build-v2.sh`: builds the community edition `graphql-engine` executable from
  source with the toolchain the upstream tag declares (GHC 9.14.1, cabal-install 3.16.1.0)
  and the unmodified upstream `cabal.project` / `cabal.project.freeze`. The binary links
  the system `libgmp`, `libpq`, `libkrb5` and `libodbc` dynamically.
- `scripts/deploy-dev-host.sh`: development helper that deploys a locally built binary to a
  BigBlueButton development host over SSH.
- `.github/workflows/build-v2.yml`: GitHub Actions workflow that builds the binary for
  Ubuntu 22.04 (jammy) and Ubuntu 24.04 (noble) and, on `v*-bbb.*` tags, publishes a
  GitHub release with one binary per distro plus `SHA256SUMS` and per-distro build info.
- `BBB-CHANGES.md` (this file) and a fork notice at the top of `README.md`.

## Modified upstream files

None yet on this branch: the build scripts, the workflow and this file are additions, and the
removed upstream lint workflows are documented below. The Prometheus metrics patch (see the
`v2.48-bbb` branch for its previous iteration) is reapplied on top of this branch in a
follow-up change; when it lands, every modified upstream file carries a modification notice
at the top and is listed here.

- `.github/workflows/hlint.yml` and `.github/workflows/lint.yml` (removed): the first runs on
  a deprecated actions/checkout version and the second lints console code this fork does not
  touch.
