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

## Prometheus metrics endpoint (phase 1)

The community edition code already updates a full set of Prometheus metric references
(`Hasura.Server.Prometheus`), but never registers them to a metric store, so they cannot be
sampled. This fork registers those same references to a store from the bundled
`server/lib/ekg-prometheus` library and adds a `GET /v1/metrics` endpoint to the community
edition build. This endpoint is an implementation by the BigBlueButton project built on the
open source code.

Behavior and configuration:

- The endpoint is enabled by listing `metrics` in `HASURA_GRAPHQL_ENABLED_APIS` (the API
  enum already exists in the community edition); it returns 404 otherwise.
- `HASURA_GRAPHQL_METRICS_SECRET` secures the endpoint: requests must send
  `Authorization: Bearer <secret>` and receive 401 otherwise. The endpoint is fail-closed:
  if the secret is not set, every request is rejected with 401, so enabling the metrics API
  without configuring a secret exposes nothing.
- The response is `text/plain; version=0.0.4` and contains the Prometheus metrics sample
  followed by the metrics of the EKG store, converted to the Prometheus text format by
  `Hasura.Server.Prometheus.EkgExport`.
- In the EKG part of the output, metric names have their dots replaced by underscores, and
  the `rts.gc.*` subtree (statistics of the most recent GC) is renamed to `rts_gcdetails_*`:
  a plain dot replacement would merge it with the cumulative `rts.gc_*` metrics (for
  example, `rts.gc.elapsed_ns` and `rts.gc_elapsed_ns` would both become
  `rts_gc_elapsed_ns`). Consumers of the raw `/dev/ekg` names must account for this rename.
- The two parts of the output overlap on purpose: a few families appear once with the
  `hasura_` prefix (from the metric store) and once under their legacy EKG name, with
  independent sampling. The pairs are `hasura_active_subscriptions` and
  `active_subscriptions`, `hasura_websocket_connections` and `websocket_connections`,
  `hasura_http_connections` and `warp_threads` (the EKG side counts websockets in),
  `hasura_events_fetched_per_batch` and `events_fetched_per_batch`,
  `hasura_event_trigger_http_workers` and `num_event_trigger_http_workers`, plus the four
  event trigger distributions exported by EKG as summaries.
- GC/RTS metrics come from a single source: the EKG store (`rts_*` series, registered by
  upstream `Main.hs` via `EKG.registerGcMetrics`). They are deliberately not registered to
  the Prometheus store as well, to keep one family per name.
- Metric fields that are never incremented by community edition code
  (`hasura_cache_request_count`, `hasura_otel_*`) are not registered. Postgres connection
  pool metrics are not exported in this phase.

Exported metric families (names, labels, buckets) are defined in `PrometheusMetricsSpec`
(`server/src-lib/Hasura/Server/Prometheus.hs`) and pinned by the golden test
`server/src-test/Hasura/Server/PrometheusSpec.hs`.

## Modified upstream files

Every file below carries a modification notice at the top, as required by Apache License
2.0 section 4(b):

- `server/src-lib/Hasura/Server/Prometheus.hs`: adds `PrometheusMetricsSpec` (the static
  specification of every exported metric) and `makePrometheusMetrics` (creates the same
  mutable references as `makeDummyPrometheusMetrics`, registered to a store), plus a
  `pmSample` field to sample the store.
- `server/src-exec/Main.hs`: creates the Prometheus metric store and calls
  `makePrometheusMetrics` instead of `makeDummyPrometheusMetrics`.
- `server/src-lib/Hasura/Server/App.hs`: adds the `GET /v1/metrics` endpoint.
- `server/src-lib/Hasura/Server/Prometheus/EkgExport.hs` (new file): the EKG-to-Prometheus
  text conversion.
- `server/src-lib/Hasura/Server/Init/Arg/Command/Serve.hs`,
  `server/src-lib/Hasura/Server/Init.hs`, `server/src-lib/Hasura/Server/Init/Config.hs`,
  `server/src-lib/Hasura/App/State.hs`: add the `HASURA_GRAPHQL_METRICS_SECRET` option and
  carry it into the app context.
- `server/src-test/Hasura/Server/InitSpec.hs`: adds the new raw serve option default.
- `server/test-postgres/Constants.hs` and `server/lib/test-harness/src/Harness/Constants.hs`:
  add the new serve option default to the test fixtures.
- `server/src-test/Hasura/Server/PrometheusSpec.hs` (new file): tests that every metric
  registers without a `RegistrationError` and that the store exports exactly the expected
  metric families once every vector metric has been observed.
- `server/src-test/Hasura/Server/Prometheus/EkgExportSpec.hs` (new file): tests name
  uniqueness and the name grammar of the EKG conversion.
- `server/graphql-engine.cabal`: registers the new modules.
- `.github/workflows/hlint.yml` and `.github/workflows/lint.yml` (removed): upstream CI
  workflows that are not applicable to this fork.
