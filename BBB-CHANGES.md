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

## Live query first-result race (upstream issue hasura/graphql-engine#10952)

Since upstream v2.50.4, `onChange` in `Hasura.GraphQL.Transport.WebSocket` drops any
push whose operation id is not in the connection's operation map, and the operation is
only inserted into that map after `addLiveQuery` returns. When the poller runs in that
window (a CPU-starved host makes it likely), the subscription's first result is
discarded, and `pushResultToCohort` then treats the subscriber as already served, so
nothing is sent until the result actually changes. The fix replaces the map lookup
with a per-operation liveness flag (`STM.TVar Bool`) created before `addLiveQuery`
and cleared in the same STM transaction that removes the operation from the map, so
the startup window delivers and a straggler poll after teardown is still dropped.

## Operation name count of streaming subscriptions

Each poller keeps a count of its subscribers per operation name, and the poll metrics use
the names in that count as their `operation_name` label. `removeStreamingQuery` looks the
removed subscriber up in every cohort of the poller, and it decremented the count once per
cohort it visited rather than once for the subscriber, so with subscribers of one name in
several cohorts the name left the count while some of them were still active (live
queries were not affected: `removeLiveQuery` already decrements once). The count is now
decremented once, and only when the subscriber is still in one of the poller's cohorts.

A subscriber can end up in two cohorts of a streaming poller, against the design the
poller documents. When a subscriber joins a cohort while a poll is advancing that cohort's
cursor, the comment in the cohort map rebuild of `pollStreamingQuery` says it stays only in
a cohort under the old key, to get its first result from the initial cursor. The code
creates that cohort, but the cohort it moves to the new key is the same object as the
original, and the filter that keeps its subscribers keeps the newcomer too, so the newcomer
is in both until their keys coincide again. This deviation is upstream behavior and is
not changed here; the metrics handle it. `removeStreamingQuery` also decremented
`hasura_active_subscriptions` once per cohort the subscriber was in, so such a subscriber
was counted down twice. It is now counted down once, under the same condition as the
operation name. Live queries look the subscriber up in a single cohort and decrement once.
When the removal empties the poller and its thread is not installed (a state the code logs
as a bug), the base did not count the gauge down, for live queries or streaming
subscriptions, and now it does for both, which matches the unconditional count up when the
subscriber was added.

## Streaming subscription plan built once

For a streaming subscription, `buildSubscriptionPlan` in
`Hasura.GraphQL.Execute` ran the plan builder twice, once for the model info and once for
the plan, and each run validates the subscription's variables against Postgres. The plan
is now built once and both parts come from that result.

## One observation per subscription poll

`recordSubscriptionMetric` records the total and database times of a subscription poll
for every operation name the poller serves. With granular metrics off, which drops the
name from the label, it still recorded once per name, so a poll serving three names was
observed three times in the unlabeled series. It now records once per name with granular
metrics on and once per poll with them off, so the `_count` of
`hasura_subscription_total_time_seconds` is the number of polls and the `_count` of
`hasura_subscription_db_execution_time_seconds` the number of batches.

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
- `server/src-lib/Hasura/GraphQL/Transport/WebSocket.hs` and
  `server/src-lib/Hasura/GraphQL/Transport/WebSocket/Types.hs`: per-operation liveness
  flag for subscription pushes (see "Live query first-result race" above).
- `server/src-lib/Hasura/GraphQL/Execute/Subscription/State.hs`: decrements the operation
  name count and `hasura_active_subscriptions` once per removed streaming subscriber, and
  counts the gauge down when a removal empties a poller without a thread (see "Operation
  name count of streaming subscriptions" above).
- `server/test-postgres/Test/Hasura/StreamingSubscriptionSuite.hs`: tests that count when
  subscribers of one operation name are removed from two cohorts, and the count and the
  active subscriptions gauge when a subscriber that is in two cohorts is removed, and when
  the last subscriber of a live query poller without a thread is removed. The first test
  replaces the upstream test "adding a new subscriber concurrently should place the
  subscriber in the appropriate cohort", which ran its poll after the suite's teardown had
  dropped the table, so the cohort's key never advanced and its assertions passed
  vacuously.
- `server/src-lib/Hasura/GraphQL/Execute.hs`: builds the streaming subscription plan once
  (see "Streaming subscription plan built once" above).
- `server/src-lib/Hasura/Server/Prometheus.hs`: `recordSubscriptionMetric` observes a poll
  once when granular metrics are off (see "One observation per subscription poll" above).
- `server/src-test/Hasura/Server/PrometheusSpec.hs` and
  `server/test-postgres/Test/Hasura/StreamingSubscriptionSuite.hs`: test the subscription
  poll observations, with and without granular metrics.
- `.github/workflows/hlint.yml` and `.github/workflows/lint.yml` (removed): upstream CI
  workflows that are not applicable to this fork.
