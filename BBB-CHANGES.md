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
  the system `libgmp`, `libpq`, `libkrb5` and `libodbc` dynamically. With `--dev-version`
  it stamps a test build: the version is `HASURA_VERSION` followed by `-dev.g` and the
  first 8 hex digits of HEAD (for example `v2.50.7-bbb2-dev.g0123abcd`), so `/v1/version`
  tells which commit a test binary was built from. The `g` keeps the hash an alphanumeric
  semver identifier: `Hasura.Server.Version` parses the version as semver, which reads 8
  decimal digits as a number and drops a leading zero. `.dirty` is appended when the tree
  has uncommitted changes to tracked files. A later build without `--dev-version` and
  without `HASURA_VERSION` refuses to reuse a `server/CURRENT_VERSION` that holds a test
  build version, since it names another commit. The `version` mode only writes
  `server/CURRENT_VERSION`.
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
  if the secret is not set, or is set to an empty value, every request is rejected with
  401, so enabling the metrics API without configuring a secret exposes nothing.
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
  (`hasura_cache_request_count`, `hasura_otel_*`) are not registered.

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
`hasura_subscription_db_execution_time_seconds` the number of batches (that ran a query,
see the next section).
Since "Per-operation subscription metrics" below, the two histograms are labeled only by
`subscription_kind` and observed once per poll and once per batch that ran a query, whatever
the granularity setting; `recordSubscriptionMetric` is gone.

## Subscription database time without the pool wait

`runPGSubscription` and `runPGStreamingSubscription` in
`Hasura.Backends.Postgres.Instances.Transport` timed the whole call, which includes the
wait for a connection from the Postgres pool, so
`hasura_subscription_db_execution_time_seconds` and the `db_execution_time` of the
poller log grew with the pool wait. The timer now starts and stops inside the
transaction, around the query, and the time is taken however the query ends: with a
result, with a statement error, or with an exception such as the connection being lost
(which `runQueryTx` turns into an error afterwards; the exception itself is rethrown
unchanged). When no connection could be obtained the query never starts: no time is
reported, and the poll does not observe the batch in
`hasura_subscription_db_execution_time_seconds`, so that a batch without a query does
not add a zero to it; the `_count` of that histogram is the number of batches that ran a
query. Such a batch still fails, and the poller log reports 0 for its `db_execution_time`.
The Postgres `runDBSubscription` and `runDBStreamingSubscription` of `BackendTransport`
return `Nothing` for the time in that case; the MS SQL Server `runDBSubscription` returns the
time of the whole call, as before, and never `Nothing`.
The test of the exception case calls the real `withQueryExecutionTime` with a
simulated query that throws: a real connection drop was not tested, because a lost
connection goes through the pg-client retry (`pgRetrying` resets the connection and runs the
statement again), which makes it neither certain nor repeatable that the timed query ends
with the exception.
The help of `hasura_subscription_db_execution_time_seconds` says the pool wait is left
out.

## Subscription poll time buckets

The two subscription poll time histograms,
`hasura_subscription_total_time_seconds` and
`hasura_subscription_db_execution_time_seconds`, used the buckets
`[0.000001, 0.0001, 0.01, 0.1, 0.3, 1, 3, 10, 30, 100]`, which cannot tell a poll near
the 150 ms streaming interval or the 333 ms live query interval from its neighbours.
They now use `[0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.333, 0.5,
0.75, 1, 2.5, 5, 10, 30]` (`pollAndPoolTimeBuckets`), as do the two Postgres pool time
histograms, `hasura_postgres_pool_wait_time_seconds` and
`hasura_postgres_connection_init_time_seconds`.

## Help of the full send queue counter

When a connection's websocket send queue is full, the message is not sent and the
operation it belongs to is cancelled (`cancelOperationOnOvercapacity` in
`Hasura.GraphQL.Transport.WebSocket`). The help of `hasura_websocket_messages_dropped_total`
said the message was dropped; it now says it was rejected and that each rejection cancels
its operation. The metric name is unchanged.

## Postgres pool metrics

The bundled `pg-client` kept a few pool metrics of its own (wait and connection open
times, an error counter) that nothing exported. A pool now takes a `PGPoolObserver`, a
record of functions it calls as it serves and manages connections
(`initPGPoolWithObserver`; `initPGPool` passes one that records nothing), and the engine
builds one per pool from labeled vectors in its Prometheus metrics
(`postgresPoolObserver`). There is no registry of live pools: a pool recreated for the same
source keeps adding to the same series.

- Pools observed: every Postgres source pool (`mkPgSourceResolver`), labeled with the
  source name, and the metadata database pool, labeled `(metadata)`. Both go through the
  same `initPGPoolWithObserver`. The one-off minimal pools (version checks, downgrade) are
  not observed. Source names are not restricted (the metadata API accepts a source named
  `(metadata)`), so a source with that name adds to the same series as the metadata pool.
- Metrics, all with the `source_name` label: `hasura_postgres_pool_wait_time_seconds`,
  `hasura_postgres_pool_waiting_requests`, `hasura_postgres_pool_connections_in_use`,
  `hasura_postgres_pool_connections_open` (up when the pool opens a connection, down when it
  closes one),
  `hasura_postgres_pool_max_connections`, `hasura_postgres_connection_init_time_seconds`,
  `hasura_postgres_connection_errors_total`,
  `hasura_postgres_connections_closed_total{reason}` and
  `hasura_postgres_pool_wait_timeouts_total`. The three counters and the waiting, in use
  and open gauges of a pool start at zero when its observer is created, so a pool that has
  not been used, or has had no errors, closes or timeouts, shows zero rather than a missing
  series.
- `hasura_postgres_pool_max_connections` is the most connections the pool can have open:
  the bundled `resource-pool` applies the configured limit to each stripe, so it is the
  limit times the number of stripes, on creation and after every resize.
- The wait (`hasura_postgres_pool_wait_time_seconds`) is observed only when a request gets
  a connection. It includes opening a new connection when the pool grows to serve the
  request, and a request whose connection is discarded for being past its lifetime waits
  again, so it is observed twice. A request that gives up when the pool's timeout
  (`cpTimeout`, which the pool rounds to whole seconds) expires is not observed in the wait
  and is counted in `hasura_postgres_pool_wait_timeouts_total` instead.
- `reason` is `lifetime` when the connection was older than the configured lifetime when
  it was closed, the same age test `withExpiringPGconn` uses to discard it, and `other`
  otherwise. It is a test of age, not of why the connection was closed: one past its
  lifetime that is closed for being idle, after an error or when the pool is destroyed
  also counts as `lifetime`. The bundled `resource-pool` is not changed, so the cause of a
  close cannot be told apart.

## Per-operation subscription metrics

The per-operation series replace the label of the parameterized query hash, which changes with
the query text and so cannot compare one operation across builds, with the operation's name,
always on.

- **Operation name labels.** Every series that names an operation carries `operation_name`
  whatever the granularity setting, which the community edition fixes to off
  (`runGetPrometheusMetricsGranularity` in `Hasura.App`): `hasura_active_subscriptions`,
  `hasura_graphql_requests_total` and
  `hasura_websocket_messages_sent_bytes_total`. No series carries `parameterized_query_hash`.
  The value comes from `operationNameLabel`: the name itself for the first distinct names
  of up to 128 characters, up to the operation name limit, `(other)` for any other, `(unnamed)`
  for an operation without a name, so a client cannot create series at will. The limit is 500
  by default, configurable with `HASURA_GRAPHQL_METRICS_OPERATION_NAME_LIMIT` (or
  `--metrics-operation-name-limit`), read once at start; a value that is not a positive
  integer refuses the start, as the other positive integer options do. The 128 characters are
  fixed. bbb-graphql-middleware has the same cap, `prometheus_operation_name_limit` in its
  `config.yml`: the two should be changed together, so that the counts by name of the engine
  and of the middleware stay comparable. The option has no
  maximum: the memory held until a restart and the time and size of a scrape grow in
  proportion to the names admitted; with 10,000 names the export took 2.1 to 2.4 s and 34 MB
  in the benchmark below, on a release build, against the default Prometheus scrape timeout
  of 10 s. The effective limit is logged at start with the
  serve options (`metrics_operation_name_limit`). A request that fails does not admit its
  name: it is labeled by its name only if the name was already admitted, and `(other)` if not.
  The residual risk: an authenticated client can still fill the cap with valid names, and the
  symptom is `(other)` growing while new real names no longer get their own series; the cure is
  a restart. The admitted names and their series are never dropped while the server runs, even
  after the operations end.
  `hasura_websocket_messages_sent_bytes_total` carries it for the data messages of an
  operation; every other message (acknowledgements, keepalives, and an operation's error and
  complete) stays in the unlabeled series, which is no longer the total: the total needs
  `sum`.
- **Histograms by kind only.** `hasura_subscription_total_time_seconds` and
  `hasura_subscription_db_execution_time_seconds` are labeled only by `subscription_kind`. The
  first is observed once per poll and the second once per batch that ran a query; upstream
  observed each once
  for every operation name the poller served, so their counts overstated polls and batches
  (see "One observation per subscription poll" above).
- **Per-operation series.** They are counters and gauges, never histograms, labeled by
  `subscription_kind` and `operation_name` (plus `changed` where stated). A poller can serve
  several operation names (same query, different names); each series attributes the poll as
  follows:
  - `hasura_subscription_polls_total` and `hasura_subscription_poll_errors_total` are events:
    each operation the poller serves counts the poll, and each failed batch, once.
    Compared with the database (`pg_stat_statements`), the calls of a subscription's multiplexed
    query are its poll batches (one per poll while the poller's cohorts fit in one batch) plus
    one validation run per subscription start (`testMultiplexedQueryTx`,
    `Hasura.Backends.Postgres.Instances.Execute`), which no metric counts as a poll.
  - `hasura_subscription_cohorts_processed_total` counts, for each operation the poller serves,
    the cohorts the poll processed; `hasura_active_subscription_cohorts_by_operation` holds,
    for each operation, the cohorts of the pollers serving it, as of each poller's last poll.
    Both count the cohorts of the pollers, not the operation's subscribers, so the series of
    two operations sharing a poller count the same cohorts, and the poll does no work per
    subscriber for them. The gauge changes when a poller polls (an operation that left the
    poller drops out at the next poll) and when a poller is removed. A poller is registered
    when it is created and forgotten when it is removed, and a poll that ends after the removal
    (its thread is stopped, not awaited) changes nothing.
  - `hasura_active_subscription_pollers_by_operation` counts each poller once for each operation
    it serves: +1 when the operation's first subscriber enters the poller (or creates it), -1
    when its last one leaves, as the poller's map of operation names changes. Two names that
    both become `(other)` count the poller once each. The change is applied after the
    transaction, and an operation's -1 always follows its +1: a subscription can only be
    removed once `addLiveQuery` or `addStreamSubscriptionQuery`, which applies the +1, has
    returned.

## Formatting

`server/src-lib/Hasura/GraphQL/Transport/WebSocket.hs` and
`server/src-lib/Hasura/GraphQL/Transport/WebSocket/Server.hs` are not ormolu-clean upstream with the
ormolu of `server/VERSIONS.json`, so for these two files the rule is no new ormolu violation
relative to the base commit `391cf8372c` (the lines ormolu would rewrite are the same as in the
base, or fewer), to keep the diff against upstream minimal; every other file passes
`ormolu --mode check`.

## Modified upstream files

Every file below carries a modification notice at the top, as required by Apache License
2.0 section 4(b):

- `server/src-lib/Hasura/Server/Prometheus.hs`: adds `PrometheusMetricsSpec` (the static
  specification of every exported metric) and `makePrometheusMetrics` (creates the same
  mutable references as `makeDummyPrometheusMetrics`, registered to a store), plus a
  `pmSample` field to sample the store.
- `server/src-exec/Main.hs`: creates the Prometheus metric store and calls
  `makePrometheusMetricsWithOperationNameLimit`, with the configured operation name limit,
  instead of `makeDummyPrometheusMetrics`.
- `server/src-lib/Hasura/Server/App.hs`: adds the `GET /v1/metrics` endpoint.
- `server/src-lib/Hasura/Server/Prometheus/EkgExport.hs` (new file): the EKG-to-Prometheus
  text conversion.
- `server/src-lib/Hasura/Server/Init/Arg/Command/Serve.hs`,
  `server/src-lib/Hasura/Server/Init.hs`, `server/src-lib/Hasura/Server/Init/Config.hs`,
  `server/src-lib/Hasura/App/State.hs`: add the `HASURA_GRAPHQL_METRICS_SECRET` option and
  carry it into the app context. The first three also add the
  `HASURA_GRAPHQL_METRICS_OPERATION_NAME_LIMIT` option (see "Operation name labels" above),
  which `server/src-exec/Main.hs` passes to the metrics.
- `server/src-lib/Hasura/Server/Init/Logging.hs`: logs the operation name limit with the
  serve options.
- `server/src-test/Hasura/Server/InitSpec.hs`: adds the new raw serve option defaults, and
  tests that the metrics secret is read hashed and that an empty one counts as not set, and
  the operation name limit's default, its environment variable, that a parsed argument wins
  over it, and that a value that is not a positive integer is refused.
- `server/src-test/Hasura/Server/Init/ArgSpec.hs`: tests that the
  `--metrics-operation-name-limit` argument accepts a positive integer and refuses 0.
- `server/test-postgres/Constants.hs` and `server/lib/test-harness/src/Harness/Constants.hs`:
  add the new serve option defaults to the test fixtures.
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
- `server/src-lib/Hasura/Server/Prometheus.hs`: observes a subscription poll once (see "One
  observation per subscription poll" above), by subscription kind only since "Per-operation
  subscription metrics" above.
- `server/src-test/Hasura/Server/PrometheusSpec.hs` and
  `server/test-postgres/Test/Hasura/StreamingSubscriptionSuite.hs`: test the subscription
  poll observations.
- `server/src-lib/Hasura/Backends/Postgres/Instances/Transport.hs`: times subscription
  queries inside the transaction (see "Subscription database time without the pool wait"
  above), and exports `withQueryExecutionTime` for its test.
- `server/src-lib/Hasura/GraphQL/Transport/Backend.hs`: the subscription methods of
  `BackendTransport` return no time for a query that never started, and
  `server/src-lib/Hasura/Backends/MSSQL/Instances/Transport.hs` follows the new type.
- `server/src-lib/Hasura/GraphQL/Execute/Subscription/Poll/LiveQuery.hs` and
  `server/src-lib/Hasura/GraphQL/Execute/Subscription/Poll/StreamingQuery.hs`: do not
  observe the database time of a batch that ran no query.
- `server/test-postgres/Test/Hasura/StreamingSubscriptionSuite.hs`: also tests the
  database time of a subscription query that waits for a pool connection, of one that
  ends with a statement error or an exception, and of a poll batch that gets no
  connection.
- `server/src-lib/Hasura/Server/Prometheus.hs`: also sets the buckets of the subscription
  poll time and Postgres pool time histograms (see "Subscription poll time buckets" above),
  tested in
  `server/src-test/Hasura/Server/PrometheusSpec.hs`.
- `server/src-lib/Hasura/Server/Prometheus.hs`: also changes the help of
  `hasura_websocket_messages_dropped_total` (see "Help of the full send queue counter"
  above), tested in `server/src-test/Hasura/Server/PrometheusSpec.hs`.
- `server/lib/pg-client/src/Database/PG/Query/Pool.hs`: adds the pool observer (see
  "Postgres pool metrics" above).
- `server/src-lib/Hasura/App.hs`: observes the metadata pool and every Postgres source pool.
- `server/src-lib/Hasura/Server/Prometheus.hs`: also adds the Postgres pool metrics and
  `postgresPoolObserver`, tested in `server/src-test/Hasura/Server/PrometheusSpec.hs`.
- `server/test-postgres/Test/Hasura/PostgresPoolSuite.hs` (new file) and
  `server/test-postgres/Main.hs`: test the pool observer against a real database; `Main`
  passes the metrics to the source resolver.
- `server/graphql-engine.cabal`: also registers the new test module.
- `server/src-test/Hasura/Server/VersionSpec.hs`: tests the version form of a test build
  (see `scripts/build-v2.sh` above).
- `server/src-lib/Hasura/Server/Prometheus.hs`, `server/src-lib/Hasura/GraphQL/Execute/Subscription/State.hs`,
  `server/src-lib/Hasura/GraphQL/Execute/Subscription/Poll/LiveQuery.hs`,
  `server/src-lib/Hasura/GraphQL/Execute/Subscription/Poll/StreamingQuery.hs`,
  `server/src-lib/Hasura/GraphQL/Transport/HTTP.hs`, `server/src-lib/Hasura/GraphQL/Transport/WebSocket.hs` and
  `server/src-lib/Hasura/GraphQL/Transport/WebSocket/Server.hs`: the per-operation subscription metrics (see
  "Per-operation subscription metrics" above), tested in `server/src-test/Hasura/Server/PrometheusSpec.hs` and
  `server/test-postgres/Test/Hasura/StreamingSubscriptionSuite.hs`.
- `.github/workflows/hlint.yml` and `.github/workflows/lint.yml` (removed): upstream CI
  workflows that are not applicable to this fork.
