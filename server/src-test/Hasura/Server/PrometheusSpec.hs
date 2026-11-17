{-# LANGUAGE QuasiQuotes #-}

-- Added by the BigBlueButton project (https://github.com/bigbluebutton/hasura-graphql-engine):
-- tests for the store-backed Prometheus metrics. See BBB-CHANGES.md.

-- | Tests that 'makePrometheusMetrics' registers every metric to the store
-- without a 'System.Metrics.Prometheus.RegistrationError' (duplicate
-- name/label pairs or invalid names) and that, once every vector metric has
-- been observed at least once, the store exports exactly the expected metric
-- families. Vector metrics (dynamic labels) are absent from a fresh store, so
-- the spec drives one observation through each of them: this also catches a
-- vector silently shadowed by a scalar of the same name, which the unchecked
-- dynamic group registration would not reject.
module Hasura.Server.PrometheusSpec (spec) where

import Control.Exception (evaluate)
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Lazy qualified as BL
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Map.Strict qualified as OrdMap
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.UUID qualified as UUID
import Database.PG.Query qualified as PG
import GHC.AssertNF (isNF)
import Hasura.GraphQL.Transport.HTTP.Protocol (OperationName (..))
import Hasura.Prelude
import Hasura.RQL.Types.Common (defaultSource)
import Hasura.Server.Prometheus
import Language.GraphQL.Draft.Syntax qualified as G
import Language.GraphQL.Draft.Syntax.QQ qualified as G
import System.Metrics.Prometheus qualified as Prometheus
import System.Metrics.Prometheus.CounterVector qualified as CounterVector
import System.Metrics.Prometheus.Export qualified as PromExport
import System.Metrics.Prometheus.GaugeVector qualified as GaugeVector
import System.Metrics.Prometheus.Histogram (HistogramSample (..))
import System.Metrics.Prometheus.HistogramVector qualified as HistogramVector
import Test.Hspec

spec :: Spec
spec = do
  makePrometheusMetricsSpec
  operationNameLabelSpec
  operationNameSeriesSpec
  pollAndPoolTimeBucketsSpec
  messagesDroppedHelpSpec
  dbExecutionTimeHelpSpec
  postgresPoolObserverSpec
  splitBySubscribersSpec
  cohortsGaugeSpec
  addCohortResultBytesSpec

-- | The per-operation cohorts gauge follows the pollers from their registration to their removal.
cohortsGaugeSpec :: Spec
cohortsGaugeSpec = describe "the active cohorts by operation" $ do
  it "counts a registered poller's cohorts, takes them out on removal and ignores a later poll" $ do
    metrics <- makeDummyPrometheusMetrics
    let pollerId = fromMaybe (error "uuid") (UUID.fromString "0b6f1c1e-2a8f-4a0e-9a41-6c9b1d6a7f01")
        report = recordPollByOperation metrics streamingSubscriptionLabel (HashMap.singleton (OperationNameLabel "getUsers") 1) (PollByOperation pollerId 0 3 0 0 0 0)
        gauge = OrdMap.elems <$> GaugeVector.read (submActiveCohortsByOperation (pmSubscriptionMetrics metrics))
    report
    unregistered <- gauge
    registerPollerByOperation metrics pollerId
    report
    registered <- gauge
    forgetPollerByOperation metrics pollerId
    removed <- gauge
    report
    late <- gauge
    (unregistered, registered, removed, late) `shouldBe` ([], [3], [0], [0])

  it "leaves the gauge as it is on a repeated report, and moves it on a changed one" $ do
    metrics <- makeDummyPrometheusMetrics
    let pollerId = fromMaybe (error "uuid") (UUID.fromString "0b6f1c1e-2a8f-4a0e-9a41-6c9b1d6a7f02")
        report cohorts = recordPollByOperation metrics streamingSubscriptionLabel (HashMap.singleton (OperationNameLabel "getUsers") 1) (PollByOperation pollerId 0 cohorts 0 0 0 0)
        gauge = OrdMap.elems <$> GaugeVector.read (submActiveCohortsByOperation (pmSubscriptionMetrics metrics))
    registerPollerByOperation metrics pollerId
    report 3
    report 3
    repeated <- gauge
    report 5
    changed <- gauge
    (repeated, changed) `shouldBe` ([3], [5])

-- | The bytes of a poll's cohorts are kept evaluated, not as a chain of additions.
addCohortResultBytesSpec :: Spec
addCohortResultBytesSpec = describe "addCohortResultBytes" $ do
  it "adds a cohort's bytes to the changed or unchanged count, both evaluated" $ do
    resultBytes <- newIORef (0, 0)
    addCohortResultBytes resultBytes True 100
    addCohortResultBytes resultBytes False 30
    addCohortResultBytes resultBytes True 5
    -- The reference holds a selector thunk of atomicModifyIORef'; evaluating it gives the pair,
    -- whose fields are checked before anything else looks at them (which would evaluate them).
    counts <- evaluate =<< readIORef resultBytes
    isNF counts `shouldReturn` True
    counts `shouldBe` (105, 30)

-- | Amounts of a poll split among the operations of its poller.
splitBySubscribersSpec :: Spec
splitBySubscribersSpec = describe "splitBySubscribers" $ do
  let label = OperationNameLabel
      split amount operations = sort (splitBySubscribers amount (HashMap.fromList [(label name, n) | (name, n) <- operations]))
  it "splits in proportion to the subscribers, with the remainder to the operation with the most" $ do
    split 10 [("A", 2), ("B", 1)] `shouldBe` [(label "A", 7), (label "B", 3)]
  it "gives the remainder to the greatest label among the operations with the most subscribers" $ do
    split 3 [("A", 1), ("B", 1)] `shouldBe` [(label "A", 1), (label "B", 2)]
  it "keeps the sum of the parts equal to the amount" $ do
    sum (map snd (split 1000003 [("A", 7), ("B", 13), ("C", 1)])) `shouldBe` 1000003
  it "splits nothing among no operations" $ do
    split 10 [] `shouldBe` []

-- | The observer of a pool records under the pool's label: "(metadata)" or the source name.
postgresPoolObserverSpec :: Spec
postgresPoolObserverSpec = describe "postgresPoolObserver" $ do
  it "starts the pool's counters and its waiting, in use and open gauges at zero" $ do
    store <- Prometheus.newStore @PrometheusMetricsSpec
    metrics <- makePrometheusMetrics store
    _ <- postgresPoolObserver metrics (PostgresPoolLabel "default")
    rendered <- renderSample metrics
    sort [line | line <- T.lines rendered, "hasura_postgres_" `T.isPrefixOf` line]
      `shouldBe` sort
        [ "hasura_postgres_connection_errors_total{source_name=\"default\"} 0.0",
          "hasura_postgres_connections_closed_total{source_name=\"default\",reason=\"lifetime\"} 0.0",
          "hasura_postgres_connections_closed_total{source_name=\"default\",reason=\"other\"} 0.0",
          "hasura_postgres_pool_connections_in_use{source_name=\"default\"} 0.0",
          "hasura_postgres_pool_connections_open{source_name=\"default\"} 0.0",
          "hasura_postgres_pool_wait_timeouts_total{source_name=\"default\"} 0.0",
          "hasura_postgres_pool_waiting_requests{source_name=\"default\"} 0.0"
        ]

  it "records each pool event under the pool's source_name" $ do
    store <- Prometheus.newStore @PrometheusMetricsSpec
    metrics <- makePrometheusMetrics store
    observer <- postgresPoolObserver metrics metadataPoolLabel
    PG.ppoMaxConnections observer 50
    PG.ppoRequestWaiting observer
    PG.ppoRequestServed observer
    PG.ppoWaitTimeout observer
    PG.ppoConnectionTaken observer
    PG.ppoWaitTime observer 0.2
    PG.ppoConnectionInitTime observer 0.01
    PG.ppoConnectionInitTime observer 0.01
    PG.ppoConnectionInitTime observer 0.01
    PG.ppoConnectionError observer
    PG.ppoConnectionClosed observer PG.PGConnectionClosedLifetime
    PG.ppoConnectionClosed observer PG.PGConnectionClosedOther
    PG.ppoConnectionClosed observer PG.PGConnectionClosedOther
    rendered <- renderSample metrics
    let poolLines =
          sort
            [ line
            | line <- T.lines rendered,
              "hasura_postgres_" `T.isPrefixOf` line,
              not ("_bucket{" `T.isInfixOf` line)
            ]
    poolLines
      `shouldBe` sort
        [ "hasura_postgres_connection_errors_total{source_name=\"(metadata)\"} 1.0",
          "hasura_postgres_connection_init_time_seconds_count{source_name=\"(metadata)\"} 3",
          "hasura_postgres_connection_init_time_seconds_sum{source_name=\"(metadata)\"} 3.0e-2",
          "hasura_postgres_pool_connections_open{source_name=\"(metadata)\"} 0.0",
          "hasura_postgres_connections_closed_total{source_name=\"(metadata)\",reason=\"lifetime\"} 1.0",
          "hasura_postgres_connections_closed_total{source_name=\"(metadata)\",reason=\"other\"} 2.0",
          "hasura_postgres_pool_connections_in_use{source_name=\"(metadata)\"} 1.0",
          "hasura_postgres_pool_max_connections{source_name=\"(metadata)\"} 50.0",
          "hasura_postgres_pool_wait_time_seconds_count{source_name=\"(metadata)\"} 1",
          "hasura_postgres_pool_wait_time_seconds_sum{source_name=\"(metadata)\"} 0.2",
          "hasura_postgres_pool_waiting_requests{source_name=\"(metadata)\"} 0.0",
          "hasura_postgres_pool_wait_timeouts_total{source_name=\"(metadata)\"} 1.0"
        ]

  it "counts a waiting request and a connection in use up while they last, and down after" $ do
    store <- Prometheus.newStore @PrometheusMetricsSpec
    metrics <- makePrometheusMetrics store
    observer <- postgresPoolObserver metrics metadataPoolLabel
    let gauges = do
          rendered <- renderSample metrics
          pure
            $ sort
              [ line
              | line <- T.lines rendered,
                any (`T.isPrefixOf` line) ["hasura_postgres_pool_waiting_requests", "hasura_postgres_pool_connections_in_use"]
              ]
    PG.ppoRequestWaiting observer
    PG.ppoConnectionTaken observer
    during <- gauges
    PG.ppoRequestServed observer
    PG.ppoConnectionReturned observer
    after <- gauges
    during
      `shouldBe` [ "hasura_postgres_pool_connections_in_use{source_name=\"(metadata)\"} 1.0",
                   "hasura_postgres_pool_waiting_requests{source_name=\"(metadata)\"} 1.0"
                 ]
    after
      `shouldBe` [ "hasura_postgres_pool_connections_in_use{source_name=\"(metadata)\"} 0.0",
                   "hasura_postgres_pool_waiting_requests{source_name=\"(metadata)\"} 0.0"
                 ]

dbExecutionTimeHelpSpec :: Spec
dbExecutionTimeHelpSpec = describe "hasura_subscription_db_execution_time_seconds" $ do
  it "says in its help that the pool wait is left out" $ do
    store <- Prometheus.newStore @PrometheusMetricsSpec
    metrics <- makePrometheusMetrics store
    observeEveryVectorOnce metrics
    rendered <- renderSample metrics
    filter ("# HELP hasura_subscription_db_execution_time_seconds " `T.isPrefixOf`) (T.lines rendered)
      `shouldBe` ["# HELP hasura_subscription_db_execution_time_seconds Time spent executing the subscription query in the database, excluding the wait for a pool connection, in seconds"]

messagesDroppedHelpSpec :: Spec
messagesDroppedHelpSpec = describe "hasura_websocket_messages_dropped_total" $ do
  it "says in its help that a full send queue cancels the operation" $ do
    store <- Prometheus.newStore @PrometheusMetricsSpec
    metrics <- makePrometheusMetrics store
    rendered <- renderSample metrics
    filter ("# HELP hasura_websocket_messages_dropped_total " `T.isPrefixOf`) (T.lines rendered)
      `shouldBe` ["# HELP hasura_websocket_messages_dropped_total Total number of messages rejected because the websocket send queue was full; each rejection cancels its operation"]

-- | The upper bounds the store exports for the two subscription poll time histograms and
-- the two Postgres pool time histograms.
pollAndPoolTimeBucketsSpec :: Spec
pollAndPoolTimeBucketsSpec = describe "subscription poll and Postgres pool time buckets" $ do
  it "are the full list, including 150 ms and 333 ms, for every such histogram" $ do
    store <- Prometheus.newStore @PrometheusMetricsSpec
    metrics <- makePrometheusMetrics store
    observeEveryVectorOnce metrics
    rendered <- renderSample metrics
    let upperBoundsOf family =
          [ T.takeWhile (/= '"') afterLe
          | line <- T.lines rendered,
            (family <> "_bucket{") `T.isPrefixOf` line,
            let (_, rest) = T.breakOn "le=\"" line,
            let afterLe = T.drop 4 rest
          ]
        expected = ["1.0e-3", "5.0e-3", "1.0e-2", "2.5e-2", "5.0e-2", "7.5e-2", "0.1", "0.15", "0.2", "0.25", "0.333", "0.5", "0.75", "1.0", "2.5", "5.0", "10.0", "30.0", "+Inf"]
    for_
      [ "hasura_subscription_total_time_seconds",
        "hasura_subscription_db_execution_time_seconds",
        "hasura_postgres_pool_wait_time_seconds",
        "hasura_postgres_connection_init_time_seconds",
        "hasura_subscription_poll_interval_seconds"
      ]
      \family -> (family, upperBoundsOf family) `shouldBe` (family, expected)

-- | Every operation name label passes through 'operationNameLabel': the name for the first
-- 'defaultOperationNameLimit' distinct names of up to 'maxOperationNameLabelLength' characters,
-- "(other)" past them, "(unnamed)" without a name.
operationNameLabelSpec :: Spec
operationNameLabelSpec = describe "operationNameLabel" $ do
  let opName = Just . OperationName . G.unsafeMkName
  it "caps the distinct names and their length" $ do
    defaultOperationNameLimit `shouldBe` 500
    metrics <- makeDummyPrometheusMetrics
    for_ [1 .. defaultOperationNameLimit] \i -> operationNameLabel metrics (opName ("op" <> tshow i))
    operationNameLabel metrics (opName ("op" <> tshow (defaultOperationNameLimit + 1))) `shouldReturn` otherOperationLabel
    operationNameLabel metrics (opName "op1") `shouldReturn` OperationNameLabel "op1"
    fresh <- makeDummyPrometheusMetrics
    let longest = T.replicate maxOperationNameLabelLength "a"
    operationNameLabel fresh (opName longest) `shouldReturn` OperationNameLabel longest
    operationNameLabel fresh (opName (longest <> "a")) `shouldReturn` otherOperationLabel
    operationNameLabel metrics Nothing `shouldReturn` unnamedOperationLabel

  it "admits as many distinct names as the configured limit, lower or higher than the default" $ do
    let admitted limit count = do
          store <- Prometheus.newStore @PrometheusMetricsSpec
          metrics <- makePrometheusMetricsWithOperationNameLimit limit store
          labels <- for [1 .. count] \i -> operationNameLabel metrics (opName ("op" <> tshow i))
          pure $ length $ filter (/= otherOperationLabel) labels
    admitted 5 10 `shouldReturn` 5
    admitted 10000 10001 `shouldReturn` 10000

  it "counts two names past the cap in one poller as one (other), with their subscribers added" $ do
    metrics <- makeDummyPrometheusMetrics
    for_ [1 .. defaultOperationNameLimit] \i -> operationNameLabel metrics (opName ("op" <> tshow i))
    labels <- pollOperationLabels metrics (HashMap.fromList [(opName "lateA", 2), (opName "lateB", 3), (opName "op1", 1)])
    labels `shouldBe` HashMap.fromList [(otherOperationLabel, 5), (OperationNameLabel "op1", 1)]
  it "admits no name for a request that failed, and labels it by name only if already admitted" $ do
    metrics <- makeDummyPrometheusMetrics
    let request status name = do
          label <- newIORef Nothing
          recordGraphqlOperationMetric metrics (Just G.OperationTypeQuery) status (opName name) (writeIORef label . fmap (\(DynamicGraphqlOperationLabel (OperationNameLabel n)) -> n) . dynamic_label)
          readIORef label
        other = Just (unOperationNameLabel otherOperationLabel)
    failedFirst <- request Failed "neverSucceeded"
    succeeded <- request Success "getUsers"
    failedAfter <- request Failed "getUsers"
    -- The name of the failed request was not admitted: after it, it is still only looked up.
    stillOther <- request Failed "neverSucceeded"
    (failedFirst, succeeded, failedAfter, stillOther)
      `shouldBe` (other, Just "getUsers", Just "getUsers", other)

-- | The subscription and request series carry operation_name whatever the granularity setting,
-- never parameterized_query_hash, and the poll histograms carry only subscription_kind.
operationNameSeriesSpec :: Spec
operationNameSeriesSpec = describe "operation_name series" $ do
  it "label every subscription and request series with the operation name, without a hash" $ do
    store <- Prometheus.newStore @PrometheusMetricsSpec
    metrics <- makePrometheusMetrics store
    let subm = pmSubscriptionMetrics metrics
        name = Just (OperationName [G.name|getUsers|])
    subscriptionLabel metrics streamingSubscriptionLabel name >>= GaugeVector.inc (submActiveSubscriptions subm)
    recordGraphqlOperationMetric metrics (Just G.OperationTypeQuery) Success name (CounterVector.inc (gqlRequests (pmGraphQLRequestMetrics metrics)))
    HistogramVector.observe (submTotalTime subm) streamingSubscriptionLabel 0.1
    rendered <- renderSample metrics
    let seriesOf family = [line | line <- T.lines rendered, (family <> "{") `T.isPrefixOf` line]
    seriesOf "hasura_active_subscriptions" `shouldBe` ["hasura_active_subscriptions{operation_name=\"getUsers\",subscription_kind=\"streaming\"} 1.0"]
    map (T.isInfixOf "operation_name=\"getUsers\"") (seriesOf "hasura_graphql_requests_total") `shouldBe` [True]
    seriesOf "hasura_subscription_total_time_seconds_count" `shouldBe` ["hasura_subscription_total_time_seconds_count{subscription_kind=\"streaming\"} 1"]
    filter (T.isInfixOf "parameterized_query_hash") (T.lines rendered) `shouldBe` []
  it "observe the time between two poll starts, and nothing at a poller's first poll" $ do
    metrics <- makeDummyPrometheusMetrics
    previousStart <- newIORef Nothing
    let intervals = OrdMap.lookup streamingSubscriptionLabel <$> HistogramVector.read (submPollInterval (pmSubscriptionMetrics metrics))
    observePollStart metrics streamingSubscriptionLabel previousStart
    afterFirst <- intervals
    observePollStart metrics streamingSubscriptionLabel previousStart
    afterSecond <- intervals
    fmap histCount afterFirst `shouldBe` Nothing
    fmap histCount afterSecond `shouldBe` Just 1
    fmap histSum afterSecond `shouldSatisfy` maybe False (>= 0)
  it "label the websocket bytes sent by operation, and leave messages of no operation unlabeled" $ do
    store <- Prometheus.newStore @PrometheusMetricsSpec
    metrics <- makePrometheusMetrics store
    label <- operationNameLabel metrics (Just (OperationName [G.name|getUsers|]))
    CounterVector.add (pmWebSocketBytesSent metrics) (Just (DynamicGraphqlOperationLabel label)) 120
    CounterVector.add (pmWebSocketBytesSent metrics) Nothing 30
    rendered <- renderSample metrics
    sort [line | line <- T.lines rendered, "hasura_websocket_messages_sent_bytes_total" `T.isPrefixOf` line, not ("#" `T.isPrefixOf` line)]
      `shouldBe` ["hasura_websocket_messages_sent_bytes_total 30.0", "hasura_websocket_messages_sent_bytes_total{operation_name=\"getUsers\"} 120.0"]
  it "export the per-operation times, kept in microseconds, in seconds" $ do
    store <- Prometheus.newStore @PrometheusMetricsSpec
    metrics <- makePrometheusMetrics store
    let subm = pmSubscriptionMetrics metrics
    label <- subscriptionLabel metrics streamingSubscriptionLabel (Just (OperationName [G.name|getUsers|]))
    CounterVector.add (submDBExecutionMicroseconds subm) label 1500000
    CounterVector.add (submTotalTimeMicroseconds subm) label 250
    rendered <- renderSample metrics
    let seriesOf family = [line | line <- T.lines rendered, (family <> "{") `T.isPrefixOf` line]
    seriesOf "hasura_subscription_db_execution_seconds_total" `shouldBe` ["hasura_subscription_db_execution_seconds_total{operation_name=\"getUsers\",subscription_kind=\"streaming\"} 1.5"]
    seriesOf "hasura_subscription_total_time_seconds_total" `shouldBe` ["hasura_subscription_total_time_seconds_total{operation_name=\"getUsers\",subscription_kind=\"streaming\"} 2.5e-4"]

makePrometheusMetricsSpec :: Spec
makePrometheusMetricsSpec = describe "makePrometheusMetrics" $ do
  it "registers every metric and exports the expected # TYPE lines" $ do
    store <- Prometheus.newStore @PrometheusMetricsSpec
    -- Throws a RegistrationError if the metric specification contains a
    -- duplicate name/label pair or an invalid name, label or help text.
    metrics <- makePrometheusMetrics store
    observeEveryVectorOnce metrics
    sample <- pmSample metrics
    let rendered =
          TE.decodeUtf8
            $ BL.toStrict
            $ BB.toLazyByteString
            $ PromExport.sampleToPrometheus sample
        typeLines = sort $ filter ("# TYPE " `T.isPrefixOf`) $ T.lines rendered
    typeLines `shouldBe` expectedTypeLines

-- | The store's sample in the Prometheus text format.
renderSample :: PrometheusMetrics -> IO Text
renderSample metrics = do
  sample <- pmSample metrics
  pure $ TE.decodeUtf8 $ BL.toStrict $ BB.toLazyByteString $ PromExport.sampleToPrometheus sample

-- | Drive one observation through every vector metric so that each of them
-- shows up in the sample (empty vector groups are discarded by the exporter).
observeEveryVectorOnce :: PrometheusMetrics -> IO ()
observeEveryVectorOnce metrics = do
  let gql = pmGraphQLRequestMetrics metrics
      etm = pmEventTriggerMetrics metrics
      subm = pmSubscriptionMetrics metrics
      subLabel = SubscriptionLabel streamingSubscriptionLabel Nothing
      eventStatus = EventStatusWithTriggerLabel eventSuccessLabel Nothing
  CounterVector.inc (gqlRequests gql) (GraphQLRequestsLabels (Just G.OperationTypeQuery) Success Nothing)
  HistogramVector.observe (eventsFetchQueryTime etm) (EventsFetchQueryStatusLabel defaultSource Success) 0.1
  CounterVector.inc (eventsFetchedTotal etm) (EventTriggerSourceLabel defaultSource)
  HistogramVector.observe (eventQueueTimeSeconds etm) Nothing 0.1
  HistogramVector.observe (eventWebhookProcessingTime etm) Nothing 0.1
  HistogramVector.observe (eventProcessingTime etm) Nothing 0.1
  CounterVector.inc (eventProcessedTotal etm) eventStatus
  CounterVector.inc (eventInvocationTotal etm) eventStatus
  HistogramVector.observe (submTotalTime subm) streamingSubscriptionLabel 0.1
  HistogramVector.observe (submDBExecTotalTime subm) streamingSubscriptionLabel 0.1
  GaugeVector.inc (submActiveSubscriptions subm) subLabel
  CounterVector.inc (submPollsByOperation subm) subLabel
  CounterVector.inc (submPollErrorsByOperation subm) subLabel
  CounterVector.inc (submCohortsProcessedByOperation subm) subLabel
  GaugeVector.inc (submActiveCohortsByOperation subm) subLabel
  GaugeVector.inc (submActivePollersByOperation subm) subLabel
  CounterVector.inc (submSubscribersServed subm) (SubscriptionChangedLabel subLabel True)
  CounterVector.inc (submSentBytes subm) (SubscriptionChangedLabel subLabel True)
  CounterVector.inc (submResultBytes subm) (SubscriptionChangedLabel subLabel True)
  CounterVector.inc (submDBExecutionMicroseconds subm) subLabel
  CounterVector.inc (submTotalTimeMicroseconds subm) subLabel
  HistogramVector.observe (submPollInterval subm) streamingSubscriptionLabel 0.2
  CounterVector.inc (pmWebSocketBytesSent metrics) Nothing
  let pool = pmPostgresPoolMetrics metrics
  HistogramVector.observe (ppmWaitTime pool) metadataPoolLabel 0.1
  GaugeVector.inc (ppmWaitingRequests pool) metadataPoolLabel
  GaugeVector.inc (ppmConnectionsInUse pool) metadataPoolLabel
  GaugeVector.inc (ppmConnectionsOpen pool) metadataPoolLabel
  GaugeVector.inc (ppmMaxConnections pool) metadataPoolLabel
  HistogramVector.observe (ppmConnectionInitTime pool) metadataPoolLabel 0.1
  CounterVector.inc (ppmConnectionErrors pool) metadataPoolLabel
  CounterVector.inc (ppmConnectionsClosed pool) (PostgresPoolClosedLabel metadataPoolLabel "other")
  CounterVector.inc (ppmWaitTimeouts pool) metadataPoolLabel

-- | Every metric family 'makePrometheusMetrics' registers: scalar and
-- statically labeled metrics appear in a fresh store, vector metrics after
-- their first observation (driven by 'observeEveryVectorOnce').
expectedTypeLines :: [Text]
expectedTypeLines =
  sort
    [ "# TYPE hasura_action_bytes_received_total counter",
      "# TYPE hasura_action_bytes_sent_total counter",
      "# TYPE hasura_active_subscription_pollers gauge",
      "# TYPE hasura_active_subscription_pollers_in_error_state gauge",
      "# TYPE hasura_active_subscriptions gauge",
      "# TYPE hasura_cron_events_invocation_total counter",
      "# TYPE hasura_cron_events_processed_total counter",
      "# TYPE hasura_event_fetch_time_per_batch_seconds histogram",
      "# TYPE hasura_event_invocations_total counter",
      "# TYPE hasura_event_processed_total counter",
      "# TYPE hasura_event_processing_time_seconds histogram",
      "# TYPE hasura_event_queue_time_seconds histogram",
      "# TYPE hasura_event_trigger_bytes_received_total counter",
      "# TYPE hasura_event_trigger_bytes_sent_total counter",
      "# TYPE hasura_event_trigger_http_workers gauge",
      "# TYPE hasura_event_webhook_processing_time_seconds histogram",
      "# TYPE hasura_events_fetch_query_time_seconds histogram",
      "# TYPE hasura_events_fetched_per_batch gauge",
      "# TYPE hasura_events_fetched_total counter",
      "# TYPE hasura_graphql_execution_time_seconds histogram",
      "# TYPE hasura_graphql_requests_total counter",
      "# TYPE hasura_http_connections gauge",
      "# TYPE hasura_oneoff_events_invocation_total counter",
      "# TYPE hasura_postgres_connection_errors_total counter",
      "# TYPE hasura_postgres_connection_init_time_seconds histogram",
      "# TYPE hasura_postgres_connections_closed_total counter",
      "# TYPE hasura_postgres_pool_connections_in_use gauge",
      "# TYPE hasura_postgres_pool_connections_open gauge",
      "# TYPE hasura_postgres_pool_max_connections gauge",
      "# TYPE hasura_postgres_pool_wait_time_seconds histogram",
      "# TYPE hasura_postgres_pool_wait_timeouts_total counter",
      "# TYPE hasura_postgres_pool_waiting_requests gauge",
      "# TYPE hasura_oneoff_events_processed_total counter",
      "# TYPE hasura_scheduled_trigger_bytes_received_total counter",
      "# TYPE hasura_scheduled_trigger_bytes_sent_total counter",
      "# TYPE hasura_subscription_db_execution_time_seconds histogram",
      "# TYPE hasura_subscription_total_time_seconds histogram",
      "# TYPE hasura_subscription_polls_total counter",
      "# TYPE hasura_subscription_poll_errors_total counter",
      "# TYPE hasura_subscription_cohorts_processed_total counter",
      "# TYPE hasura_active_subscription_cohorts_by_operation gauge",
      "# TYPE hasura_active_subscription_pollers_by_operation gauge",
      "# TYPE hasura_subscription_subscribers_served_total counter",
      "# TYPE hasura_subscription_sent_bytes_total counter",
      "# TYPE hasura_subscription_result_bytes_total counter",
      "# TYPE hasura_subscription_db_execution_seconds_total counter",
      "# TYPE hasura_subscription_total_time_seconds_total counter",
      "# TYPE hasura_subscription_poll_interval_seconds histogram",
      "# TYPE hasura_websocket_connections gauge",
      "# TYPE hasura_websocket_message_queue_time histogram",
      "# TYPE hasura_websocket_message_write_time histogram",
      "# TYPE hasura_websocket_messages_dropped_total counter",
      "# TYPE hasura_websocket_messages_queued_total counter",
      "# TYPE hasura_websocket_messages_received_bytes_total counter",
      "# TYPE hasura_websocket_messages_sent_bytes_total counter"
    ]
