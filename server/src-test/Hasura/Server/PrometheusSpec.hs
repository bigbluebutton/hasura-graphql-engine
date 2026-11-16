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

import Data.ByteString.Builder qualified as BB
import Data.ByteString.Lazy qualified as BL
import Data.HashMap.Strict qualified as HashMap
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Database.PG.Query qualified as PG
import Hasura.GraphQL.ParameterizedQueryHash (mkUnsafeParameterizedQueryHash)
import Hasura.GraphQL.Transport.HTTP.Protocol (OperationName (..))
import Hasura.Prelude
import Hasura.RQL.Types.Common (defaultSource)
import Hasura.Server.Prometheus
import Hasura.Server.Types (GranularPrometheusMetricsState (..))
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
  recordSubscriptionMetricSpec
  pollAndPoolTimeBucketsSpec
  messagesDroppedHelpSpec
  dbExecutionTimeHelpSpec
  postgresPoolObserverSpec

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
  it "are the full list, including 150 ms and 333 ms, for all four histograms" $ do
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
        "hasura_postgres_connection_init_time_seconds"
      ]
      \family -> (family, upperBoundsOf family) `shouldBe` (family, expected)

-- | A subscription poll serving several operation names is observed once per
-- name with granular metrics (each observation carries its name) and once
-- without them.
recordSubscriptionMetricSpec :: Spec
recordSubscriptionMetricSpec = describe "recordSubscriptionMetric" $ do
  let operationNames =
        HashMap.fromList
          [ (Just (OperationName [G.name|first|]), 1),
            (Just (OperationName [G.name|second|]), 2)
          ]
      queryHash = mkUnsafeParameterizedQueryHash "hash"
      labelFor operationName = SubscriptionLabel streamingSubscriptionLabel (Just (DynamicGraphqlOperationLabel (Just queryHash) operationName))
      observationsOnePoll metricsState = do
        histogram <- HistogramVector.new []
        recordSubscriptionMetric (pure metricsState) True operationNames queryHash streamingSubscriptionLabel \label ->
          HistogramVector.observe histogram label 0.5
        countsByLabels <$> HistogramVector.read histogram
  it "observes a poll once without granular metrics" $ do
    observationsOnePoll GranularMetricsOff
      `shouldReturn` expectedCounts [(SubscriptionLabel streamingSubscriptionLabel Nothing, 1)]
  it "observes a poll once per operation name with granular metrics" $ do
    observationsOnePoll GranularMetricsOn
      `shouldReturn` expectedCounts
        [ (labelFor (Just (OperationName [G.name|first|])), 1),
          (labelFor (Just (OperationName [G.name|second|])), 1)
        ]

-- | The observation count of each series of a histogram vector, keyed by the
-- series' label names and values (the label types have no 'Show' instance).
countsByLabels :: (Prometheus.ToLabels l) => Map.Map l HistogramSample -> [([(Text, Text)], Int64)]
countsByLabels = expectedCounts . map (fmap histCount) . Map.toList

expectedCounts :: (Prometheus.ToLabels l) => [(l, Int64)] -> [([(Text, Text)], Int64)]
expectedCounts = sort . map (first (sort . HashMap.toList . Prometheus.toLabels))

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
  HistogramVector.observe (submTotalTime subm) subLabel 0.1
  HistogramVector.observe (submDBExecTotalTime subm) subLabel 0.1
  GaugeVector.inc (submActiveSubscriptions subm) subLabel
  CounterVector.inc (pmWebSocketBytesSent metrics) (DynamicGraphqlOperationLabel Nothing Nothing)
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
      "# TYPE hasura_websocket_connections gauge",
      "# TYPE hasura_websocket_message_queue_time histogram",
      "# TYPE hasura_websocket_message_write_time histogram",
      "# TYPE hasura_websocket_messages_dropped_total counter",
      "# TYPE hasura_websocket_messages_queued_total counter",
      "# TYPE hasura_websocket_messages_received_bytes_total counter",
      "# TYPE hasura_websocket_messages_sent_bytes_total counter"
    ]
