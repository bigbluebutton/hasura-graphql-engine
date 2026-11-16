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
  dbExecutionTimeHelpSpec

dbExecutionTimeHelpSpec :: Spec
dbExecutionTimeHelpSpec = describe "hasura_subscription_db_execution_time_seconds" $ do
  it "says in its help that the pool wait is left out" $ do
    store <- Prometheus.newStore @PrometheusMetricsSpec
    metrics <- makePrometheusMetrics store
    observeEveryVectorOnce metrics
    rendered <- renderSample metrics
    filter ("# HELP hasura_subscription_db_execution_time_seconds " `T.isPrefixOf`) (T.lines rendered)
      `shouldBe` ["# HELP hasura_subscription_db_execution_time_seconds Time spent executing the subscription query in the database, excluding the wait for a pool connection, in seconds"]

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
