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
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Hasura.Prelude
import Hasura.RQL.Types.Common (defaultSource)
import Hasura.Server.Prometheus
import Language.GraphQL.Draft.Syntax qualified as G
import System.Metrics.Prometheus qualified as Prometheus
import System.Metrics.Prometheus.CounterVector qualified as CounterVector
import System.Metrics.Prometheus.Export qualified as PromExport
import System.Metrics.Prometheus.GaugeVector qualified as GaugeVector
import System.Metrics.Prometheus.HistogramVector qualified as HistogramVector
import Test.Hspec

spec :: Spec
spec = describe "makePrometheusMetrics" $ do
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
