{-# LANGUAGE DeriveAnyClass #-}

-- Modified by the BigBlueButton project (https://github.com/bigbluebutton/hasura-graphql-engine):
-- adds a metric specification and a store-backed constructor for the Prometheus
-- metrics, used by the GET /v1/metrics endpoint. See BBB-CHANGES.md.

-- | Mutable references for Prometheus metrics.
--
-- These metrics are independent from the metrics in "Hasura.Server.Metrics".
module Hasura.Server.Prometheus
  ( PrometheusMetrics (..),
    GraphQLRequestMetrics (..),
    EventTriggerMetrics (..),
    CacheRequestMetrics (..),
    OpenTelemetryMetrics (..),
    PrometheusMetricsSpec,
    makePrometheusMetrics,
    makeDummyPrometheusMetrics,
    ConnectionsGauge,
    Connections (..),
    newConnectionsGauge,
    readConnectionsGauge,
    incWarpThreads,
    decWarpThreads,
    incWebsocketConnections,
    decWebsocketConnections,
    ScheduledTriggerMetrics (..),
    SubscriptionMetrics (..),
    EventTriggerSourceLabel (..),
    EventsFetchQueryStatusLabel (..),
    DynamicEventTriggerLabel (..),
    ResponseStatus (..),
    responseStatusToLabelValue,
    EventStatusLabel (..),
    eventSuccessLabel,
    eventFailedLabel,
    EventStatusWithTriggerLabel (..),
    GranularPrometheusMetricsState (..),
    observeHistogramWithLabel,
    SubscriptionKindLabel (..),
    SubscriptionLabel (..),
    DynamicGraphqlOperationLabel (..),
    streamingSubscriptionLabel,
    liveQuerySubscriptionLabel,
    recordMetricWithLabel,
    recordSubscriptionMetric,
    GraphQLRequestsLabels (..),
    recordGraphqlOperationMetric,
  )
where

import Data.HashMap.Internal.Strict qualified as Map
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.Kind (Type)
import Data.Map.Strict qualified as OrdMap
import GHC.TypeLits (Symbol)
import Hasura.GraphQL.ParameterizedQueryHash
import Hasura.GraphQL.Transport.HTTP.Protocol (OperationName (..))
import Hasura.Prelude
import Hasura.RQL.Types.Common (SourceName, sourceNameToText)
import Hasura.RQL.Types.EventTrigger (TriggerName, triggerNameToTxt)
import Hasura.Server.Types (GranularPrometheusMetricsState (..))
import Language.GraphQL.Draft.Syntax qualified as G
import System.Metrics.Prometheus
  ( MetricType (..),
    Sample,
    SamplingGroup (..),
    Store,
    ToLabels (..),
    createCounter,
    createCounterVector,
    createGauge,
    createGaugeVector,
    createHistogram,
    createHistogramVector,
    registerGroup,
    registerPermanently,
    sampleAll,
  )
import System.Metrics.Prometheus.Counter (Counter)
import System.Metrics.Prometheus.Counter qualified as Counter
import System.Metrics.Prometheus.CounterVector (CounterVector)
import System.Metrics.Prometheus.CounterVector qualified as CounterVector
import System.Metrics.Prometheus.Gauge (Gauge)
import System.Metrics.Prometheus.Gauge qualified as Gauge
import System.Metrics.Prometheus.GaugeVector qualified as GaugeVector
import System.Metrics.Prometheus.Histogram (Histogram)
import System.Metrics.Prometheus.Histogram qualified as Histogram
import System.Metrics.Prometheus.HistogramVector (HistogramVector)
import System.Metrics.Prometheus.HistogramVector qualified as HistogramVector

--------------------------------------------------------------------------------

-- | Mutable references for Prometheus metrics.
data PrometheusMetrics = PrometheusMetrics
  { pmConnections :: ConnectionsGauge,
    pmGraphQLRequestMetrics :: GraphQLRequestMetrics,
    pmEventTriggerMetrics :: EventTriggerMetrics,
    pmWebSocketBytesReceived :: Counter,
    pmWebSocketBytesSent :: CounterVector DynamicGraphqlOperationLabel,
    pmActionBytesReceived :: Counter,
    pmActionBytesSent :: Counter,
    pmScheduledTriggerMetrics :: ScheduledTriggerMetrics,
    pmSubscriptionMetrics :: SubscriptionMetrics,
    pmWebsocketMsgQueueTimeSeconds :: Histogram,
    pmWebsocketMsgWriteTimeSeconds :: Histogram,
    -- | Total data and control messages successfully enqueued for sending
    -- over a websocket connection. Excludes keepalive traffic
    -- ('SMConnKeepAlive'/'SMPing'), which is coalesced into a single-slot
    -- mailbox rather than queued, so counting it here wouldn't mean
    -- "messages queued" in the same sense as everything else.
    pmWebsocketMsgQueued :: Counter,
    -- | Data messages that could not be enqueued because the connection's
    -- send queue was at capacity. Never includes control/signal messages
    -- (completions, errors, acks, pongs), which always succeed -- see
    -- 'Hasura.GraphQL.Transport.WebSocket.Server.sendMsgUnconditional'.
    -- Unlike the old evicted-message metric this replaces, nothing is ever
    -- silently dropped: the caller is expected to notify the client (and,
    -- for subscriptions, tear down the operation) whenever this increments.
    pmWebsocketMsgDropped :: Counter,
    pmCacheRequestMetrics :: CacheRequestMetrics,
    pmOpenTelemetryMetrics :: OpenTelemetryMetrics,
    -- | Sample every metric registered to the Prometheus metrics store.
    -- 'makeDummyPrometheusMetrics' returns an empty sample; the sample of
    -- 'makePrometheusMetrics' covers all the metrics above that are backed
    -- by its store.
    pmSample :: IO Sample
  }

data GraphQLRequestMetrics = GraphQLRequestMetrics
  { gqlRequests :: CounterVector GraphQLRequestsLabels,
    gqlExecutionTimeSecondsQuery :: Histogram,
    gqlExecutionTimeSecondsMutation :: Histogram
  }

data EventTriggerMetrics = EventTriggerMetrics
  { eventTriggerHTTPWorkers :: Gauge,
    eventsFetchQueryTime :: HistogramVector EventsFetchQueryStatusLabel,
    eventsFetchedTotal :: CounterVector EventTriggerSourceLabel,
    eventsFetchedPerBatch :: Gauge,
    eventQueueTimeSeconds :: HistogramVector (Maybe DynamicEventTriggerLabel),
    eventsFetchTimePerBatch :: Histogram,
    eventWebhookProcessingTime :: HistogramVector (Maybe DynamicEventTriggerLabel),
    eventProcessingTime :: HistogramVector (Maybe DynamicEventTriggerLabel),
    eventTriggerBytesReceived :: Counter,
    eventTriggerBytesSent :: Counter,
    eventProcessedTotal :: CounterVector EventStatusWithTriggerLabel,
    eventInvocationTotal :: CounterVector EventStatusWithTriggerLabel
  }

data ScheduledTriggerMetrics = ScheduledTriggerMetrics
  { stmScheduledTriggerBytesReceived :: Counter,
    stmScheduledTriggerBytesSent :: Counter,
    stmCronEventsInvocationTotalSuccess :: Counter,
    stmCronEventsInvocationTotalFailure :: Counter,
    stmOneOffEventsInvocationTotalSuccess :: Counter,
    stmOneOffEventsInvocationTotalFailure :: Counter,
    stmCronEventsProcessedTotalSuccess :: Counter,
    stmCronEventsProcessedTotalFailure :: Counter,
    stmOneOffEventsProcessedTotalSuccess :: Counter,
    stmOneOffEventsProcessedTotalFailure :: Counter
  }

data SubscriptionMetrics = SubscriptionMetrics
  { submActiveLiveQueryPollers :: Gauge,
    submActiveStreamingPollers :: Gauge,
    submActiveLiveQueryPollersInError :: Gauge,
    submActiveStreamingPollersInError :: Gauge,
    submTotalTime :: HistogramVector.HistogramVector SubscriptionLabel,
    submDBExecTotalTime :: HistogramVector.HistogramVector SubscriptionLabel,
    submActiveSubscriptions :: GaugeVector.GaugeVector SubscriptionLabel
  }

data CacheRequestMetrics = CacheRequestMetrics
  { crmCacheHits :: Counter,
    crmCacheMisses :: Counter
  }

-- | Metrics related to OTel telemetry export; for now the volume of logs and
-- trace spans shipped, and counts of log lines and spans dropped due to high
-- volume.
data OpenTelemetryMetrics = OpenTelemetryMetrics
  { otmSentSpans :: Counter,
    -- | Dropped due to the send buffer being full
    otmDroppedSpansInBuffer :: Counter,
    -- | Dropped due to some error (after retrying) when sending to collector
    otmDroppedSpansInSend :: Counter,
    otmSentLogs :: Counter,
    otmDroppedLogsInBuffer :: Counter,
    otmDroppedLogsInSend :: Counter
  }

-- | Create dummy mutable references without associating them to a metrics
-- store.
makeDummyPrometheusMetrics :: IO PrometheusMetrics
makeDummyPrometheusMetrics = do
  pmConnections <- newConnectionsGauge
  pmGraphQLRequestMetrics <- makeDummyGraphQLRequestMetrics
  pmEventTriggerMetrics <- makeDummyEventTriggerMetrics
  pmWebSocketBytesReceived <- Counter.new
  pmWebSocketBytesSent <- CounterVector.new
  pmActionBytesReceived <- Counter.new
  pmActionBytesSent <- Counter.new
  pmScheduledTriggerMetrics <- makeDummyScheduledTriggerMetrics
  pmSubscriptionMetrics <- makeDummySubscriptionMetrics
  pmWebsocketMsgQueueTimeSeconds <- Histogram.new []
  pmWebsocketMsgWriteTimeSeconds <- Histogram.new []
  pmWebsocketMsgQueued <- Counter.new
  pmWebsocketMsgDropped <- Counter.new
  pmCacheRequestMetrics <- makeDummyCacheRequestMetrics
  pmOpenTelemetryMetrics <- makeDummyOpenTelemetryMetrics
  let pmSample = pure mempty
  pure PrometheusMetrics {..}

makeDummyGraphQLRequestMetrics :: IO GraphQLRequestMetrics
makeDummyGraphQLRequestMetrics = do
  gqlRequests <- CounterVector.new
  gqlExecutionTimeSecondsQuery <- Histogram.new []
  gqlExecutionTimeSecondsMutation <- Histogram.new []
  pure GraphQLRequestMetrics {..}

makeDummyEventTriggerMetrics :: IO EventTriggerMetrics
makeDummyEventTriggerMetrics = do
  eventTriggerHTTPWorkers <- Gauge.new
  eventsFetchQueryTime <- HistogramVector.new []
  eventsFetchedTotal <- CounterVector.new
  eventsFetchedPerBatch <- Gauge.new
  eventQueueTimeSeconds <- HistogramVector.new []
  eventsFetchTimePerBatch <- Histogram.new []
  eventWebhookProcessingTime <- HistogramVector.new []
  eventProcessingTime <- HistogramVector.new []
  eventTriggerBytesReceived <- Counter.new
  eventTriggerBytesSent <- Counter.new
  eventProcessedTotal <- CounterVector.new
  eventInvocationTotal <- CounterVector.new
  pure EventTriggerMetrics {..}

makeDummyScheduledTriggerMetrics :: IO ScheduledTriggerMetrics
makeDummyScheduledTriggerMetrics = do
  stmScheduledTriggerBytesReceived <- Counter.new
  stmScheduledTriggerBytesSent <- Counter.new
  stmCronEventsInvocationTotalSuccess <- Counter.new
  stmCronEventsInvocationTotalFailure <- Counter.new
  stmOneOffEventsInvocationTotalSuccess <- Counter.new
  stmOneOffEventsInvocationTotalFailure <- Counter.new
  stmCronEventsProcessedTotalSuccess <- Counter.new
  stmCronEventsProcessedTotalFailure <- Counter.new
  stmOneOffEventsProcessedTotalSuccess <- Counter.new
  stmOneOffEventsProcessedTotalFailure <- Counter.new
  pure ScheduledTriggerMetrics {..}

makeDummySubscriptionMetrics :: IO SubscriptionMetrics
makeDummySubscriptionMetrics = do
  submActiveLiveQueryPollers <- Gauge.new
  submActiveStreamingPollers <- Gauge.new
  submActiveLiveQueryPollersInError <- Gauge.new
  submActiveStreamingPollersInError <- Gauge.new
  submTotalTime <- HistogramVector.new []
  submDBExecTotalTime <- HistogramVector.new []
  submActiveSubscriptions <- GaugeVector.new
  pure SubscriptionMetrics {..}

makeDummyCacheRequestMetrics :: IO CacheRequestMetrics
makeDummyCacheRequestMetrics = do
  crmCacheHits <- Counter.new
  crmCacheMisses <- Counter.new
  pure CacheRequestMetrics {..}

makeDummyOpenTelemetryMetrics :: IO OpenTelemetryMetrics
makeDummyOpenTelemetryMetrics = do
  otmSentSpans <- Counter.new
  otmDroppedSpansInSend <- Counter.new
  otmDroppedSpansInBuffer <- Counter.new
  otmSentLogs <- Counter.new
  otmDroppedLogsInSend <- Counter.new
  otmDroppedLogsInBuffer <- Counter.new
  pure OpenTelemetryMetrics {..}

--------------------------------------------------------------------------------

-- | A mutable reference for atomically sampling the number of websocket
-- connections and number of threads forked by the warp webserver.
--
-- Because we derive the number of (non-websocket) HTTP connections by the
-- difference of these two metrics, we must sample them simultaneously,
-- otherwise we might report a negative number of HTTP connections.
newtype ConnectionsGauge = ConnectionsGauge (IORef Connections)

data Connections = Connections
  { connWarpThreads :: Int64,
    connWebsockets :: Int64
  }

newConnectionsGauge :: IO ConnectionsGauge
newConnectionsGauge =
  ConnectionsGauge
    <$> newIORef Connections {connWarpThreads = 0, connWebsockets = 0}

readConnectionsGauge :: ConnectionsGauge -> IO Connections
readConnectionsGauge (ConnectionsGauge ref) = readIORef ref

incWarpThreads :: ConnectionsGauge -> IO ()
incWarpThreads =
  modifyConnectionsGauge $ \connections ->
    connections {connWarpThreads = connWarpThreads connections + 1}

decWarpThreads :: ConnectionsGauge -> IO ()
decWarpThreads =
  modifyConnectionsGauge $ \connections ->
    connections {connWarpThreads = connWarpThreads connections - 1}

incWebsocketConnections :: ConnectionsGauge -> IO ()
incWebsocketConnections =
  modifyConnectionsGauge $ \connections ->
    connections {connWebsockets = connWebsockets connections + 1}

decWebsocketConnections :: ConnectionsGauge -> IO ()
decWebsocketConnections =
  modifyConnectionsGauge $ \connections ->
    connections {connWebsockets = connWebsockets connections - 1}

modifyConnectionsGauge ::
  (Connections -> Connections) -> ConnectionsGauge -> IO ()
modifyConnectionsGauge f (ConnectionsGauge ref) =
  atomicModifyIORef' ref $ \connections -> (f connections, ())

data EventTriggerSourceLabel = EventTriggerSourceLabel
  { _eslSourceName :: SourceName
  }
  deriving (Eq, Ord)

instance ToLabels EventTriggerSourceLabel where
  toLabels (EventTriggerSourceLabel {..}) = Map.singleton "source_name" $ sourceNameToText _eslSourceName

data EventsFetchQueryStatusLabel = EventsFetchQueryStatusLabel
  { _efqsSourceName :: SourceName,
    _efqsStatus :: ResponseStatus
  }
  deriving (Eq, Ord)

instance ToLabels EventsFetchQueryStatusLabel where
  toLabels (EventsFetchQueryStatusLabel sourceName status) =
    Map.fromList
      [ ("source_name", sourceNameToText sourceName),
        ("status", responseStatusToLabelValue status)
      ]

data DynamicEventTriggerLabel = DynamicEventTriggerLabel
  { _detlTriggerName :: TriggerName,
    _detlSourceName :: SourceName
  }
  deriving (Ord, Eq)

instance ToLabels (Maybe DynamicEventTriggerLabel) where
  toLabels Nothing = Map.empty
  toLabels (Just (DynamicEventTriggerLabel triggerName sourceName)) = Map.fromList $ [("trigger_name", triggerNameToTxt triggerName), ("source_name", sourceNameToText sourceName)]

data ResponseStatus = Success | Failed
  deriving stock (Generic, Ord, Eq)

-- TODO: Make this a method of a new typeclass of the metrics library
responseStatusToLabelValue :: ResponseStatus -> Text
responseStatusToLabelValue = \case
  Success -> "success"
  Failed -> "failed"

newtype EventStatusLabel = EventStatusLabel
  { status :: Text
  }
  deriving stock (Generic, Ord, Eq)
  deriving anyclass (ToLabels)

eventSuccessLabel :: EventStatusLabel
eventSuccessLabel = EventStatusLabel $ responseStatusToLabelValue Success

eventFailedLabel :: EventStatusLabel
eventFailedLabel = EventStatusLabel $ responseStatusToLabelValue Failed

data EventStatusWithTriggerLabel = EventStatusWithTriggerLabel
  { _eswtlStatus :: EventStatusLabel,
    _eswtlDynamicLabels :: Maybe DynamicEventTriggerLabel
  }
  deriving stock (Generic, Ord, Eq)

instance ToLabels (EventStatusWithTriggerLabel) where
  toLabels (EventStatusWithTriggerLabel esl tl) = (HashMap.fromList $ [("status", status esl)]) <> toLabels tl

data SubscriptionKindLabel = SubscriptionKindLabel
  { subscription_kind :: Text
  }
  deriving stock (Generic, Ord, Eq)
  deriving anyclass (ToLabels)

streamingSubscriptionLabel :: SubscriptionKindLabel
streamingSubscriptionLabel = SubscriptionKindLabel "streaming"

liveQuerySubscriptionLabel :: SubscriptionKindLabel
liveQuerySubscriptionLabel = SubscriptionKindLabel "live-query"

data DynamicGraphqlOperationLabel = DynamicGraphqlOperationLabel
  { _dslParamQueryHash :: Maybe ParameterizedQueryHash,
    _dslOperationName :: Maybe OperationName
  }
  deriving stock (Generic, Ord, Eq)

instance ToLabels DynamicGraphqlOperationLabel where
  toLabels (DynamicGraphqlOperationLabel hash opName) =
    Map.fromList
      $ maybe [] (\pqh -> [("parameterized_query_hash", bsToTxt $ unParamQueryHash pqh)]) hash
      <> maybe [] (\op -> [("operation_name", G.unName $ _unOperationName op)]) opName

data SubscriptionLabel = SubscriptionLabel
  { _slKind :: SubscriptionKindLabel,
    _slDynamicLabels :: Maybe DynamicGraphqlOperationLabel
  }
  deriving stock (Generic, Ord, Eq)

instance ToLabels SubscriptionLabel where
  toLabels (SubscriptionLabel kind Nothing) = Map.fromList $ [("subscription_kind", subscription_kind kind)]
  toLabels (SubscriptionLabel kind (Just dl)) = (Map.fromList $ [("subscription_kind", subscription_kind kind)]) <> toLabels dl

-- TODO: Make this a method of a new typeclass of the metrics library
opTypeToLabelValue :: Maybe G.OperationType -> Text
opTypeToLabelValue = \case
  (Just G.OperationTypeQuery) -> "query"
  (Just G.OperationTypeMutation) -> "mutation"
  (Just G.OperationTypeSubscription) -> "subscription"
  Nothing -> "unknown"

data GraphQLRequestsLabels = GraphQLRequestsLabels
  { operation_type :: Maybe G.OperationType,
    response_status :: ResponseStatus,
    dynamic_label :: Maybe DynamicGraphqlOperationLabel
  }
  deriving stock (Generic, Ord, Eq)

instance ToLabels (GraphQLRequestsLabels) where
  toLabels (GraphQLRequestsLabels op_type res_status dynamic_labels) =
    (HashMap.fromList $ [("operation_type", opTypeToLabelValue op_type), ("response_status", responseStatusToLabelValue res_status)]) <> (fromMaybe mempty (toLabels <$> dynamic_labels))

-- | Record metrics with dynamic label
recordMetricWithLabel ::
  (MonadIO m) =>
  (IO GranularPrometheusMetricsState) ->
  -- should the metric be observed without a label when granularMetricsState is OFF
  Bool ->
  -- the action to perform when granularMetricsState is ON
  IO () ->
  -- the action to perform when granularMetricsState is OFF
  IO () ->
  m ()
recordMetricWithLabel getMetricState alwaysObserve metricActionWithLabel metricActionWithoutLabel = do
  metricState <- liftIO $ getMetricState
  case metricState of
    GranularMetricsOn -> liftIO $ metricActionWithLabel
    -- Some metrics do not make sense without a dynamic label, hence only record the
    -- metric when alwaysObserve is set to true else do not record the metric
    GranularMetricsOff -> do
      when alwaysObserve
        $ liftIO
        $ metricActionWithoutLabel

-- | Observe a histogram metric with a label.
--
-- If the granularity is set to 'GranularMetricsOn', the label will be
-- included in the metric. Otherwise, the label will be set to `Nothing`
observeHistogramWithLabel ::
  (Ord l, MonadIO m) =>
  (IO GranularPrometheusMetricsState) ->
  -- should the metric be observed without a label when granularMetricsState is OFF
  Bool ->
  HistogramVector (Maybe l) ->
  l ->
  Double ->
  m ()
observeHistogramWithLabel getMetricState alwaysObserve histogramVector label value = do
  recordMetricWithLabel
    getMetricState
    alwaysObserve
    (liftIO $ HistogramVector.observe histogramVector (Just label) value)
    (liftIO $ HistogramVector.observe histogramVector Nothing value)

-- | Record a subscription metric for all the operation names present in the subscription.
-- Use this when you want to update the same value of the metric for all the operation names.
recordSubscriptionMetric ::
  (MonadIO m) =>
  (IO GranularPrometheusMetricsState) ->
  -- should the metric be observed without a label when granularMetricsState is OFF
  Bool ->
  HashMap (Maybe OperationName) Int ->
  ParameterizedQueryHash ->
  SubscriptionKindLabel ->
  -- the mertic action to perform
  (SubscriptionLabel -> IO ()) ->
  m ()
recordSubscriptionMetric getMetricState alwaysObserve operationNamesMap parameterizedQueryHash subscriptionKind metricAction = do
  -- if no operation names are present, then emit metric with only param query hash as dynamic label
  if (null operationNamesMap)
    then do
      let promMetricGranularLabel = SubscriptionLabel subscriptionKind (Just $ DynamicGraphqlOperationLabel (Just parameterizedQueryHash) Nothing)
          promMetricLabel = SubscriptionLabel subscriptionKind Nothing
      recordMetricWithLabel
        getMetricState
        alwaysObserve
        (metricAction promMetricGranularLabel)
        (metricAction promMetricLabel)
    else -- if operationNames are present, then emit the same metric for all the operation names
    do
      let operationNames = HashMap.keys operationNamesMap
      for_ operationNames $ \opName -> do
        let promMetricGranularLabel = SubscriptionLabel subscriptionKind (Just $ DynamicGraphqlOperationLabel (Just parameterizedQueryHash) opName)
            promMetricLabel = SubscriptionLabel subscriptionKind Nothing
        recordMetricWithLabel
          getMetricState
          alwaysObserve
          (metricAction promMetricGranularLabel)
          (metricAction promMetricLabel)

recordGraphqlOperationMetric ::
  (MonadIO m) =>
  (IO GranularPrometheusMetricsState) ->
  Maybe G.OperationType ->
  ResponseStatus ->
  Maybe OperationName ->
  Maybe ParameterizedQueryHash ->
  (GraphQLRequestsLabels -> IO ()) ->
  m ()
recordGraphqlOperationMetric getMetricState operationType responseStatus operationName parameterizedQueryHash metricAction = do
  let dynamicLabel = DynamicGraphqlOperationLabel parameterizedQueryHash operationName
      promMetricGranularLabel = GraphQLRequestsLabels operationType responseStatus (Just dynamicLabel)
      promMetricLabel = GraphQLRequestsLabels operationType responseStatus Nothing
  recordMetricWithLabel
    getMetricState
    True
    (metricAction promMetricGranularLabel)
    (metricAction promMetricLabel)

--------------------------------------------------------------------------------
-- Store-backed metrics (BigBlueButton)
--
-- The upstream community edition creates every mutable reference above with
-- 'makeDummyPrometheusMetrics', without registering it to a metric store, so
-- the values are updated but can never be sampled. The specification and
-- constructor below register the same references to an
-- "System.Metrics.Prometheus" store so that GET /v1/metrics can export them.
-- The exported metric families, their labels and the histogram buckets are
-- documented in BBB-CHANGES.md. Fields that are never incremented by
-- community edition code (cache and OpenTelemetry metrics) are deliberately
-- not registered.

-- | Static specification of every metric registered by
-- 'makePrometheusMetrics'.
data PrometheusMetricsSpec :: Symbol -> Symbol -> MetricType -> Type -> Type where
  HttpConnections ::
    PrometheusMetricsSpec
      "hasura_http_connections"
      "Current number of active HTTP connections, excluding websocket connections"
      'GaugeType
      ()
  WebsocketConnections ::
    PrometheusMetricsSpec
      "hasura_websocket_connections"
      "Current number of active websocket connections"
      'GaugeType
      ()
  GraphqlRequestsTotal ::
    PrometheusMetricsSpec
      "hasura_graphql_requests_total"
      "Number of GraphQL requests received"
      'CounterType
      GraphQLRequestsLabels
  GraphqlExecutionTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_graphql_execution_time_seconds"
      "Execution time of successful GraphQL requests, in seconds"
      'HistogramType
      GraphQLOperationTypeLabel
  EventTriggerHttpWorkers ::
    PrometheusMetricsSpec
      "hasura_event_trigger_http_workers"
      "Current number of active event trigger HTTP workers"
      'GaugeType
      ()
  EventsFetchQueryTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_events_fetch_query_time_seconds"
      "Time taken to fetch a batch of events from the database, in seconds"
      'HistogramType
      EventsFetchQueryStatusLabel
  EventsFetchedTotal ::
    PrometheusMetricsSpec
      "hasura_events_fetched_total"
      "Number of events fetched from the database"
      'CounterType
      EventTriggerSourceLabel
  EventsFetchedPerBatch ::
    PrometheusMetricsSpec
      "hasura_events_fetched_per_batch"
      "Number of events fetched from the database in the last batch"
      'GaugeType
      ()
  EventQueueTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_event_queue_time_seconds"
      "Time an event spends waiting in the queue before being processed, in seconds"
      'HistogramType
      (Maybe DynamicEventTriggerLabel)
  EventFetchTimePerBatchSeconds ::
    PrometheusMetricsSpec
      "hasura_event_fetch_time_per_batch_seconds"
      "Time taken to fetch a batch of events, in seconds"
      'HistogramType
      ()
  EventWebhookProcessingTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_event_webhook_processing_time_seconds"
      "Time taken by the webhook to process an event, in seconds"
      'HistogramType
      (Maybe DynamicEventTriggerLabel)
  EventProcessingTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_event_processing_time_seconds"
      "Time taken to process an event end to end, in seconds"
      'HistogramType
      (Maybe DynamicEventTriggerLabel)
  EventTriggerBytesReceivedTotal ::
    PrometheusMetricsSpec
      "hasura_event_trigger_bytes_received_total"
      "Total number of bytes received in event trigger webhook responses"
      'CounterType
      ()
  EventTriggerBytesSentTotal ::
    PrometheusMetricsSpec
      "hasura_event_trigger_bytes_sent_total"
      "Total number of bytes sent in event trigger webhook requests"
      'CounterType
      ()
  EventProcessedTotal ::
    PrometheusMetricsSpec
      "hasura_event_processed_total"
      "Number of events processed"
      'CounterType
      EventStatusWithTriggerLabel
  EventInvocationsTotal ::
    PrometheusMetricsSpec
      "hasura_event_invocations_total"
      "Number of event webhook invocations"
      'CounterType
      EventStatusWithTriggerLabel
  ScheduledTriggerBytesReceivedTotal ::
    PrometheusMetricsSpec
      "hasura_scheduled_trigger_bytes_received_total"
      "Total number of bytes received in scheduled trigger webhook responses"
      'CounterType
      ()
  ScheduledTriggerBytesSentTotal ::
    PrometheusMetricsSpec
      "hasura_scheduled_trigger_bytes_sent_total"
      "Total number of bytes sent in scheduled trigger webhook requests"
      'CounterType
      ()
  CronEventsInvocationTotal ::
    PrometheusMetricsSpec
      "hasura_cron_events_invocation_total"
      "Number of cron event webhook invocations"
      'CounterType
      EventStatusLabel
  OneOffEventsInvocationTotal ::
    PrometheusMetricsSpec
      "hasura_oneoff_events_invocation_total"
      "Number of one-off scheduled event webhook invocations"
      'CounterType
      EventStatusLabel
  CronEventsProcessedTotal ::
    PrometheusMetricsSpec
      "hasura_cron_events_processed_total"
      "Number of cron events processed"
      'CounterType
      EventStatusLabel
  OneOffEventsProcessedTotal ::
    PrometheusMetricsSpec
      "hasura_oneoff_events_processed_total"
      "Number of one-off scheduled events processed"
      'CounterType
      EventStatusLabel
  ActiveSubscriptionPollers ::
    PrometheusMetricsSpec
      "hasura_active_subscription_pollers"
      "Current number of active subscription pollers"
      'GaugeType
      SubscriptionKindLabel
  ActiveSubscriptionPollersInErrorState ::
    PrometheusMetricsSpec
      "hasura_active_subscription_pollers_in_error_state"
      "Current number of active subscription pollers in error state"
      'GaugeType
      SubscriptionKindLabel
  SubscriptionTotalTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_subscription_total_time_seconds"
      "Time taken to complete a subscription poll cycle, in seconds"
      'HistogramType
      SubscriptionLabel
  SubscriptionDbExecutionTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_subscription_db_execution_time_seconds"
      "Time spent executing the subscription query in the database, in seconds"
      'HistogramType
      SubscriptionLabel
  ActiveSubscriptions ::
    PrometheusMetricsSpec
      "hasura_active_subscriptions"
      "Current number of active subscriptions"
      'GaugeType
      SubscriptionLabel
  WebsocketMessagesReceivedBytesTotal ::
    PrometheusMetricsSpec
      "hasura_websocket_messages_received_bytes_total"
      "Total number of bytes received in websocket messages"
      'CounterType
      ()
  WebsocketMessagesSentBytesTotal ::
    PrometheusMetricsSpec
      "hasura_websocket_messages_sent_bytes_total"
      "Total number of bytes sent in websocket messages"
      'CounterType
      DynamicGraphqlOperationLabel
  ActionBytesReceivedTotal ::
    PrometheusMetricsSpec
      "hasura_action_bytes_received_total"
      "Total number of bytes received in action webhook responses"
      'CounterType
      ()
  ActionBytesSentTotal ::
    PrometheusMetricsSpec
      "hasura_action_bytes_sent_total"
      "Total number of bytes sent in action webhook requests"
      'CounterType
      ()
  WebsocketMessageQueueTime ::
    PrometheusMetricsSpec
      "hasura_websocket_message_queue_time"
      "Time a message spends in the websocket send queue, in seconds"
      'HistogramType
      ()
  WebsocketMessageWriteTime ::
    PrometheusMetricsSpec
      "hasura_websocket_message_write_time"
      "Time taken to write a message to the websocket connection, in seconds"
      'HistogramType
      ()
  WebsocketMessagesQueuedTotal ::
    PrometheusMetricsSpec
      "hasura_websocket_messages_queued_total"
      "Total number of messages enqueued for sending over a websocket connection"
      'CounterType
      ()
  WebsocketMessagesDroppedTotal ::
    PrometheusMetricsSpec
      "hasura_websocket_messages_dropped_total"
      "Total number of messages dropped because the websocket send queue was full"
      'CounterType
      ()

-- | Static @operation_type@ label for the GraphQL execution time histograms.
data GraphQLOperationTypeLabel
  = QueryOperationTypeLabel
  | MutationOperationTypeLabel
  deriving stock (Eq, Ord)

instance ToLabels GraphQLOperationTypeLabel where
  toLabels QueryOperationTypeLabel = HashMap.singleton "operation_type" "query"
  toLabels MutationOperationTypeLabel = HashMap.singleton "operation_type" "mutation"

-- | Create mutable references for all the metrics of 'PrometheusMetrics'
-- and register them to the given store, so that they can be sampled (see
-- 'pmSample'). This mirrors 'makeDummyPrometheusMetrics', which creates the
-- same references without a store.
--
-- Can throw a 'System.Metrics.Prometheus.RegistrationError' exception if a
-- metric name/label pair is registered twice; the specification above
-- guarantees this does not happen.
makePrometheusMetrics :: Store PrometheusMetricsSpec -> IO PrometheusMetrics
makePrometheusMetrics store = do
  pmConnections <- newConnectionsGauge
  registerPermanently store
    $ registerGroup
      ( SamplingGroup
          :> (HttpConnections, (), \c -> fromIntegral (connWarpThreads c - connWebsockets c))
          :> (WebsocketConnections, (), fromIntegral . connWebsockets)
      )
      (readConnectionsGauge pmConnections)
  pmGraphQLRequestMetrics <- makeGraphQLRequestMetrics store
  pmEventTriggerMetrics <- makeEventTriggerMetrics store
  pmWebSocketBytesReceived <- createCounter WebsocketMessagesReceivedBytesTotal () store
  pmWebSocketBytesSent <- createCounterVector WebsocketMessagesSentBytesTotal store OrdMap.toList
  pmActionBytesReceived <- createCounter ActionBytesReceivedTotal () store
  pmActionBytesSent <- createCounter ActionBytesSentTotal () store
  pmScheduledTriggerMetrics <- makeScheduledTriggerMetrics store
  pmSubscriptionMetrics <- makeSubscriptionMetrics store
  pmWebsocketMsgQueueTimeSeconds <- createHistogram websocketMessageTimeBuckets WebsocketMessageQueueTime () store
  pmWebsocketMsgWriteTimeSeconds <- createHistogram websocketMessageTimeBuckets WebsocketMessageWriteTime () store
  pmWebsocketMsgQueued <- createCounter WebsocketMessagesQueuedTotal () store
  pmWebsocketMsgDropped <- createCounter WebsocketMessagesDroppedTotal () store
  -- Cache and OpenTelemetry metrics are never incremented by community
  -- edition code, so they are not registered to the store.
  pmCacheRequestMetrics <- makeDummyCacheRequestMetrics
  pmOpenTelemetryMetrics <- makeDummyOpenTelemetryMetrics
  let pmSample = sampleAll store
  pure PrometheusMetrics {..}

makeGraphQLRequestMetrics :: Store PrometheusMetricsSpec -> IO GraphQLRequestMetrics
makeGraphQLRequestMetrics store = do
  gqlRequests <- createCounterVector GraphqlRequestsTotal store OrdMap.toList
  gqlExecutionTimeSecondsQuery <-
    createHistogram graphqlExecutionTimeBuckets GraphqlExecutionTimeSeconds QueryOperationTypeLabel store
  gqlExecutionTimeSecondsMutation <-
    createHistogram graphqlExecutionTimeBuckets GraphqlExecutionTimeSeconds MutationOperationTypeLabel store
  pure GraphQLRequestMetrics {..}

makeEventTriggerMetrics :: Store PrometheusMetricsSpec -> IO EventTriggerMetrics
makeEventTriggerMetrics store = do
  eventTriggerHTTPWorkers <- createGauge EventTriggerHttpWorkers () store
  eventsFetchQueryTime <-
    createHistogramVector eventsFetchQueryTimeBuckets EventsFetchQueryTimeSeconds store OrdMap.toList
  eventsFetchedTotal <- createCounterVector EventsFetchedTotal store OrdMap.toList
  eventsFetchedPerBatch <- createGauge EventsFetchedPerBatch () store
  eventQueueTimeSeconds <-
    createHistogramVector eventProcessingTimeBuckets EventQueueTimeSeconds store OrdMap.toList
  eventsFetchTimePerBatch <-
    createHistogram eventsFetchQueryTimeBuckets EventFetchTimePerBatchSeconds () store
  eventWebhookProcessingTime <-
    createHistogramVector eventWebhookProcessingTimeBuckets EventWebhookProcessingTimeSeconds store OrdMap.toList
  eventProcessingTime <-
    createHistogramVector eventProcessingTimeBuckets EventProcessingTimeSeconds store OrdMap.toList
  eventTriggerBytesReceived <- createCounter EventTriggerBytesReceivedTotal () store
  eventTriggerBytesSent <- createCounter EventTriggerBytesSentTotal () store
  eventProcessedTotal <- createCounterVector EventProcessedTotal store OrdMap.toList
  eventInvocationTotal <- createCounterVector EventInvocationsTotal store OrdMap.toList
  pure EventTriggerMetrics {..}

makeScheduledTriggerMetrics :: Store PrometheusMetricsSpec -> IO ScheduledTriggerMetrics
makeScheduledTriggerMetrics store = do
  stmScheduledTriggerBytesReceived <- createCounter ScheduledTriggerBytesReceivedTotal () store
  stmScheduledTriggerBytesSent <- createCounter ScheduledTriggerBytesSentTotal () store
  stmCronEventsInvocationTotalSuccess <- createCounter CronEventsInvocationTotal eventSuccessLabel store
  stmCronEventsInvocationTotalFailure <- createCounter CronEventsInvocationTotal eventFailedLabel store
  stmOneOffEventsInvocationTotalSuccess <- createCounter OneOffEventsInvocationTotal eventSuccessLabel store
  stmOneOffEventsInvocationTotalFailure <- createCounter OneOffEventsInvocationTotal eventFailedLabel store
  stmCronEventsProcessedTotalSuccess <- createCounter CronEventsProcessedTotal eventSuccessLabel store
  stmCronEventsProcessedTotalFailure <- createCounter CronEventsProcessedTotal eventFailedLabel store
  stmOneOffEventsProcessedTotalSuccess <- createCounter OneOffEventsProcessedTotal eventSuccessLabel store
  stmOneOffEventsProcessedTotalFailure <- createCounter OneOffEventsProcessedTotal eventFailedLabel store
  pure ScheduledTriggerMetrics {..}

makeSubscriptionMetrics :: Store PrometheusMetricsSpec -> IO SubscriptionMetrics
makeSubscriptionMetrics store = do
  submActiveLiveQueryPollers <- createGauge ActiveSubscriptionPollers liveQuerySubscriptionLabel store
  submActiveStreamingPollers <- createGauge ActiveSubscriptionPollers streamingSubscriptionLabel store
  submActiveLiveQueryPollersInError <-
    createGauge ActiveSubscriptionPollersInErrorState liveQuerySubscriptionLabel store
  submActiveStreamingPollersInError <-
    createGauge ActiveSubscriptionPollersInErrorState streamingSubscriptionLabel store
  submTotalTime <-
    createHistogramVector subscriptionTimeBuckets SubscriptionTotalTimeSeconds store OrdMap.toList
  submDBExecTotalTime <-
    createHistogramVector subscriptionTimeBuckets SubscriptionDbExecutionTimeSeconds store OrdMap.toList
  submActiveSubscriptions <- createGaugeVector ActiveSubscriptions store OrdMap.toList
  pure SubscriptionMetrics {..}

-- Histogram buckets of the exported metric families (see BBB-CHANGES.md).

graphqlExecutionTimeBuckets :: [Histogram.UpperBound]
graphqlExecutionTimeBuckets = [0.01, 0.03, 0.1, 0.3, 1, 3, 10]

eventsFetchQueryTimeBuckets :: [Histogram.UpperBound]
eventsFetchQueryTimeBuckets = [0.0001, 0.0003, 0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1, 3, 10, 30, 100]

eventProcessingTimeBuckets :: [Histogram.UpperBound]
eventProcessingTimeBuckets = [0.01, 0.03, 0.1, 0.3, 1, 3, 10, 30, 100]

eventWebhookProcessingTimeBuckets :: [Histogram.UpperBound]
eventWebhookProcessingTimeBuckets = [0.01, 0.03, 0.1, 0.3, 1, 3, 10]

subscriptionTimeBuckets :: [Histogram.UpperBound]
subscriptionTimeBuckets = [0.000001, 0.0001, 0.01, 0.1, 0.3, 1, 3, 10, 30, 100]

websocketMessageTimeBuckets :: [Histogram.UpperBound]
websocketMessageTimeBuckets = [0.000001, 0.0001, 0.01, 0.1, 0.3, 1, 3, 10, 30, 100]
