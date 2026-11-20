{-# LANGUAGE DeriveAnyClass #-}

-- Modified by the BigBlueButton project (https://github.com/bigbluebutton/hasura-graphql-engine):
-- adds a metric specification and a store-backed constructor for the Prometheus
-- metrics, used by the GET /v1/metrics endpoint; observes a subscription poll once, by
-- subscription kind; labels operations by their capped name, always, without the query
-- hash, up to a configurable number of names; adds the per-operation subscription metrics;
-- sets the buckets of the subscription poll times; says in the help of the subscription
-- database time that the pool wait is left out; describes what a full websocket send queue
-- does in the help of its counter; adds the Postgres pool metrics and the pool observer that
-- records them. See BBB-CHANGES.md.

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
    makePrometheusMetricsWithOperationNameLimit,
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
    OperationNameLabel (..),
    operationNameLabel,
    subscriptionLabel,
    pollOperationLabels,
    PollByOperation (..),
    recordPollByOperation,
    forgetPollerByOperation,
    registerPollerByOperation,
    recordPushByOperation,
    splitBySubscribers,
    wholeMicroseconds,
    observePollStart,
    observeSubscriptionInitTime,
    addCohortResultBytes,
    SubscriptionChangedLabel (..),
    unnamedOperationLabel,
    otherOperationLabel,
    defaultOperationNameLimit,
    maxOperationNameLabelLength,
    streamingSubscriptionLabel,
    liveQuerySubscriptionLabel,
    recordMetricWithLabel,
    GraphQLRequestsLabels (..),
    recordGraphqlOperationMetric,
    PostgresPoolMetrics (..),
    PostgresPoolLabel (..),
    PostgresPoolClosedLabel (..),
    sourcePoolLabel,
    metadataPoolLabel,
    postgresPoolObserver,
  )
where

import Control.Exception (mask_)
import Data.HashMap.Internal.Strict qualified as Map
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.Kind (Type)
import Data.Map.Strict qualified as OrdMap
import Data.Text qualified as T
import Data.Time.Clock (diffTimeToPicoseconds)
import Data.UUID (UUID)
import Database.PG.Query.Pool (PGConnectionCloseReason (..), PGPoolObserver (..))
import GHC.Clock (getMonotonicTime)
import GHC.TypeLits (Symbol)
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
    pmWebSocketBytesSent :: CounterVector (Maybe DynamicGraphqlOperationLabel),
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
    pmPostgresPoolMetrics :: PostgresPoolMetrics,
    -- | The operation names admitted as label values so far, and how many (see
    -- 'operationNameLabel').
    pmOperationNameLabels :: IORef (HashSet.HashSet Text, Int),
    -- | How many distinct operation names 'operationNameLabel' admits as label values.
    pmOperationNameLimit :: Int,
    -- | Sample every metric registered to the Prometheus metrics store.
    -- 'makeDummyPrometheusMetrics' returns an empty sample; the sample of
    -- 'makePrometheusMetrics' covers all the metrics above that are backed
    -- by its store.
    pmSample :: IO Sample
  }

-- | How the Postgres pools serve and manage their connections, one series per pool
-- ('PostgresPoolLabel'). A pool that is recreated for the same source keeps adding to the same
-- series.
data PostgresPoolMetrics = PostgresPoolMetrics
  { ppmWaitTime :: HistogramVector PostgresPoolLabel,
    ppmWaitingRequests :: GaugeVector.GaugeVector PostgresPoolLabel,
    ppmConnectionsInUse :: GaugeVector.GaugeVector PostgresPoolLabel,
    ppmConnectionsOpen :: GaugeVector.GaugeVector PostgresPoolLabel,
    ppmMaxConnections :: GaugeVector.GaugeVector PostgresPoolLabel,
    ppmConnectionInitTime :: HistogramVector PostgresPoolLabel,
    ppmConnectionErrors :: CounterVector PostgresPoolLabel,
    ppmConnectionsClosed :: CounterVector PostgresPoolClosedLabel,
    ppmWaitTimeouts :: CounterVector PostgresPoolLabel
  }

-- | The pool a metric is about: a source's name, or "(metadata)" for the metadata database
-- pool. Source names are not restricted, so a source can also be named "(metadata)", and
-- its pool then adds to the same series as the metadata database pool.
newtype PostgresPoolLabel = PostgresPoolLabel {_pplSourceName :: Text}
  deriving stock (Eq, Ord)

instance ToLabels PostgresPoolLabel where
  toLabels (PostgresPoolLabel sourceName) = Map.singleton "source_name" sourceName

data PostgresPoolClosedLabel = PostgresPoolClosedLabel
  { _ppclPool :: PostgresPoolLabel,
    _ppclReason :: Text
  }
  deriving stock (Eq, Ord)

instance ToLabels PostgresPoolClosedLabel where
  toLabels (PostgresPoolClosedLabel pool reason) = toLabels pool <> Map.singleton "reason" reason

sourcePoolLabel :: SourceName -> PostgresPoolLabel
sourcePoolLabel = PostgresPoolLabel . sourceNameToText

metadataPoolLabel :: PostgresPoolLabel
metadataPoolLabel = PostgresPoolLabel "(metadata)"

-- | The pool observer that records a pool in the Postgres pool metrics, under its label.
-- The counters and the waiting, in use and open gauges of the pool start at zero when it is
-- created, so a pool that has not been used, or has had no errors, closes or timeouts, shows
-- zero rather than a missing series.
postgresPoolObserver :: PrometheusMetrics -> PostgresPoolLabel -> IO PGPoolObserver
postgresPoolObserver PrometheusMetrics {pmPostgresPoolMetrics = PostgresPoolMetrics {..}} pool = do
  CounterVector.add ppmConnectionErrors pool 0
  for_ [PGConnectionClosedLifetime, PGConnectionClosedOther] \reason ->
    CounterVector.add ppmConnectionsClosed (closedLabel reason) 0
  CounterVector.add ppmWaitTimeouts pool 0
  GaugeVector.add ppmWaitingRequests pool 0
  GaugeVector.add ppmConnectionsInUse pool 0
  GaugeVector.add ppmConnectionsOpen pool 0
  pure
    PGPoolObserver
      { ppoWaitTime = HistogramVector.observe ppmWaitTime pool,
        ppoRequestWaiting = GaugeVector.inc ppmWaitingRequests pool,
        ppoRequestServed = GaugeVector.dec ppmWaitingRequests pool,
        ppoWaitTimeout = CounterVector.inc ppmWaitTimeouts pool,
        ppoConnectionTaken = GaugeVector.inc ppmConnectionsInUse pool,
        ppoConnectionReturned = GaugeVector.dec ppmConnectionsInUse pool,
        ppoMaxConnections = GaugeVector.set ppmMaxConnections pool . fromIntegral,
        -- The pool reports the time of every connection it opens, so that is where one opens.
        ppoConnectionInitTime = \seconds -> do
          HistogramVector.observe ppmConnectionInitTime pool seconds
          GaugeVector.inc ppmConnectionsOpen pool,
        ppoConnectionError = CounterVector.inc ppmConnectionErrors pool,
        ppoConnectionClosed = \reason -> do
          GaugeVector.dec ppmConnectionsOpen pool
          CounterVector.inc ppmConnectionsClosed $ closedLabel reason
      }
  where
    closedLabel reason = PostgresPoolClosedLabel pool $ case reason of
      PGConnectionClosedLifetime -> "lifetime"
      PGConnectionClosedOther -> "other"

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
    submTotalTime :: HistogramVector.HistogramVector SubscriptionKindLabel,
    submDBExecTotalTime :: HistogramVector.HistogramVector SubscriptionKindLabel,
    submActiveSubscriptions :: GaugeVector.GaugeVector SubscriptionLabel,
    -- | Polls, by the operations the poller serves (+1 for each operation present).
    submPollsByOperation :: CounterVector SubscriptionLabel,
    -- | Failed poll batches, by the operations the poller serves (+1 for each operation present).
    submPollErrorsByOperation :: CounterVector SubscriptionLabel,
    -- | Cohorts processed by the polls of the pollers serving an operation.
    submCohortsProcessedByOperation :: CounterVector SubscriptionLabel,
    -- | Cohorts of the pollers serving an operation, at their last poll.
    submActiveCohortsByOperation :: GaugeVector.GaugeVector SubscriptionLabel,
    -- | What each poller last added to 'submActiveCohortsByOperation', by poller id, so that a
    -- poll applies only the difference and a removed poller takes its share out. A poller is in
    -- it from its creation to its removal.
    submReportedCohorts :: IORef (HashMap UUID (OrdMap.Map SubscriptionLabel Int)),
    -- | Pollers serving an operation: +1 when the operation's first subscriber enters a poller,
    -- -1 when its last one leaves.
    submActivePollersByOperation :: GaugeVector.GaugeVector SubscriptionLabel,
    -- | Results pushed to subscribers, by each subscriber's operation.
    submSubscribersServed :: CounterVector SubscriptionChangedLabel,
    -- | Bytes of the results pushed to subscribers, by each subscriber's operation.
    submSentBytes :: CounterVector SubscriptionChangedLabel,
    -- | Bytes of the cohorts' results returned by the database, split among the operations of
    -- the poller in proportion to their subscribers.
    submResultBytes :: CounterVector SubscriptionChangedLabel,
    -- | Database execution time and poll time, in whole microseconds, split among the operations
    -- of the poller in proportion to their subscribers. Exported in seconds.
    submDBExecutionMicroseconds :: CounterVector SubscriptionLabel,
    submTotalTimeMicroseconds :: CounterVector SubscriptionLabel,
    -- | Time between the starts of consecutive polls of a poller.
    submPollInterval :: HistogramVector.HistogramVector SubscriptionKindLabel,
    -- | Time from a subscription's start message until its poller has it.
    submInitTime :: HistogramVector.HistogramVector SubscriptionKindLabel
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
  pmPostgresPoolMetrics <- makeDummyPostgresPoolMetrics
  pmOperationNameLabels <- newIORef (mempty, 0)
  let pmOperationNameLimit = defaultOperationNameLimit
      pmSample = pure mempty
  pure PrometheusMetrics {..}

makeDummyPostgresPoolMetrics :: IO PostgresPoolMetrics
makeDummyPostgresPoolMetrics = do
  ppmWaitTime <- HistogramVector.new []
  ppmWaitingRequests <- GaugeVector.new
  ppmConnectionsInUse <- GaugeVector.new
  ppmConnectionsOpen <- GaugeVector.new
  ppmMaxConnections <- GaugeVector.new
  ppmConnectionInitTime <- HistogramVector.new []
  ppmConnectionErrors <- CounterVector.new
  ppmConnectionsClosed <- CounterVector.new
  ppmWaitTimeouts <- CounterVector.new
  pure PostgresPoolMetrics {..}

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
  submPollsByOperation <- CounterVector.new
  submPollErrorsByOperation <- CounterVector.new
  submCohortsProcessedByOperation <- CounterVector.new
  submActiveCohortsByOperation <- GaugeVector.new
  submReportedCohorts <- newIORef mempty
  submActivePollersByOperation <- GaugeVector.new
  submSubscribersServed <- CounterVector.new
  submSentBytes <- CounterVector.new
  submResultBytes <- CounterVector.new
  submDBExecutionMicroseconds <- CounterVector.new
  submTotalTimeMicroseconds <- CounterVector.new
  submPollInterval <- HistogramVector.new []
  submInitTime <- HistogramVector.new []
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

-- | The operation_name label of a metric: the operation's name as 'operationNameLabel' admits it.
-- There is no parameterized_query_hash label: it changes with the query text, so series of one
-- operation could not be compared across builds.
newtype DynamicGraphqlOperationLabel = DynamicGraphqlOperationLabel
  { _dslOperationName :: OperationNameLabel
  }
  deriving stock (Generic, Ord, Eq)

instance ToLabels DynamicGraphqlOperationLabel where
  toLabels (DynamicGraphqlOperationLabel (OperationNameLabel name)) = Map.singleton "operation_name" name

-- | A websocket message that belongs to no operation (an acknowledgement, a keepalive) has no
-- operation_name label.
instance ToLabels (Maybe DynamicGraphqlOperationLabel) where
  toLabels = maybe mempty toLabels

-- | An operation_name label value.
newtype OperationNameLabel = OperationNameLabel {unOperationNameLabel :: Text}
  deriving stock (Show, Generic, Ord, Eq)
  deriving newtype (Hashable)

-- | The label of a subscription's series: its kind and its operation's name.
subscriptionLabel :: PrometheusMetrics -> SubscriptionKindLabel -> Maybe OperationName -> IO SubscriptionLabel
subscriptionLabel prometheusMetrics kind operationName =
  SubscriptionLabel kind . Just . DynamicGraphqlOperationLabel <$> operationNameLabel prometheusMetrics operationName

-- | The operations a poller serves, by label, with their numbers of subscribers. Names that get
-- the same label ("(other)") are counted together.
pollOperationLabels :: PrometheusMetrics -> HashMap (Maybe OperationName) Int -> IO (HashMap OperationNameLabel Int)
pollOperationLabels prometheusMetrics names =
  HashMap.fromListWith (+) <$> for (HashMap.toList names) \(name, subscribers) -> (,subscribers) <$> operationNameLabel prometheusMetrics name

-- | A cohort's result pushed to subscribers: each subscriber counts under its own operation,
-- with the bytes of the result's payload.
recordPushByOperation :: PrometheusMetrics -> SubscriptionKindLabel -> Bool -> Int -> [Maybe OperationName] -> IO ()
recordPushByOperation prometheusMetrics@PrometheusMetrics {pmSubscriptionMetrics = SubscriptionMetrics {..}} kind changed bytes operationNames =
  unless (null operationNames) do
    byOperation <- pollOperationLabels prometheusMetrics (HashMap.fromListWith (+) [(name, 1) | name <- operationNames])
    for_ (HashMap.toList byOperation) \(operation, subscribers) -> do
      let label = SubscriptionChangedLabel (SubscriptionLabel kind (Just (DynamicGraphqlOperationLabel operation))) changed
      CounterVector.add submSubscribersServed label (fromIntegral subscribers)
      CounterVector.add submSentBytes label (fromIntegral (subscribers * bytes))

-- | What a poll did, for the per-operation metrics.
data PollByOperation = PollByOperation
  { -- | the poller's id
    pboPollerId :: UUID,
    -- | the poll's batches that failed
    pboFailedBatches :: Int,
    -- | the cohorts the poll processed
    pboCohorts :: Int,
    -- | bytes of the cohorts' results returned by the database, of the cohorts whose result
    -- changed and of those whose result did not
    pboChangedResultBytes :: Int64,
    pboUnchangedResultBytes :: Int64,
    -- | the database execution time of the poll's batches, added up, and the poll's time, in
    -- whole microseconds
    pboDBExecutionMicroseconds :: Int64,
    pboTotalTimeMicroseconds :: Int64
  }

-- | Record a poll in the per-operation metrics of the operations the poller serves. These are
-- events, so each operation present counts the poll once.
recordPollByOperation :: PrometheusMetrics -> SubscriptionKindLabel -> HashMap OperationNameLabel Int -> PollByOperation -> IO ()
recordPollByOperation PrometheusMetrics {pmSubscriptionMetrics = SubscriptionMetrics {..}} kind operations PollByOperation {..} = do
  let labels = [SubscriptionLabel kind (Just (DynamicGraphqlOperationLabel operation)) | operation <- HashMap.keys operations]
  for_ labels \label -> do
    CounterVector.inc submPollsByOperation label
    when (pboFailedBatches > 0) $ CounterVector.add submPollErrorsByOperation label (fromIntegral pboFailedBatches)
    CounterVector.add submCohortsProcessedByOperation label (fromIntegral pboCohorts)
  -- Amounts, unlike events, are split so that the operations' parts add up to the poll's.
  let addSplit :: (Ord l) => CounterVector l -> (SubscriptionLabel -> l) -> Int64 -> IO ()
      addSplit vector toLabel amount =
        for_ (splitBySubscribers amount operations) \(operation, part) ->
          when (part > 0) $ CounterVector.add vector (toLabel (SubscriptionLabel kind (Just (DynamicGraphqlOperationLabel operation)))) (fromIntegral part)
  addSplit submResultBytes (`SubscriptionChangedLabel` True) pboChangedResultBytes
  addSplit submResultBytes (`SubscriptionChangedLabel` False) pboUnchangedResultBytes
  addSplit submDBExecutionMicroseconds id pboDBExecutionMicroseconds
  addSplit submTotalTimeMicroseconds id pboTotalTimeMicroseconds
  -- The gauge gets only the difference from what this poller added at its last poll: an
  -- operation the poller no longer serves drops its share. A poll that ends after its poller was
  -- removed (and so forgotten) changes nothing.
  -- The record and the gauge change together: an exception between the two (the poller stopped)
  -- would leave the record ahead of the gauge, and the removal would take out what the gauge
  -- never got. Neither step blocks.
  let reported = OrdMap.fromList [(label, pboCohorts) | label <- labels]
  -- A poll that reports what the poller's last poll reported (the usual case) writes nothing. Only
  -- the poller writes its own record, so the record cannot change between the read and the write
  -- but by the poller's removal, which the write sees.
  mask_ do
    current <- HashMap.lookup pboPollerId <$> readIORef submReportedCohorts
    when (current /= Just reported) do
      previous <- atomicModifyIORef' submReportedCohorts \byPoller ->
        case HashMap.lookup pboPollerId byPoller of
          Nothing -> (byPoller, Nothing)
          Just previous -> (HashMap.insert pboPollerId reported byPoller, Just previous)
      for_ previous \previousCohorts -> applyCohortsDifference submActiveCohortsByOperation previousCohorts reported

-- | Registers a new poller for the per-operation gauges, before its first poll.
registerPollerByOperation :: PrometheusMetrics -> UUID -> IO ()
registerPollerByOperation PrometheusMetrics {pmSubscriptionMetrics = SubscriptionMetrics {..}} pollerId =
  atomicModifyIORef' submReportedCohorts \byPoller -> (HashMap.insert pollerId mempty byPoller, ())

-- | Adds the bytes of a cohort's result returned by the database to a poll's count, to the
-- changed (first) or unchanged (second) bytes.
addCohortResultBytes :: IORef (Int64, Int64) -> Bool -> Int -> IO ()
addCohortResultBytes resultBytes changed size =
  -- Both fields are forced, as atomicModifyIORef' only forces the pair.
  atomicModifyIORef' resultBytes \(changedBytes, unchangedBytes) ->
    let !changedBytes' = if changed then changedBytes + fromIntegral size else changedBytes
        !unchangedBytes' = if changed then unchangedBytes else unchangedBytes + fromIntegral size
     in ((changedBytes', unchangedBytes'), ())

-- | Observes the time since the previous poll of a poller started, kept in the given reference,
-- and records this start. The first poll of a poller has no previous one and is not observed.
observePollStart :: PrometheusMetrics -> SubscriptionKindLabel -> IORef (Maybe Double) -> IO ()
observePollStart PrometheusMetrics {pmSubscriptionMetrics = SubscriptionMetrics {..}} kind previousStart = do
  start <- getMonotonicTime
  previous <- atomicModifyIORef' previousStart (Just start,)
  for_ previous \previousTime -> HistogramVector.observe submPollInterval kind (start - previousTime)

-- | Observes the time a subscription took to start, since its start message arrived.
observeSubscriptionInitTime :: PrometheusMetrics -> SubscriptionKindLabel -> DiffTime -> IO ()
observeSubscriptionInitTime PrometheusMetrics {pmSubscriptionMetrics = SubscriptionMetrics {..}} kind sinceStart =
  HistogramVector.observe submInitTime kind (realToFrac sinceStart)

-- | A time in whole microseconds (truncated).
wholeMicroseconds :: DiffTime -> Int64
wholeMicroseconds time = fromIntegral (diffTimeToPicoseconds time `div` 1000000)

-- | Splits an amount among operations in proportion to their subscribers, by integer division.
-- The remainder goes to the operation with the most subscribers (the greatest label among
-- equals), so the parts add up to the amount.
splitBySubscribers :: Int64 -> HashMap OperationNameLabel Int -> [(OperationNameLabel, Int64)]
splitBySubscribers amount operations
  | total <= 0 = []
  | otherwise = [(operation, part + if operation == largest then remainder else 0) | (operation, part) <- parts]
  where
    entries = HashMap.toList operations
    total = fromIntegral (sum (map snd entries)) :: Int64
    parts = [(operation, amount * fromIntegral subscribers `div` total) | (operation, subscribers) <- entries]
    remainder = amount - sum (map snd parts)
    largest = snd (maximum [(subscribers, operation) | (operation, subscribers) <- entries])

-- | Take out of the per-operation gauges what a removed poller added to them, and forget the
-- poller, so that a poll of it still running cannot add to them again.
forgetPollerByOperation :: PrometheusMetrics -> UUID -> IO ()
forgetPollerByOperation PrometheusMetrics {pmSubscriptionMetrics = SubscriptionMetrics {..}} pollerId = mask_ do
  previous <- atomicModifyIORef' submReportedCohorts \byPoller ->
    (HashMap.delete pollerId byPoller, HashMap.lookupDefault mempty pollerId byPoller)
  applyCohortsDifference submActiveCohortsByOperation previous mempty

applyCohortsDifference :: GaugeVector.GaugeVector SubscriptionLabel -> OrdMap.Map SubscriptionLabel Int -> OrdMap.Map SubscriptionLabel Int -> IO ()
applyCohortsDifference gauge previous current =
  for_ (OrdMap.toList (OrdMap.unionWith (+) current (negate <$> previous))) \(label, difference) ->
    when (difference /= 0) $ GaugeVector.add gauge label (fromIntegral difference)

-- | The label value of an operation without a name.
unnamedOperationLabel :: OperationNameLabel
unnamedOperationLabel = OperationNameLabel "(unnamed)"

-- | The label value of a name past the cap of distinct names, or longer than 128 characters. The
-- parentheses keep both values apart from real names, which GraphQL restricts to
-- [_A-Za-z][_0-9A-Za-z]* (the engine only sees parsed names).
otherOperationLabel :: OperationNameLabel
otherOperationLabel = OperationNameLabel "(other)"

-- | The operation_name label value of every metric: the name itself for the first
-- 'pmOperationNameLimit' distinct names of up to 'maxOperationNameLabelLength' characters,
-- 'otherOperationLabel' for any other, and 'unnamedOperationLabel' for an operation without a
-- name. The names come from the clients, so without the cap any client could create new series
-- at will.
operationNameLabel :: PrometheusMetrics -> Maybe OperationName -> IO OperationNameLabel
operationNameLabel PrometheusMetrics {pmOperationNameLabels, pmOperationNameLimit} = \case
  Nothing -> pure unnamedOperationLabel
  Just (OperationName name)
    | T.length text > maxOperationNameLabelLength -> pure otherOperationLabel
    | otherwise -> do
        (admitted, _) <- readIORef pmOperationNameLabels
        if HashSet.member text admitted
          then pure (OperationNameLabel text)
          else atomicModifyIORef' pmOperationNameLabels \(names, count) ->
            -- The count is kept alongside, as the size of a set is linear in it.
            if HashSet.member text names
              then ((names, count), OperationNameLabel text)
              else
                if count >= pmOperationNameLimit
                  then ((names, count), otherOperationLabel)
                  else ((HashSet.insert text names, count + 1), OperationNameLabel text)
    where
      text = G.unName name

-- | How many distinct operation names 'operationNameLabel' admits as label values by default
-- (HASURA_GRAPHQL_METRICS_OPERATION_NAME_LIMIT sets it).
defaultOperationNameLimit :: Int
defaultOperationNameLimit = 500

-- | The longest operation name, in characters, that 'operationNameLabel' admits as a label value.
maxOperationNameLabelLength :: Int
maxOperationNameLabelLength = 128

-- | The label of a name already admitted by 'operationNameLabel', and 'otherOperationLabel' for any
-- other name: it only looks the name up, so the name takes no place.
admittedOperationNameLabel :: PrometheusMetrics -> Maybe OperationName -> IO OperationNameLabel
admittedOperationNameLabel PrometheusMetrics {pmOperationNameLabels} = \case
  Nothing -> pure unnamedOperationLabel
  Just (OperationName name) -> do
    (admitted, _) <- readIORef pmOperationNameLabels
    let text = G.unName name
    pure if HashSet.member text admitted then OperationNameLabel text else otherOperationLabel

data SubscriptionLabel = SubscriptionLabel
  { _slKind :: SubscriptionKindLabel,
    _slDynamicLabels :: Maybe DynamicGraphqlOperationLabel
  }
  deriving stock (Generic, Ord, Eq)

instance ToLabels SubscriptionLabel where
  toLabels (SubscriptionLabel kind Nothing) = Map.fromList $ [("subscription_kind", subscription_kind kind)]
  toLabels (SubscriptionLabel kind (Just dl)) = (Map.fromList $ [("subscription_kind", subscription_kind kind)]) <> toLabels dl

-- | A per-operation label, with whether the cohort's result changed since the previous poll.
data SubscriptionChangedLabel = SubscriptionChangedLabel
  { _sclSubscription :: SubscriptionLabel,
    _sclChanged :: Bool
  }
  deriving stock (Generic, Ord, Eq)

instance ToLabels SubscriptionChangedLabel where
  toLabels (SubscriptionChangedLabel subscription changed) =
    toLabels subscription <> Map.singleton "changed" (if changed then "true" else "false")

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

-- | Record a GraphQL request metric, always labeled with the operation's name.
recordGraphqlOperationMetric ::
  (MonadIO m) =>
  PrometheusMetrics ->
  Maybe G.OperationType ->
  ResponseStatus ->
  Maybe OperationName ->
  (GraphQLRequestsLabels -> IO ()) ->
  m ()
recordGraphqlOperationMetric prometheusMetrics operationType responseStatus operationName metricAction = liftIO do
  -- A request that failed (an invalid one, say) does not take a place among the names.
  label <- case responseStatus of
    Success -> operationNameLabel prometheusMetrics operationName
    Failed -> admittedOperationNameLabel prometheusMetrics operationName
  metricAction $ GraphQLRequestsLabels operationType responseStatus (Just (DynamicGraphqlOperationLabel label))

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
  SubscriptionPollIntervalSeconds ::
    PrometheusMetricsSpec
      "hasura_subscription_poll_interval_seconds"
      "Time between the starts of consecutive polls of a subscription poller, in seconds"
      'HistogramType
      SubscriptionKindLabel
  SubscriptionInitTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_subscription_init_time_seconds"
      "Time taken to start a subscription, from the start message until its poller accepts it, in seconds"
      'HistogramType
      SubscriptionKindLabel
  SubscriptionTotalTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_subscription_total_time_seconds"
      "Time taken to complete a subscription poll cycle, in seconds"
      'HistogramType
      SubscriptionKindLabel
  SubscriptionDbExecutionTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_subscription_db_execution_time_seconds"
      "Time spent executing the subscription query in the database, excluding the wait for a pool connection, in seconds"
      'HistogramType
      SubscriptionKindLabel
  SubscriptionPollsTotal ::
    PrometheusMetricsSpec
      "hasura_subscription_polls_total"
      "Total number of subscription polls, by the operations the poller serves"
      'CounterType
      SubscriptionLabel
  SubscriptionPollErrorsTotal ::
    PrometheusMetricsSpec
      "hasura_subscription_poll_errors_total"
      "Total number of subscription poll batches that failed"
      'CounterType
      SubscriptionLabel
  SubscriptionCohortsProcessedTotal ::
    PrometheusMetricsSpec
      "hasura_subscription_cohorts_processed_total"
      "Total number of cohorts processed by the polls of the pollers serving an operation"
      'CounterType
      SubscriptionLabel
  ActiveSubscriptionCohortsByOperation ::
    PrometheusMetricsSpec
      "hasura_active_subscription_cohorts_by_operation"
      "Current number of active cohorts of the pollers serving an operation"
      'GaugeType
      SubscriptionLabel
  SubscriptionSubscribersServedTotal ::
    PrometheusMetricsSpec
      "hasura_subscription_subscribers_served_total"
      "Total number of subscription results pushed to subscribers"
      'CounterType
      SubscriptionChangedLabel
  SubscriptionSentBytesTotal ::
    PrometheusMetricsSpec
      "hasura_subscription_sent_bytes_total"
      "Total number of bytes of subscription results pushed to subscribers"
      'CounterType
      SubscriptionChangedLabel
  SubscriptionResultBytesTotal ::
    PrometheusMetricsSpec
      "hasura_subscription_result_bytes_total"
      "Total number of bytes of subscription results returned by the database"
      'CounterType
      SubscriptionChangedLabel
  SubscriptionDBExecutionSecondsTotal ::
    PrometheusMetricsSpec
      "hasura_subscription_db_execution_seconds_total"
      "Total time spent executing subscription queries in the database, excluding the pool wait, in seconds"
      'CounterType
      SubscriptionLabel
  SubscriptionTotalTimeSecondsTotal ::
    PrometheusMetricsSpec
      "hasura_subscription_total_time_seconds_total"
      "Total time spent in subscription poll cycles, in seconds"
      'CounterType
      SubscriptionLabel
  ActiveSubscriptionPollersByOperation ::
    PrometheusMetricsSpec
      "hasura_active_subscription_pollers_by_operation"
      "Current number of active subscription pollers serving an operation"
      'GaugeType
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
      (Maybe DynamicGraphqlOperationLabel)
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
  PostgresPoolWaitTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_postgres_pool_wait_time_seconds"
      "Time spent waiting for a connection from the Postgres pool, in seconds"
      'HistogramType
      PostgresPoolLabel
  PostgresPoolWaitingRequests ::
    PrometheusMetricsSpec
      "hasura_postgres_pool_waiting_requests"
      "Current number of requests waiting for a Postgres pool connection"
      'GaugeType
      PostgresPoolLabel
  PostgresPoolConnectionsInUse ::
    PrometheusMetricsSpec
      "hasura_postgres_pool_connections_in_use"
      "Current number of Postgres pool connections in use"
      'GaugeType
      PostgresPoolLabel
  PostgresPoolConnectionsOpen ::
    PrometheusMetricsSpec
      "hasura_postgres_pool_connections_open"
      "Current number of open Postgres pool connections"
      'GaugeType
      PostgresPoolLabel
  PostgresPoolMaxConnections ::
    PrometheusMetricsSpec
      "hasura_postgres_pool_max_connections"
      "Maximum number of connections in the Postgres pool"
      'GaugeType
      PostgresPoolLabel
  PostgresConnectionInitTimeSeconds ::
    PrometheusMetricsSpec
      "hasura_postgres_connection_init_time_seconds"
      "Time taken to open a new Postgres connection, in seconds"
      'HistogramType
      PostgresPoolLabel
  PostgresConnectionErrorsTotal ::
    PrometheusMetricsSpec
      "hasura_postgres_connection_errors_total"
      "Total number of failed attempts to open a Postgres connection"
      'CounterType
      PostgresPoolLabel
  PostgresConnectionsClosedTotal ::
    PrometheusMetricsSpec
      "hasura_postgres_connections_closed_total"
      "Total number of Postgres connections closed by the pool"
      'CounterType
      PostgresPoolClosedLabel
  PostgresPoolWaitTimeoutsTotal ::
    PrometheusMetricsSpec
      "hasura_postgres_pool_wait_timeouts_total"
      "Total number of requests that timed out waiting for a Postgres pool connection"
      'CounterType
      PostgresPoolLabel
  WebsocketMessagesDroppedTotal ::
    PrometheusMetricsSpec
      "hasura_websocket_messages_dropped_total"
      "Total number of messages rejected because the websocket send queue was full; each rejection cancels its operation"
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
makePrometheusMetrics = makePrometheusMetricsWithOperationNameLimit defaultOperationNameLimit

-- | 'makePrometheusMetrics' with the number of distinct operation names admitted as label values.
makePrometheusMetricsWithOperationNameLimit :: Int -> Store PrometheusMetricsSpec -> IO PrometheusMetrics
makePrometheusMetricsWithOperationNameLimit pmOperationNameLimit store = do
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
  pmPostgresPoolMetrics <- makePostgresPoolMetrics store
  pmOperationNameLabels <- newIORef (mempty, 0)
  let pmSample = sampleAll store
  pure PrometheusMetrics {..}

makePostgresPoolMetrics :: Store PrometheusMetricsSpec -> IO PostgresPoolMetrics
makePostgresPoolMetrics store = do
  ppmWaitTime <- createHistogramVector pollAndPoolTimeBuckets PostgresPoolWaitTimeSeconds store OrdMap.toList
  ppmWaitingRequests <- createGaugeVector PostgresPoolWaitingRequests store OrdMap.toList
  ppmConnectionsInUse <- createGaugeVector PostgresPoolConnectionsInUse store OrdMap.toList
  ppmConnectionsOpen <- createGaugeVector PostgresPoolConnectionsOpen store OrdMap.toList
  ppmMaxConnections <- createGaugeVector PostgresPoolMaxConnections store OrdMap.toList
  ppmConnectionInitTime <- createHistogramVector pollAndPoolTimeBuckets PostgresConnectionInitTimeSeconds store OrdMap.toList
  ppmConnectionErrors <- createCounterVector PostgresConnectionErrorsTotal store OrdMap.toList
  ppmConnectionsClosed <- createCounterVector PostgresConnectionsClosedTotal store OrdMap.toList
  ppmWaitTimeouts <- createCounterVector PostgresPoolWaitTimeoutsTotal store OrdMap.toList
  pure PostgresPoolMetrics {..}

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
    createHistogramVector pollAndPoolTimeBuckets SubscriptionTotalTimeSeconds store OrdMap.toList
  submDBExecTotalTime <-
    createHistogramVector pollAndPoolTimeBuckets SubscriptionDbExecutionTimeSeconds store OrdMap.toList
  submActiveSubscriptions <- createGaugeVector ActiveSubscriptions store OrdMap.toList
  submPollsByOperation <- createCounterVector SubscriptionPollsTotal store OrdMap.toList
  submPollErrorsByOperation <- createCounterVector SubscriptionPollErrorsTotal store OrdMap.toList
  submCohortsProcessedByOperation <- createCounterVector SubscriptionCohortsProcessedTotal store OrdMap.toList
  submActiveCohortsByOperation <- createGaugeVector ActiveSubscriptionCohortsByOperation store OrdMap.toList
  submReportedCohorts <- newIORef mempty
  submActivePollersByOperation <- createGaugeVector ActiveSubscriptionPollersByOperation store OrdMap.toList
  submSubscribersServed <- createCounterVector SubscriptionSubscribersServedTotal store OrdMap.toList
  submSentBytes <- createCounterVector SubscriptionSentBytesTotal store OrdMap.toList
  submResultBytes <- createCounterVector SubscriptionResultBytesTotal store OrdMap.toList
  -- Kept in microseconds so that the parts add up exactly; exported in seconds.
  let microsecondsToSeconds = map (fmap (/ 1000000)) . OrdMap.toList
  submDBExecutionMicroseconds <- createCounterVector SubscriptionDBExecutionSecondsTotal store microsecondsToSeconds
  submTotalTimeMicroseconds <- createCounterVector SubscriptionTotalTimeSecondsTotal store microsecondsToSeconds
  submPollInterval <- createHistogramVector pollAndPoolTimeBuckets SubscriptionPollIntervalSeconds store OrdMap.toList
  submInitTime <- createHistogramVector pollAndPoolTimeBuckets SubscriptionInitTimeSeconds store OrdMap.toList
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

-- | Buckets for the subscription poll times (total and database), the time between polls, the
-- time to start a subscription and the two Postgres pool times (the wait for a connection and
-- opening a connection). They resolve the poll intervals in use (150 ms for streaming, 333 ms
-- for live queries) and the range up to 30 s.
pollAndPoolTimeBuckets :: [Histogram.UpperBound]
pollAndPoolTimeBuckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.333, 0.5, 0.75, 1, 2.5, 5, 10, 30]

websocketMessageTimeBuckets :: [Histogram.UpperBound]
websocketMessageTimeBuckets = [0.000001, 0.0001, 0.01, 0.1, 0.3, 1, 3, 10, 30, 100]
