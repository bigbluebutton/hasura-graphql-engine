{-# LANGUAGE QuasiQuotes #-}

-- | Benchmarks of the subscription metrics:
--
-- * @export@: rendering the whole Prometheus export with the per-operation series of many
--   operation names, at the default operation name limit and at a limit of 10,000 (needs
--   nothing else);
-- * @poll@: one streaming poll of a poller with many cohorts and subscribers, against the
--   database at @HASURA_GRAPHQL_DATABASE_URL@, the cost a poll pays for its metrics included.
--
-- Run with @cabal bench subscription-metrics@ (criterion options after @--benchmark-options@).
module Main (main) where

import Control.Concurrent.STM qualified as STM
import Control.DeepSeq (NFData (..))
import Control.Immortal qualified as Immortal
import Criterion.Main
import Data.Aeson qualified as J
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Lazy qualified as BL
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (newIORef, readIORef)
import Data.Text qualified as T
import Data.UUID qualified as UUID (fromString)
import Data.UUID.V4 qualified as UUID
import Database.PG.Query qualified as PG
import GHC.AssertNF.CPP (disableAssertNF)
import Hasura.Authentication.Role (mkRoleName)
import Hasura.Backends.Postgres.Connection
import Hasura.Backends.Postgres.Execute.Subscription (MultiplexedQuery (MultiplexedQuery))
import Hasura.Backends.Postgres.Instances.Execute ()
import Hasura.Backends.Postgres.Instances.Transport ()
import Hasura.Backends.Postgres.SQL.Value (TxtEncodedVal (TELit))
import Hasura.Base.Error (showQErr)
import Hasura.GraphQL.Execute.Subscription.Options (mkSubscriptionsOptions)
import Hasura.GraphQL.Execute.Subscription.Plan
import Hasura.GraphQL.Execute.Subscription.Poll.Common
import Hasura.GraphQL.Execute.Subscription.Poll.StreamingQuery (pollStreamingQuery)
import Hasura.GraphQL.Execute.Subscription.State
import Hasura.GraphQL.ParameterizedQueryHash
import Hasura.GraphQL.Transport.HTTP.Protocol (OperationName (..))
import Hasura.GraphQL.Transport.WebSocket.Protocol (unsafeMkOperationId)
import Hasura.GraphQL.Transport.WebSocket.Server qualified as WS
import Hasura.Logging
import Hasura.Name qualified as Name
import Hasura.Prelude
import Hasura.RQL.Types.BackendType
import Hasura.RQL.Types.Common
import Hasura.RQL.Types.ResizePool (ResizePoolStrategy (..))
import Hasura.Server.Metrics (createServerMetrics)
import Hasura.Server.Prometheus
import Hasura.Server.Types (ModelInfoLogState (ModelInfoLogOff), RequestId (..))
import Language.GraphQL.Draft.Syntax (unsafeMkName)
import Language.GraphQL.Draft.Syntax qualified as G
import Language.GraphQL.Draft.Syntax.QQ qualified as G
import ListT qualified
import StmContainers.Map qualified as STMMap
import System.Environment (lookupEnv)
import System.Metrics (newStore)
import System.Metrics.Prometheus qualified as Prometheus
import System.Metrics.Prometheus.CounterVector qualified as CounterVector
import System.Metrics.Prometheus.Export qualified as PromExport
import System.Metrics.Prometheus.GaugeVector qualified as GaugeVector

main :: IO ()
main = do
  -- As the server does at startup ("Hasura.App"): the heap checks of 'assertNFHere' are for
  -- tests and would weigh on the poll.
  disableAssertNF
  srcConfig <- lookupEnv "HASURA_GRAPHQL_DATABASE_URL" >>= traverse sourceConfigFor
  defaultMain
    $ exportBenchmarks
    <> pollMetricsBenchmarks
    <> maybe [] pollBenchmarks srcConfig

-- * Export

-- | The whole export, with every per-operation series of each name, for both kinds.
exportBenchmarks :: [Benchmark]
exportBenchmarks =
  [ bgroup
      "export"
      [ env (Opaque <$> populatedMetrics limit names) \(Opaque metrics) -> bench (show names <> " names, limit " <> show limit) $ nfIO (render metrics)
      | (limit, names) <- [(defaultOperationNameLimit, n) | n <- [0, 10, 100, 500, 1000]] <> [(10000, 10000)]
      ]
  ]
  where
    render metrics = BL.length . BB.toLazyByteString . PromExport.sampleToPrometheus <$> pmSample metrics

-- | Metrics registered in a store with the given operation name limit, with every per-operation
-- series of the given number of names.
populatedMetrics :: Int -> Int -> IO PrometheusMetrics
populatedMetrics limit names = do
  store <- Prometheus.newStore @PrometheusMetricsSpec
  metrics <- makePrometheusMetricsWithOperationNameLimit limit store
  let SubscriptionMetrics {..} = pmSubscriptionMetrics metrics
  for_ [1 .. names] \n -> do
    let name = Just (OperationName (unsafeMkName ("Operation" <> tshow n)))
    for_ [liveQuerySubscriptionLabel, streamingSubscriptionLabel] \kind -> do
      label <- subscriptionLabel metrics kind name
      GaugeVector.inc submActiveSubscriptions label
      GaugeVector.inc submActiveCohortsByOperation label
      GaugeVector.inc submActivePollersByOperation label
      for_ [submPollsByOperation, submPollErrorsByOperation, submCohortsProcessedByOperation, submDBExecutionMicroseconds, submTotalTimeMicroseconds] \vector ->
        CounterVector.inc vector label
      for_ [True, False] \changed ->
        for_ [submSubscribersServed, submSentBytes, submResultBytes] \vector ->
          CounterVector.inc vector (SubscriptionChangedLabel label changed)
    recordGraphqlOperationMetric metrics (Just G.OperationTypeSubscription) Success name (CounterVector.inc (gqlRequests (pmGraphQLRequestMetrics metrics)))
    operation <- operationNameLabel metrics name
    CounterVector.add (pmWebSocketBytesSent metrics) (Just (DynamicGraphqlOperationLabel operation)) 100
  rendered <- BL.length . BB.toLazyByteString . PromExport.sampleToPrometheus <$> pmSample metrics
  putStrLn $ "export with " <> show names <> " names, limit " <> show limit <> ": " <> show rendered <> " bytes"
  pure metrics

-- * The metrics work of a poll

-- | What the per-operation metrics add to a poll, on its own: for each of 1000 cohorts, its push
-- to 5 subscribers of 10 operation names and its result bytes, then the poll's record, as
-- 'pollStreamingQuery' does them.
pollMetricsBenchmarks :: [Benchmark]
pollMetricsBenchmarks =
  [ bgroup
      "poll metrics"
      [ env (Opaque <$> (makeDummyPrometheusMetrics >>= \metrics -> metrics <$ registerPollerByOperation metrics pollerId)) \(Opaque metrics) ->
          bench "1000 cohorts, 5000 subscribers, 10 names" $ whnfIO (pollMetrics metrics)
      ]
  ]
  where
    cohorts = 1000 :: Int
    names = [Just (OperationName (unsafeMkName ("Operation" <> tshow n))) | n <- [0 .. 9 :: Int]]
    subscribersOf cohort = [names !! ((cohort * 5 + s) `mod` 10) | s <- [1 .. 5]]
    operationNames = HashMap.fromListWith (+) [(name, 1 :: Int) | cohort <- [1 .. cohorts], name <- subscribersOf cohort]
    pollerId = fromMaybe (error "uuid") (UUID.fromString "6f1f7a5e-4b4a-4cf1-9a35-3c1f7c1b2d10")
    pollMetrics metrics = do
      resultBytes <- newIORef (0, 0)
      for_ [1 .. cohorts] \cohort -> do
        recordPushByOperation metrics streamingSubscriptionLabel True 120 (subscribersOf cohort)
        addCohortResultBytes resultBytes True 120
      (changedBytes, unchangedBytes) <- readIORef resultBytes
      operations <- pollOperationLabels metrics operationNames
      recordPollByOperation metrics streamingSubscriptionLabel operations
        $ PollByOperation
          { pboPollerId = pollerId,
            pboFailedBatches = 0,
            pboCohorts = cohorts,
            pboChangedResultBytes = changedBytes,
            pboUnchangedResultBytes = unchangedBytes,
            pboDBExecutionMicroseconds = 250000,
            pboTotalTimeMicroseconds = 300000
          }

-- * Poll

-- | One poll of a streaming poller with 1000 cohorts of 5 subscribers each, of 10 operation
-- names: with results that change at every poll (pushed to every subscriber) and with empty
-- results (pushed to no one).
pollBenchmarks :: PGSourceConfig -> [Benchmark]
pollBenchmarks srcConfig =
  [ bgroup
      "poll"
      [ env (Opaque <$> newPoller srcConfig 1000 5 10 changing) \(Opaque poll) -> bench "1000 cohorts, 5000 subscribers, changing results" $ whnfIO poll,
        env (Opaque <$> newPoller srcConfig 1000 5 10 pastTheRows) \(Opaque poll) -> bench "1000 cohorts, 5000 subscribers, empty results" $ whnfIO poll
      ]
  ]
  where
    -- Cohorts 2000 rows apart, so that their cursors, one row a poll, do not meet.
    changing cohort = cohort * 2000
    pastTheRows cohort = rowCount + cohort

rowCount :: Int
rowCount = 2000000

-- | A poller whose thread is stopped, with the given cohorts (cursor of each), subscribers per
-- cohort and operation names; returns one poll of it.
newPoller :: PGSourceConfig -> Int -> Int -> Int -> (Int -> Int) -> IO (IO ())
newPoller srcConfig cohorts subscribersPerCohort names cursorOf = do
  subscriptionState <- initSubscriptionsState (const (pure ()))
  serverMetrics <- newStore >>= createServerMetrics
  metrics <- makeDummyPrometheusMetrics
  wsId <- WS.mkUnsafeWSId <$> UUID.nextRandom
  let options = mkSubscriptionsOptions Nothing Nothing
      quiet :: Logger Hasura = Logger \_ -> pure ()
      role = fromMaybe (error "role") (mkRoleName "user")
      plan cohortId cursor =
        SubscriptionQueryPlan
          (ParameterizedSubscriptionQueryPlan role (MultiplexedQuery streamingQuery))
          srcConfig
          cohortId
          Nothing
          (mkCohortVariables mempty mempty mempty mempty (mkUnsafeValidateVariables (HashMap.singleton Name._id (TELit (tshow cursor)))))
          Nothing
  for_ [1 .. cohorts] \cohort -> do
    cohortId <- newCohortId
    for_ [1 .. subscribersPerCohort] \s -> do
      let subscriber = tshow cohort <> "-" <> tshow s
          name = Just (OperationName (unsafeMkName ("Operation" <> tshow ((cohort * subscribersPerCohort + s) `mod` names))))
      addStreamSubscriptionQuery
        @('Postgres 'Vanilla)
        quiet
        serverMetrics
        metrics
        (mkSubscriberMetadata wsId (unsafeMkOperationId subscriber) Nothing (RequestId subscriber))
        subscriptionState
        (pure (options, options))
        SNDefault
        (mkUnsafeParameterizedQueryHash "bench")
        name
        (RequestId subscriber)
        [G.name|bench_numbers|]
        (plan cohortId (cursorOf cohort))
        (pure GranularMetricsOff)
        (const (pure ()))
        Nothing
        []
        (pure ModelInfoLogOff)
  pollers <- STM.atomically $ fmap snd <$> ListT.toList (STMMap.listT (_ssStreamQueryMap subscriptionState))
  poller <- maybe (error "no poller") pure (listToMaybe pollers)
  ioState <- STM.atomically (STM.readTMVar (_pIOState poller))
  Immortal.stop (_pThread ioState)
  pure
    $ pollStreamingQuery
      @('Postgres 'Vanilla)
      (_pId ioState)
      (_pPollerState poller)
      options
      (SNDefault, srcConfig)
      role
      (mkUnsafeParameterizedQueryHash "bench")
      (MultiplexedQuery streamingQuery)
      (_pCohorts poller)
      [G.name|bench_numbers|]
      (const (pure ()))
      Nothing
      metrics
      (pure GranularMetricsOff)
      (_pOperationNamesMap poller)
      Nothing
      Nothing
      quiet
      []
      (pure ModelInfoLogOff)

-- | A value for criterion's environment, which must be in 'NFData', left as it is.
newtype Opaque a = Opaque a

instance NFData (Opaque a) where
  rnf (Opaque _) = ()

-- | The source, with the table the poll reads.
sourceConfigFor :: String -> IO (PGSourceConfig)
sourceConfigFor url = do
  let connInfo = PG.ConnInfo 1 $ PG.CDDatabaseURI $ txtToBs (T.pack url)
  pool <- PG.initPGPool connInfo J.Null PG.defaultConnParams print
  connInfoWithFinalizer <- mkConnInfoWithFinalizer connInfo (pure ())
  let srcConfig = PGSourceConfig (mkPGExecCtx PG.ReadCommitted pool NeverResizePool) connInfoWithFinalizer Nothing (pure ()) defaultPostgresExtensionsSchema mempty ConnTemplate_NotApplicable
      run sql = runPgSourceWriteTx srcConfig InternalRawQuery (PG.unitQE defaultTxErrorHandler (PG.fromText sql) () False) >>= (`onLeft` (error . T.unpack . showQErr))
  run "DROP TABLE IF EXISTS \"public\".\"bench_numbers\""
  run "CREATE TABLE \"public\".\"bench_numbers\" (id int primary key)"
  run ("INSERT INTO \"public\".\"bench_numbers\" SELECT generate_series(1, " <> tshow rowCount <> ")")
  pure srcConfig

-- | The query generated for
-- @subscription { bench_numbers_stream(cursor: {initial_value: {id: 1}}, batch_size: 1) { id } }@.
streamingQuery :: PG.Query
streamingQuery =
  [PG.sql|
     SELECT  "_subs"."result_id" , "_fld_resp"."root" AS "result", "_fld_resp"."cursor" AS "cursor" FROM UNNEST(($1)::uuid[], ($2)::json[]) AS "_subs"("result_id", "result_vars")
     LEFT OUTER JOIN LATERAL
     (SELECT  json_build_object('bench_numbers', "bench_numbers"."root" ) AS "root", to_json("bench_numbers"."cursor" ) AS "cursor"
      FROM
        (SELECT  coalesce(json_agg("root" ORDER BY "root.pg.id" ASC ), '[]' ) AS "root", json_build_object('id', (MAX("root.pg.id" ))::text ) AS "cursor"
        FROM
        (SELECT  row_to_json((SELECT  "_1_e"  FROM  (SELECT  "_0_root.base"."id" AS "id"       ) AS "_1_e"      ) ) AS "root", "_0_root.base"."id" AS "root.pg.id"
        FROM  (SELECT  *  FROM "public"."bench_numbers"  WHERE (("public"."bench_numbers"."id") > ((("_subs"."result_vars"#>>ARRAY['cursor', 'id']))::integer))   ORDER BY "id" ASC  LIMIT 1 ) AS "_0_root.base"
        ORDER BY "root.pg.id" ASC   ) AS "_2_root"      ) AS "bench_numbers"      )
        AS "_fld_resp" ON ('true')
  |]
