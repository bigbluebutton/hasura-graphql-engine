-- Modified by the BigBlueButton project (https://github.com/bigbluebutton/hasura-graphql-engine):
-- tests the poller's operation name count, and the poll metrics that use it, when
-- subscribers of one name are removed from two cohorts; the count and the active
-- subscriptions gauge when a subscriber that is in two cohorts is removed; and the database
-- execution time of a subscription query that waits for a pool connection or that ends with
-- a statement error or an exception. See BBB-CHANGES.md.
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Hasura.StreamingSubscriptionSuite (buildStreamingSubscriptionSuite) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async qualified as Async
import Control.Concurrent.MVar qualified as MVar
import Control.Concurrent.STM qualified as STM
import Control.Exception qualified as Exception
import Control.Immortal qualified as Immortal
import Control.Lens ((.~))
import Data.Aeson qualified as J
import Data.ByteString.Lazy.UTF8 qualified as LBS
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Text qualified as T
import Data.Text.IO qualified as T
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID
import Database.PG.Query qualified as PG
import Hasura.Authentication.Role (RoleName, mkRoleName)
import Hasura.Backends.Postgres.Connection
import Hasura.Backends.Postgres.Execute.Subscription (MultiplexedQuery (MultiplexedQuery))
import Hasura.Backends.Postgres.Instances.Transport (withQueryExecutionTime)
import Hasura.Backends.Postgres.SQL.Value (TxtEncodedVal (TELit))
import Hasura.Base.Error (Code (Unexpected), QErr, err500, showQErr)
import Hasura.GraphQL.Execute.Subscription.Options (mkSubscriptionsOptions)
import Hasura.GraphQL.Execute.Subscription.Plan
import Hasura.GraphQL.Execute.Subscription.Poll.Common
import Hasura.GraphQL.Execute.Subscription.Poll.LiveQuery (pollLiveQuery)
import Hasura.GraphQL.Execute.Subscription.Poll.StreamingQuery (pollStreamingQuery)
import Hasura.GraphQL.Execute.Subscription.State
import Hasura.GraphQL.Execute.Subscription.TMap qualified as TMap
import Hasura.GraphQL.ParameterizedQueryHash
import Hasura.GraphQL.Transport.Backend (BackendTransport (..))
import Hasura.GraphQL.Transport.HTTP.Protocol (OperationName (..))
import Hasura.GraphQL.Transport.WebSocket.Protocol (unsafeMkOperationId)
import Hasura.GraphQL.Transport.WebSocket.Server qualified as WS
import Hasura.Logging
import Hasura.Name qualified as Name
import Hasura.Prelude
import Hasura.RQL.Types.Backend
import Hasura.RQL.Types.BackendType
import Hasura.RQL.Types.Common
import Hasura.RQL.Types.ResizePool (ResizePoolStrategy (..))
import Hasura.Server.Init (considerEnv, databaseUrlOption, runWithEnv, _envVar)
import Hasura.Server.Metrics (createServerMetrics)
import Hasura.Server.Prometheus (DynamicGraphqlOperationLabel (..), OperationNameLabel (..), PrometheusMetrics (..), SubscriptionLabel (..), SubscriptionMetrics (..), liveQuerySubscriptionLabel, makeDummyPrometheusMetrics, streamingSubscriptionLabel)
import Hasura.Server.Types (GranularPrometheusMetricsState (..), ModelInfoLogState (ModelInfoLogOff), RequestId (..))
import Language.GraphQL.Draft.Syntax.QQ qualified as G
import ListT qualified
import StmContainers.Map qualified as STMMap
import System.Environment (getEnvironment)
import System.Exit (exitFailure)
import System.Metrics
import System.Metrics.Prometheus qualified as Prometheus
import System.Metrics.Prometheus.GaugeVector qualified as GaugeVector
import System.Metrics.Prometheus.Histogram (HistogramSample (..))
import System.Metrics.Prometheus.HistogramVector qualified as HistogramVector
import Test.Hspec

buildStreamingSubscriptionSuite :: IO Spec
buildStreamingSubscriptionSuite = do
  env <- getEnvironment

  pgUrlText :: Text <- flip onLeft (printErrExit . T.pack)
    $ runWithEnv env
    $ do
      let envVar = _envVar databaseUrlOption
      maybeV <- considerEnv envVar
      onNothing maybeV
        $ throwError
        $ "Expected: "
        <> envVar

  let pgConnInfo = PG.ConnInfo 1 $ PG.CDDatabaseURI $ txtToBs pgUrlText

  pgPool <- PG.initPGPool pgConnInfo J.Null PG.defaultConnParams print
  connInfoWithFinalizer <- liftIO $ mkConnInfoWithFinalizer pgConnInfo (pure ())

  let pgContext = mkPGExecCtx PG.ReadCommitted pgPool NeverResizePool
      dbSourceConfig = PGSourceConfig pgContext connInfoWithFinalizer Nothing (pure ()) defaultPostgresExtensionsSchema mempty ConnTemplate_NotApplicable

  -- A pool of one connection that gives up waiting for it after 1 s (the pool counts its timeout
  -- in whole seconds), destroyed by the test that uses it.
  timeoutPool <- PG.initPGPool pgConnInfo J.Null PG.defaultConnParams {PG.cpConns = 1, PG.cpTimeout = Just 1} print

  pure
    $ describe "Streaming subscriptions polling tests"
    $ streamingSubscriptionPollingSpec dbSourceConfig timeoutPool

mkRoleNameE :: Text -> RoleName
mkRoleNameE = fromMaybe (error "Use a non empty string") . mkRoleName

-- | CohortStaticSnapshot is a data type to help to compare two cohorts at a specified instant.
--   Note that two Cohorts cannot be directly compared because they have mutable references within
--   them.
data CohortStaticSnapshot = CohortStaticSnapshot
  { _cssCohortId :: CohortId,
    _cssExistingSubscribers :: [SubscriberId],
    _cssNewSubscribers :: [SubscriberId]
  }
  deriving (Show, Eq)

getStaticCohortSnapshot :: Cohort (STM.TVar CursorVariableValues) -> STM.STM CohortStaticSnapshot
getStaticCohortSnapshot (Cohort cohortId _respRef existingSubsTV newSubsTV _) = do
  existingSubs <- TMap.toList existingSubsTV
  newSubs <- TMap.toList newSubsTV
  pure $ CohortStaticSnapshot cohortId (fst <$> existingSubs) (fst <$> newSubs)

streamingSubscriptionPollingSpec :: SourceConfig ('Postgres 'Vanilla) -> PG.PGPool -> Spec
streamingSubscriptionPollingSpec srcConfig timeoutPool = do
  dummyServerStore <- runIO newStore
  dummyServerMetrics <- runIO $ createServerMetrics dummyServerStore
  dummyPromMetrics <- runIO makeDummyPrometheusMetrics
  let setupDDLTx =
        PG.unitQE
          defaultTxErrorHandler
          [PG.sql|
            CREATE TABLE "public"."numbers" (id int primary key);
          |]
          ()
          False

      setupValueTx =
        PG.unitQE
          defaultTxErrorHandler
          [PG.sql|
            INSERT INTO "public"."numbers" values (1),(2),(3),(4),(5);
          |]
          ()
          False

      teardownDDLTx =
        PG.unitQE
          defaultTxErrorHandler
          [PG.sql|
              DROP TABLE "public"."numbers";
         |]
          ()
          False

  let setup = do
        runPgSourceWriteTx srcConfig InternalRawQuery setupDDLTx >>= (`onLeft` (printErrExit . showQErr))
        runPgSourceWriteTx srcConfig InternalRawQuery setupValueTx >>= (`onLeft` (printErrExit . showQErr))

      teardown =
        runPgSourceWriteTx srcConfig InternalRawQuery teardownDDLTx >>= (`onLeft` (printErrExit . showQErr))

  runIO setup

  pollerId <- runIO $ PollerId <$> UUID.nextRandom
  pollerResponseState <- runIO $ STM.newTVarIO PRSSuccess
  emptyOperationNamesMap <- runIO $ STM.atomically $ TMap.new
  let defaultSubscriptionOptions = mkSubscriptionsOptions Nothing Nothing -- use default values
      paramQueryHash = mkUnsafeParameterizedQueryHash "random"
      -- hardcoded multiplexed query which is generated for the following GraphQL query:
      --
      -- { subscription { numbers_stream(cursor: {initial_value: {id: 1}}, batch_size: 1) { id }} }
      multiplexedQuery =
        [PG.sql|
           SELECT  "_subs"."result_id" , "_fld_resp"."root" AS "result", "_fld_resp"."cursor" AS "cursor" FROM UNNEST(($1)::uuid[], ($2)::json[]) AS "_subs"("result_id", "result_vars")
           LEFT OUTER JOIN LATERAL
           (SELECT  json_build_object('numbers_stream', "numbers_stream"."root" ) AS "root", to_json("numbers_stream"."cursor" ) AS "cursor"
            FROM
              (SELECT  coalesce(json_agg("root" ORDER BY "root.pg.id" ASC ), '[]' ) AS "root", json_build_object('id', (MAX("root.pg.id" ))::text ) AS "cursor"
              FROM
              (SELECT  row_to_json((SELECT  "_1_e"  FROM  (SELECT  "_0_root.base"."id" AS "id"       ) AS "_1_e"      ) ) AS "root", "_0_root.base"."id" AS "root.pg.id"
              FROM  (SELECT  *  FROM "public"."numbers"  WHERE (("public"."numbers"."id") > ((("_subs"."result_vars"#>>ARRAY['cursor', 'id']))::integer))   ORDER BY "id" ASC  LIMIT 1 ) AS "_0_root.base"
              ORDER BY "root.pg.id" ASC   ) AS "_2_root"      ) AS "numbers_stream"      )
              AS "_fld_resp" ON ('true')
        |]
  let logger' :: Logger Hasura = Logger $ \l -> do
        let (logLevel, logType :: EngineLogType Hasura, logDetail) = toEngineLog l
        t <- liftIO $ getFormattedTime Nothing
        liftIO $ putStrLn $ LBS.toString $ J.encode $ EngineLog t logLevel logType logDetail Nothing Nothing
  let pollingAction cohortMap testSyncAction =
        pollStreamingQuery
          @('Postgres 'Vanilla)
          pollerId
          pollerResponseState
          defaultSubscriptionOptions
          (SNDefault, srcConfig)
          (mkRoleNameE "random")
          paramQueryHash
          (MultiplexedQuery multiplexedQuery)
          cohortMap
          [G.name|randomRootField|]
          (const $ pure ())
          testSyncAction
          dummyPromMetrics
          (pure GranularMetricsOff)
          emptyOperationNamesMap
          Nothing
          Nothing
          logger'
          []
          (pure ModelInfoLogOff)

      mkSubscriber sId =
        let wsId = maybe (error "Invalid UUID") WS.mkUnsafeWSId $ UUID.fromString "ec981f92-8d5a-47ab-a306-80af7cfb1113"
            requestId = RequestId "randomRequestId"
            subscriberMetadata = mkSubscriberMetadata wsId (unsafeMkOperationId "randomOperationId") Nothing requestId
         in Subscriber sId subscriberMetadata requestId Nothing (const (pure ()))

      mkNewCohort cohortId initialCursorValue = do
        latestCursorValues <- STM.newTVar (CursorVariableValues initialCursorValue)
        Cohort cohortId <$> STM.newTVar Nothing <*> TMap.new <*> TMap.new <*> pure latestCursorValues

      addSubscriberToCohort subscriber cohort =
        TMap.insert subscriber (_sId subscriber) $ _cNewSubscribers cohort

      -- The multiplexed query we chose only deals with cursor variables, so we might as
      -- well create a new function with the other variables being set to `mempty`
      mkCohortVariables' = mkCohortVariables mempty mempty mempty mempty

  describe "Streaming subscription poll" $ do
    cohortId1 <- runIO newCohortId
    (subscriberId1, subscriberId2) <- runIO $ (,) <$> newSubscriberId <*> newSubscriberId
    let subscriber1 = mkSubscriber subscriberId1
        subscriber2 = mkSubscriber subscriberId2
    let initialCursorValue = HashMap.singleton Name._id (TELit "1")
    cohort1 <- runIO
      $ liftIO
      $ STM.atomically
      $ do
        cohort1' <- mkNewCohort cohortId1 initialCursorValue
        -- adding a subscriber to the newly created cohort
        addSubscriberToCohort subscriber1 cohort1'
        pure cohort1'
    cohortMap <- runIO $ liftIO $ STM.atomically $ TMap.new
    let cohortKey1 = mkCohortVariables' (mkUnsafeValidateVariables initialCursorValue)
    cohortId2 <- runIO newCohortId
    cohort2 <- runIO
      $ liftIO
      $ STM.atomically
      $ do
        cohort2' <- mkNewCohort cohortId2 initialCursorValue
        addSubscriberToCohort subscriber2 cohort2'
        pure cohort2'

    let mkCohortKey n = cohortKey1 & cvCursorVariables . unValidatedVariables . ix [G.name|id|] .~ TELit n
        cohortKey2 = mkCohortKey "2"
        cohortKey3 = mkCohortKey "3"

    describe "after first poll, the key of the cohort should be updated to contain the next cursor value" $ do
      runIO
        $ STM.atomically
        $ do
          TMap.reset cohortMap
          TMap.insert cohort1 cohortKey1 cohortMap

      runIO $ pollingAction cohortMap Nothing
      currentCohortMap <- runIO $ STM.atomically $ TMap.getMap cohortMap

      it "the key of the cohort1 should have been moved from the cohortKey1 to cohortKey2, so it should not be found anymore at cohortKey1" $ do
        cohortMappedToCohortKey1 <- STM.atomically $ traverse getStaticCohortSnapshot $ HashMap.lookup cohortKey1 currentCohortMap
        cohortMappedToCohortKey1 `shouldBe` Nothing

      it "the key of the cohort1 should have been moved from the cohortKey1 to cohortKey2, so it should be found anymore at cohortKey2" $ do
        cohortMappedToCohortKey2 <- STM.atomically $ traverse getStaticCohortSnapshot $ HashMap.lookup cohortKey2 currentCohortMap
        cohortMappedToCohortKey2 `shouldBe` Just (CohortStaticSnapshot cohortId1 [subscriberId1] mempty)

    describe "manipulating cohorts" $ do
      it "adding a new cohort concurrently to the poll, should leave the new cohort as it is in the cohort map" $ do
        syncMVar <- MVar.newEmptyMVar -- MVar to synchronize code
        STM.atomically $ do
          TMap.reset cohortMap
          TMap.insert cohort1 cohortKey1 cohortMap
        let syncAction = do
              -- This action will block until the `syncMVar` is not empty. This action
              -- is run after the cohorts are snapshotted in `pollStreamingQuery`
              MVar.readMVar syncMVar
              STM.atomically $ TMap.insert cohort2 cohortKey3 cohortMap
        Async.withAsync
          (pollingAction cohortMap (Just syncAction))
          ( \pollAsync -> do
              MVar.putMVar syncMVar ()
              Async.wait pollAsync
          )
        currentCohortMap <- STM.atomically $ TMap.getMap cohortMap
        let currentCohort2 = HashMap.lookup cohortKey3 currentCohortMap

        (originalCohort2StaticSnapshot, currentCohort2StaticSnapshot) <-
          STM.atomically
            $ (,)
            <$> getStaticCohortSnapshot cohort2
            <*> traverse getStaticCohortSnapshot currentCohort2
        Just originalCohort2StaticSnapshot `shouldBe` currentCohort2StaticSnapshot

      it "deleting a cohort concurrently should not retain the deleted cohort in the cohort map" $ do
        STM.atomically $ do
          -- reset the cohort map and add a single cohort
          TMap.reset cohortMap
          TMap.insert cohort2 cohortKey1 cohortMap
        syncMVar <- MVar.newEmptyMVar
        let syncAction = do
              MVar.readMVar syncMVar
              STM.atomically $ TMap.delete cohortKey1 cohortMap
        Async.withAsync
          (pollingAction cohortMap (Just syncAction))
          ( \pollAsync -> do
              MVar.putMVar syncMVar ()
              Async.wait pollAsync
          )
        currentCohortMap <- STM.atomically $ TMap.getMap cohortMap
        HashMap.size currentCohortMap `shouldBe` 0 -- since there was only one cohort initially, now the cohort map should not have any cohorts
    describe "manipulating adding and deleting of subscribers concurrently" $ do
      it "deleting a subscriber from a cohort should not retain the subscriber in any of the cohorts" $ do
        temporarySubscriberId <- newSubscriberId
        let newTemporarySubscriber = mkSubscriber temporarySubscriberId
        STM.atomically $ do
          TMap.reset cohortMap
          TMap.insert cohort1 cohortKey1 cohortMap
          addSubscriberToCohort newTemporarySubscriber cohort1
        syncMVar <- MVar.newEmptyMVar
        let syncAction = do
              MVar.readMVar syncMVar
              STM.atomically $ TMap.delete temporarySubscriberId (_cNewSubscribers cohort1)
        Async.withAsync
          (pollingAction cohortMap (Just syncAction))
          ( \pollAsync -> do
              MVar.putMVar syncMVar ()
              Async.wait pollAsync
          )
        currentCohortMap <- STM.atomically $ TMap.getMap cohortMap
        let cohortKey2Cohort = HashMap.lookup cohortKey2 currentCohortMap
            cohortKey1Cohort = HashMap.lookup cohortKey1 currentCohortMap
        cohortKey1CohortSnapshot <- STM.atomically $ traverse getStaticCohortSnapshot cohortKey1Cohort
        cohortKey2CohortSnapshot <- STM.atomically $ traverse getStaticCohortSnapshot cohortKey2Cohort

        -- check the deleted subscriber in the older cohort
        _cssExistingSubscribers
          <$> cohortKey1CohortSnapshot
          `shouldSatisfy` (\existingSubs -> temporarySubscriberId `notElem` concat (maybeToList existingSubs))
        _cssNewSubscribers
          <$> cohortKey1CohortSnapshot
          `shouldSatisfy` (\newSubs -> temporarySubscriberId `notElem` concat (maybeToList newSubs))
        _cssExistingSubscribers
          <$> cohortKey2CohortSnapshot
          `shouldSatisfy` (\existingSubs -> temporarySubscriberId `notElem` concat (maybeToList existingSubs))
        _cssNewSubscribers
          <$> cohortKey2CohortSnapshot
          `shouldSatisfy` (\newSubs -> temporarySubscriberId `notElem` concat (maybeToList newSubs))
        STM.atomically
          $ TMap.delete temporarySubscriberId (_cNewSubscribers cohort1)

    describe "Adding two subscribers concurrently" $ do
      subscriptionState <- do
        runIO $ initSubscriptionsState (const (pure ()))

      let requestId1 = RequestId "request-id1"
          requestId2 = RequestId "request-id2"

      dummyWSId <- runIO $ WS.mkUnsafeWSId <$> UUID.nextRandom
      cohortId <- runIO newCohortId

      let parameterizedSubscriptionQueryPlan = ParameterizedSubscriptionQueryPlan (mkRoleNameE "user") $ MultiplexedQuery multiplexedQuery
          opID1 = unsafeMkOperationId "1"
          opID2 = unsafeMkOperationId "2"
          subscriber1Metadata = mkSubscriberMetadata dummyWSId opID1 Nothing requestId1
          subscriber2Metadata = mkSubscriberMetadata dummyWSId opID2 Nothing requestId2
          dummyParamQueryHash = mkUnsafeParameterizedQueryHash "unsafeParamHash"

          subscriptionQueryPlan =
            SubscriptionQueryPlan
              parameterizedSubscriptionQueryPlan
              srcConfig
              cohortId
              Nothing
              cohortKey1
              Nothing

      let logger :: Logger Hasura = Logger $ \l -> do
            let (logLevel, logType :: EngineLogType Hasura, logDetail) = toEngineLog l
            t <- liftIO $ getFormattedTime Nothing
            liftIO $ putStrLn $ LBS.toString $ J.encode $ EngineLog t logLevel logType logDetail Nothing Nothing

          subOptions = mkSubscriptionsOptions Nothing Nothing
          addStreamSubQuery subscriberMetadata reqId =
            addStreamSubscriptionQuery
              @('Postgres 'Vanilla)
              logger
              dummyServerMetrics
              dummyPromMetrics
              subscriberMetadata
              subscriptionState
              (pure (subOptions, subOptions))
              SNDefault
              dummyParamQueryHash
              Nothing
              reqId
              [G.name|numbers_stream|]
              subscriptionQueryPlan
              (pure GranularMetricsOff)
              (const (pure ()))
              Nothing
              []
              (pure ModelInfoLogOff)

      it "concurrently adding two subscribers should retain both of them in the poller map" $ do
        -- Adding two subscribers that query identical queries should be adding them into the same
        -- cohort of the same poller
        liftIO
          $ Async.concurrently_
            (addStreamSubQuery subscriber1Metadata requestId1)
            (addStreamSubQuery subscriber2Metadata requestId2)

        let streamQueryMap = _ssStreamQueryMap subscriptionState

        streamQueryMapEntries <- STM.atomically $ ListT.toList $ STMMap.listT streamQueryMap
        length streamQueryMapEntries `shouldBe` 1
        let (pollerKey, (Poller currentCohortMap _ ioState _ _)) = head streamQueryMapEntries
        cohorts <- STM.atomically $ TMap.toList currentCohortMap
        length cohorts `shouldBe` 1
        let (_cohortKey, Cohort _ _ curSubsTV newSubsTV _) = head cohorts
        allSubscribers <- STM.atomically $ do
          curSubs <- TMap.toList curSubsTV
          newSubs <- TMap.toList newSubsTV
          pure $ curSubs <> newSubs
        length allSubscribers `shouldBe` 2

        threadRef <- STM.atomically $ do
          STMMap.delete pollerKey streamQueryMap
          _pThread <$> STM.readTMVar ioState
        Immortal.stop threadRef

    describe "Removing subscribers of one operation name spread over two cohorts" $ do
      subscriptionState <- do
        runIO $ initSubscriptionsState (const (pure ()))

      dummyWSId <- runIO $ WS.mkUnsafeWSId <$> UUID.nextRandom
      (cohortIdA, cohortIdB) <- runIO $ (,) <$> newCohortId <*> newCohortId

      let operationName = Just $ OperationName [G.name|NumbersStream|]
          requestIdA = RequestId "request-id-a"
          requestIdB = RequestId "request-id-b"
          subscriberAMetadata = mkSubscriberMetadata dummyWSId (unsafeMkOperationId "a") Nothing requestIdA
          subscriberBMetadata = mkSubscriberMetadata dummyWSId (unsafeMkOperationId "b") Nothing requestIdB
          dummyParamQueryHash = mkUnsafeParameterizedQueryHash "unsafeParamHash"
          parameterizedSubscriptionQueryPlan = ParameterizedSubscriptionQueryPlan (mkRoleNameE "user") $ MultiplexedQuery multiplexedQuery
          -- Two cursor values past the last row of "numbers": polls leave them as they are, so
          -- the two subscribers stay in two cohorts of one poller.
          cohortKeyA = mkCohortKey "10"
          cohortKeyB = mkCohortKey "20"
          queryPlanFor cohortId cohortKey = SubscriptionQueryPlan parameterizedSubscriptionQueryPlan srcConfig cohortId Nothing cohortKey Nothing
          subOptions = mkSubscriptionsOptions Nothing Nothing
          logger :: Logger Hasura = Logger $ \_ -> pure ()
          addSubscriber subscriberMetadata reqId queryPlan =
            addStreamSubscriptionQuery
              @('Postgres 'Vanilla)
              logger
              dummyServerMetrics
              dummyPromMetrics
              subscriberMetadata
              subscriptionState
              (pure (subOptions, subOptions))
              SNDefault
              dummyParamQueryHash
              operationName
              reqId
              [G.name|numbers_stream|]
              queryPlan
              (pure GranularMetricsOff)
              (const (pure ()))
              Nothing
              []
              (pure ModelInfoLogOff)
          removeSubscriber subscriberDetails =
            removeStreamingQuery
              logger
              dummyServerMetrics
              dummyPromMetrics
              subscriptionState
              subscriberDetails
              (pure GranularMetricsOff)
              operationName
          pollerOf = do
            entries <- STM.atomically $ ListT.toList $ STMMap.listT (_ssStreamQueryMap subscriptionState)
            pure $ snd <$> listToMaybe entries
          operationNameCount =
            pollerOf >>= \case
              Nothing -> pure Nothing
              Just poller -> STM.atomically $ TMap.lookup operationName (_pOperationNamesMap poller)

      subscriberA <- runIO $ addSubscriber subscriberAMetadata requestIdA (queryPlanFor cohortIdA cohortKeyA)
      subscriberB <- runIO $ addSubscriber subscriberBMetadata requestIdB (queryPlanFor cohortIdB cohortKeyB)

      it "both subscribers share one poller, in two cohorts, and the operation name is counted twice" $ do
        Just (Poller pollerCohorts _ _ _ _) <- pollerOf
        cohorts <- STM.atomically $ TMap.toList pollerCohorts
        length cohorts `shouldBe` 2
        operationNameCount `shouldReturn` Just 2

      it "removing one subscriber decrements the operation name count once" $ do
        removeSubscriber subscriberA
        operationNameCount `shouldReturn` Just 1

      it "removing the same subscriber again leaves the operation name count as it is" $ do
        removeSubscriber subscriberA
        operationNameCount `shouldReturn` Just 1

      it "removing the last subscriber removes the operation name, and the poller with it" $ do
        Just poller <- pollerOf
        removeSubscriber subscriberB
        STM.atomically (TMap.lookup operationName (_pOperationNamesMap poller)) `shouldReturn` Nothing
        isNothing <$> pollerOf `shouldReturn` True

    describe "Poll metrics of one operation name spread over two cohorts, after one subscriber is removed" $ do
      subscriptionState <- runIO $ initSubscriptionsState (const (pure ()))
      dummyWSId <- runIO $ WS.mkUnsafeWSId <$> UUID.nextRandom
      (cohortIdA, cohortIdB) <- runIO $ (,) <$> newCohortId <*> newCohortId
      let operationName = Just $ OperationName [G.name|NumbersStream|]
          parameterizedSubscriptionQueryPlan = ParameterizedSubscriptionQueryPlan (mkRoleNameE "user") $ MultiplexedQuery multiplexedQuery
          queryPlanFor cohortId cohortKey = SubscriptionQueryPlan parameterizedSubscriptionQueryPlan srcConfig cohortId Nothing cohortKey Nothing
          subOptions = mkSubscriptionsOptions Nothing Nothing
          quietLogger :: Logger Hasura = Logger $ \_ -> pure ()
          addSubscriber opId reqId queryPlan =
            addStreamSubscriptionQuery
              @('Postgres 'Vanilla)
              quietLogger
              dummyServerMetrics
              dummyPromMetrics
              (mkSubscriberMetadata dummyWSId (unsafeMkOperationId opId) Nothing reqId)
              subscriptionState
              (pure (subOptions, subOptions))
              SNDefault
              paramQueryHash
              operationName
              reqId
              [G.name|numbers_stream|]
              queryPlan
              (pure GranularMetricsOff)
              (const (pure ()))
              Nothing
              []
              (pure ModelInfoLogOff)
          removeSubscriber subscriberDetails =
            removeStreamingQuery quietLogger dummyServerMetrics dummyPromMetrics subscriptionState subscriberDetails (pure GranularMetricsOff) operationName
          -- One poll of the poller's cohorts, recorded in a fresh set of metrics.
          pollOnce metricsState = do
            entries <- STM.atomically $ ListT.toList $ STMMap.listT (_ssStreamQueryMap subscriptionState)
            Poller pollerCohorts _ _ _ pollerOperationNames <- maybe (error "no poller") (pure . snd) (listToMaybe entries)
            promMetrics <- makeDummyPrometheusMetrics
            pollStreamingQuery
              @('Postgres 'Vanilla)
              pollerId
              pollerResponseState
              defaultSubscriptionOptions
              (SNDefault, srcConfig)
              (mkRoleNameE "user")
              paramQueryHash
              (MultiplexedQuery multiplexedQuery)
              pollerCohorts
              [G.name|numbers_stream|]
              (const $ pure ())
              Nothing
              promMetrics
              (pure metricsState)
              pollerOperationNames
              Nothing
              Nothing
              quietLogger
              []
              (pure ModelInfoLogOff)
            seriesCounts <$> HistogramVector.read (submTotalTime $ pmSubscriptionMetrics promMetrics)

      subscriberA <- runIO $ addSubscriber "a" (RequestId "request-id-a") (queryPlanFor cohortIdA (mkCohortKey "10"))
      subscriberB <- runIO $ addSubscriber "b" (RequestId "request-id-b") (queryPlanFor cohortIdB (mkCohortKey "20"))
      runIO $ removeSubscriber subscriberA
      granularObservations <- runIO $ pollOnce GranularMetricsOn
      plainObservations <- runIO $ pollOnce GranularMetricsOff
      runIO $ removeSubscriber subscriberB

      -- The poll time histograms are by subscription kind only, so a poll is one observation
      -- whatever the granularity setting.
      it "with granular metrics, the poll is recorded once, by subscription kind" $ do
        granularObservations `shouldBe` labeledCounts [(streamingSubscriptionLabel, 1)]

      it "without granular metrics, the poll is recorded once, by subscription kind" $ do
        plainObservations `shouldBe` labeledCounts [(streamingSubscriptionLabel, 1)]

    describe "A subscriber added during a poll in which its cohort's key advances" $ do
      -- The poll runs here, while the spec is built and the "numbers" table still exists, so
      -- the cursor really advances (a poll inside an "it" runs after the teardown).
      subscriptionState <- runIO $ initSubscriptionsState (const (pure ()))
      promMetrics <- runIO makeDummyPrometheusMetrics
      dummyWSId <- runIO $ WS.mkUnsafeWSId <$> UUID.nextRandom
      (cohortIdA, cohortIdB) <- runIO $ (,) <$> newCohortId <*> newCohortId
      let operationName = Just $ OperationName [G.name|NumbersStream|]
          parameterizedSubscriptionQueryPlan = ParameterizedSubscriptionQueryPlan (mkRoleNameE "user") $ MultiplexedQuery multiplexedQuery
          queryPlanFor cohortId cohortKey = SubscriptionQueryPlan parameterizedSubscriptionQueryPlan srcConfig cohortId Nothing cohortKey Nothing
          subOptions = mkSubscriptionsOptions Nothing Nothing
          quietLogger :: Logger Hasura = Logger $ \_ -> pure ()
          addSubscriber opId reqId queryPlan =
            addStreamSubscriptionQuery
              @('Postgres 'Vanilla)
              quietLogger
              dummyServerMetrics
              promMetrics
              (mkSubscriberMetadata dummyWSId (unsafeMkOperationId opId) Nothing reqId)
              subscriptionState
              (pure (subOptions, subOptions))
              SNDefault
              paramQueryHash
              operationName
              reqId
              [G.name|numbers_stream|]
              queryPlan
              (pure GranularMetricsOff)
              (const (pure ()))
              Nothing
              []
              (pure ModelInfoLogOff)
          removeSubscriber subscriberDetails =
            removeStreamingQuery quietLogger dummyServerMetrics promMetrics subscriptionState subscriberDetails (pure GranularMetricsOff) operationName
          thePoller = do
            entries <- STM.atomically $ ListT.toList $ STMMap.listT (_ssStreamQueryMap subscriptionState)
            maybe (error "no poller") (pure . snd) (listToMaybe entries)
          -- How many cohorts of the poller each subscriber is in.
          cohortsPerSubscriber poller = STM.atomically do
            cohorts <- TMap.toList (_pCohorts poller)
            snapshots <- for cohorts (getStaticCohortSnapshot . snd)
            pure
              $ sort
              $ HashMap.elems
              $ HashMap.fromListWith (+) [(subscriberId, 1 :: Int) | snapshot <- snapshots, subscriberId <- _cssExistingSubscribers snapshot <> _cssNewSubscribers snapshot]
          keyOfTheOnlyCohort poller = STM.atomically do
            cohorts <- TMap.toList (_pCohorts poller)
            case cohorts of
              [(key, _)] -> pure key
              _ -> error "expected a single cohort"
          operationNameCount poller = STM.atomically $ TMap.lookup operationName (_pOperationNamesMap poller)
          activeSubscriptions =
            Map.lookup (SubscriptionLabel streamingSubscriptionLabel (Just (DynamicGraphqlOperationLabel (OperationNameLabel "NumbersStream"))))
              <$> GaugeVector.read (submActiveSubscriptions $ pmSubscriptionMetrics promMetrics)

      subscriberA <- runIO $ addSubscriber "a" (RequestId "request-id-a") (queryPlanFor cohortIdA cohortKey1)
      poller <- runIO thePoller
      -- Stop the poller's own thread, so the only poll from here on is the one below.
      runIO $ (STM.atomically $ _pThread <$> STM.readTMVar (_pIOState poller)) >>= Immortal.stop
      keyOfA <- runIO $ keyOfTheOnlyCohort poller
      subscriberB <- runIO $ MVar.newEmptyMVar
      -- B joins A's cohort after the poll has taken its snapshot.
      runIO
        $ pollStreamingQuery
          @('Postgres 'Vanilla)
          pollerId
          pollerResponseState
          defaultSubscriptionOptions
          (SNDefault, srcConfig)
          (mkRoleNameE "user")
          paramQueryHash
          (MultiplexedQuery multiplexedQuery)
          (_pCohorts poller)
          [G.name|numbers_stream|]
          (const $ pure ())
          (Just $ addSubscriber "b" (RequestId "request-id-b") (queryPlanFor cohortIdB keyOfA) >>= MVar.putMVar subscriberB)
          dummyPromMetrics
          (pure GranularMetricsOff)
          (_pOperationNamesMap poller)
          Nothing
          Nothing
          quietLogger
          []
          (pure ModelInfoLogOff)
      detailsOfB <- runIO $ MVar.readMVar subscriberB
      membershipsAfterPoll <- runIO $ cohortsPerSubscriber poller
      countBefore <- runIO $ operationNameCount poller
      activeBefore <- runIO activeSubscriptions
      runIO $ removeSubscriber detailsOfB
      countAfterB <- runIO $ operationNameCount poller
      activeAfterB <- runIO activeSubscriptions
      runIO $ removeSubscriber subscriberA

      it "the subscriber added during the poll ends up in two cohorts" $ do
        -- A is in the cohort of the advanced key; B also in a new cohort under the old key.
        membershipsAfterPoll `shouldBe` [1, 2]
        countBefore `shouldBe` Just 2
        activeBefore `shouldBe` Just 2

      it "removing it counts its operation name down once" $ do
        countAfterB `shouldBe` Just 1

      it "removing it counts the active subscriptions down once" $ do
        activeAfterB `shouldBe` Just 1

    describe "Removing the last subscriber of a live query poller whose thread is not installed" $ do
      activeBeforeAndAfter <- runIO do
        subscriptionState <- initSubscriptionsState (const (pure ()))
        promMetrics <- makeDummyPrometheusMetrics
        wsId <- WS.mkUnsafeWSId <$> UUID.nextRandom
        cohortId <- newCohortId
        let quietLogger :: Logger Hasura = Logger $ \_ -> pure ()
            options = mkSubscriptionsOptions Nothing Nothing
            operationName = Just $ OperationName [G.name|LiveOp|]
            liveQuery =
              [PG.sql|
                SELECT "_subs"."result_id", json_build_object('numbers', (SELECT coalesce(json_agg("id" ORDER BY "id"), '[]') FROM "public"."numbers"))::text AS "result"
                FROM UNNEST(($1)::uuid[], ($2)::json[]) AS "_subs"("result_id", "result_vars")
              |]
            plan =
              SubscriptionQueryPlan
                (ParameterizedSubscriptionQueryPlan (mkRoleNameE "user") $ MultiplexedQuery liveQuery)
                srcConfig
                cohortId
                Nothing
                (mkCohortVariables mempty mempty mempty mempty mempty)
                Nothing
            activeSubscriptions = sum . Map.elems <$> GaugeVector.read (submActiveSubscriptions $ pmSubscriptionMetrics promMetrics)
        details <-
          addLiveQuery
            @('Postgres 'Vanilla)
            quietLogger
            dummyServerMetrics
            promMetrics
            (mkSubscriberMetadata wsId (unsafeMkOperationId "l1") Nothing (RequestId "l1"))
            subscriptionState
            (pure (options, options))
            SNDefault
            paramQueryHash
            operationName
            (RequestId "l1")
            plan
            (pure GranularMetricsOff)
            (const (pure ()))
            Nothing
            []
            (pure ModelInfoLogOff)
        poller <- maybe (error "no poller") pure . listToMaybe . fmap snd =<< STM.atomically (ListT.toList (STMMap.listT (_ssLiveQueryMap subscriptionState)))
        -- Leave the poller without its thread, the state removeLiveQuery logs as a bug.
        ioState <- STM.atomically (STM.takeTMVar (_pIOState poller))
        Immortal.stop (_pThread ioState)
        before <- activeSubscriptions
        removeLiveQuery quietLogger dummyServerMetrics promMetrics subscriptionState details (pure GranularMetricsOff) operationName
        (,) before <$> activeSubscriptions

      it "counts the active subscriptions down" $ do
        activeBeforeAndAfter `shouldBe` (1, 0)

    describe "Database execution time of a subscription query while the pool is exhausted" $ do
      cohortId <- runIO newCohortId
      let -- Holds one of the pool's connections for 1.5 s.
          holdConnection =
            runPgSourceWriteTx srcConfig InternalRawQuery (PG.unitQE defaultTxErrorHandler [PG.sql| DO $$ BEGIN PERFORM pg_sleep(1.5); END $$ |] () False)
              >>= (`onLeft` (printErrExit . showQErr))
          runSubscriptionQuery =
            runDBStreamingSubscription @('Postgres 'Vanilla) srcConfig (MultiplexedQuery multiplexedQuery) [(cohortId, cohortKey1)] Nothing
      (wallTime, (reportedTime, queryResult)) <-
        runIO
          $ Async.withAsync (Async.replicateConcurrently_ testPoolSize holdConnection)
          $ \holders -> do
            -- Let the holders take every connection of the pool first.
            threadDelay 300000
            measured <- withElapsedTime runSubscriptionQuery
            Async.wait holders
            pure measured

      it "the query waited for a pool connection" $ do
        either (const False) (const True) queryResult `shouldBe` True
        wallTime `shouldSatisfy` (> 1)

      it "the reported database execution time leaves the wait out" $ do
        reportedTime `shouldSatisfy` maybe False (< 0.5)

    describe "A poll batch that gets no pool connection" $ do
      -- One streaming and one live query poll, run while the one connection of a pool that
      -- gives up waiting after 1 s is held: their batch runs no query. The subscriptions are
      -- added with other metrics than the polls run here, so that the first poll of the
      -- pollers' own threads, before they are stopped, is not counted.
      (failedBatches, dbTimeCounts, pollCounts) <- runIO do
        let timeoutSrcConfig = srcConfig {_pscExecCtx = mkPGExecCtx PG.ReadCommitted timeoutPool NeverResizePool}
        subscriptionState <- initSubscriptionsState (const (pure ()))
        promMetrics <- makeDummyPrometheusMetrics
        wsId <- WS.mkUnsafeWSId <$> UUID.nextRandom
        (streamCohortId, liveCohortId) <- (,) <$> newCohortId <*> newCohortId
        polls <- newIORef ([] :: [PollDetails])
        let quietLogger :: Logger Hasura = Logger $ \_ -> pure ()
            options = mkSubscriptionsOptions Nothing Nothing
            liveQuery =
              [PG.sql|
                SELECT "_subs"."result_id", json_build_object('numbers', (SELECT coalesce(json_agg("id" ORDER BY "id"), '[]') FROM "public"."numbers"))::text AS "result"
                FROM UNNEST(($1)::uuid[], ($2)::json[]) AS "_subs"("result_id", "result_vars")
              |]
            plan query cohortId cohortKey =
              SubscriptionQueryPlan (ParameterizedSubscriptionQueryPlan (mkRoleNameE "user") $ MultiplexedQuery query) srcConfig cohortId Nothing cohortKey Nothing
            subscriber name = mkSubscriberMetadata wsId (unsafeMkOperationId name) Nothing (RequestId name)
            keepPoll details = atomicModifyIORef' polls (\ds -> (ds <> [details], ()))
            stopThread poller = STM.atomically (STM.readTMVar (_pIOState poller)) >>= Immortal.stop . _pThread
            -- Takes the pool's connection, says so on "held", and keeps it for 3 s.
            holdConnection held =
              runPgSourceWriteTx timeoutSrcConfig InternalRawQuery (liftIO (void (MVar.tryPutMVar held ())) >> PG.unitQE defaultTxErrorHandler [PG.sql| DO $$ BEGIN PERFORM pg_sleep(3); END $$ |] () False)
                >>= (`onLeft` (printErrExit . showQErr))
        _ <-
          addStreamSubscriptionQuery @('Postgres 'Vanilla) quietLogger dummyServerMetrics dummyPromMetrics (subscriber "s1") subscriptionState (pure (options, options)) SNDefault paramQueryHash Nothing (RequestId "s1") [G.name|numbers_stream|] (plan multiplexedQuery streamCohortId cohortKey1) (pure GranularMetricsOff) (const (pure ())) Nothing [] (pure ModelInfoLogOff)
        _ <-
          addLiveQuery @('Postgres 'Vanilla) quietLogger dummyServerMetrics dummyPromMetrics (subscriber "l1") subscriptionState (pure (options, options)) SNDefault paramQueryHash Nothing (RequestId "l1") (plan liveQuery liveCohortId (mkCohortVariables mempty mempty mempty mempty mempty)) (pure GranularMetricsOff) (const (pure ())) Nothing [] (pure ModelInfoLogOff)
        streamPoller <- maybe (error "no streaming poller") pure . listToMaybe . fmap snd =<< STM.atomically (ListT.toList (STMMap.listT (_ssStreamQueryMap subscriptionState)))
        livePoller <- maybe (error "no live query poller") pure . listToMaybe . fmap snd =<< STM.atomically (ListT.toList (STMMap.listT (_ssLiveQueryMap subscriptionState)))
        stopThread streamPoller
        stopThread livePoller
        held <- MVar.newEmptyMVar
        Async.withAsync (holdConnection held) \holder -> do
          MVar.takeMVar held
          pollStreamingQuery @('Postgres 'Vanilla) pollerId (_pPollerState streamPoller) options (SNDefault, timeoutSrcConfig) (mkRoleNameE "user") paramQueryHash (MultiplexedQuery multiplexedQuery) (_pCohorts streamPoller) [G.name|numbers_stream|] keepPoll Nothing promMetrics (pure GranularMetricsOff) (_pOperationNamesMap streamPoller) Nothing Nothing quietLogger [] (pure ModelInfoLogOff)
          pollLiveQuery @('Postgres 'Vanilla) pollerId (_pPollerState livePoller) options (SNDefault, timeoutSrcConfig) (mkRoleNameE "user") paramQueryHash (MultiplexedQuery liveQuery) (_pCohorts livePoller) keepPoll promMetrics (pure GranularMetricsOff) (_pOperationNamesMap livePoller) Nothing Nothing quietLogger [] (pure ModelInfoLogOff)
          Async.wait holder
        PG.destroyPGPool timeoutPool
        let SubscriptionMetrics {..} = pmSubscriptionMetrics promMetrics
        (,,)
          <$> (map (maybe 0 length . _pdErrors) <$> readIORef polls)
          <*> (seriesCounts <$> HistogramVector.read submDBExecTotalTime)
          <*> (seriesCounts <$> HistogramVector.read submTotalTime)

      it "each poll has one failed batch" $ do
        failedBatches `shouldBe` [1, 1]

      it "the batch is not observed in the database execution time" $ do
        dbTimeCounts `shouldBe` []

      it "the poll is still observed in the poll time" $ do
        pollCounts `shouldBe` labeledCounts [(SubscriptionLabel streamingSubscriptionLabel Nothing, 1), (SubscriptionLabel liveQuerySubscriptionLabel Nothing, 1)]

    describe "Database execution time of a query that does not end with a result" $ do
      -- runQueryTx turns a lost connection, an exception, into an error after the query, so
      -- the exception is caught here the same way, outside the timed query.
      let timeFailingQuery :: ExceptT QErr IO () -> IO (Maybe DiffTime, Either ConnectionLost (Either QErr ()))
          timeFailingQuery failingQuery = withQueryExecutionTime \timeQuery -> Exception.try $ runExceptT $ timeQuery failingQuery
      (timeOfStatementError, statementError) <- runIO $ timeFailingQuery (liftIO (threadDelay 200000) >> throwError (err500 Unexpected "statement failed"))
      (timeOfException, exception) <- runIO $ timeFailingQuery (liftIO (threadDelay 200000 >> Exception.throwIO ConnectionLost))

      it "a statement error is returned, with the time until it" $ do
        either (const False) (either (const True) (const False)) statementError `shouldBe` True
        timeOfStatementError `shouldSatisfy` maybe False (>= 0.2)

      it "an exception is rethrown unchanged, with the time until it" $ do
        either (const True) (const False) exception `shouldBe` True
        timeOfException `shouldSatisfy` maybe False (>= 0.2)

  runIO teardown

-- | Stands for the exception a query throws when its connection is lost.
data ConnectionLost = ConnectionLost
  deriving stock (Show)

instance Exception.Exception ConnectionLost

-- | The observation count of each series of a histogram vector, keyed by the
-- series' label names and values (the label types have no 'Show' instance).
seriesCounts :: (Prometheus.ToLabels l) => Map.Map l HistogramSample -> [([(Text, Text)], Int64)]
seriesCounts = labeledCounts . map (fmap histCount) . Map.toList

labeledCounts :: (Prometheus.ToLabels l) => [(l, Int64)] -> [([(Text, Text)], Int64)]
labeledCounts = sort . map (first (sort . HashMap.toList . Prometheus.toLabels))

-- | Connections in the pool this suite builds ('PG.defaultConnParams').
testPoolSize :: Int
testPoolSize = PG.cpConns PG.defaultConnParams

printErrExit :: Text -> IO a
printErrExit = (*> exitFailure) . T.putStrLn
