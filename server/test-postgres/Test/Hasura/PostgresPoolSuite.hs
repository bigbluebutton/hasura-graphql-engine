-- Added by the BigBlueButton project (https://github.com/bigbluebutton/hasura-graphql-engine):
-- tests the Postgres pool observer and the engine's pool metrics fed by it. See BBB-CHANGES.md.

-- | The observer a Postgres pool reports to, against a real database.
module Test.Hasura.PostgresPoolSuite (suite) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async qualified as Async
import Control.Concurrent.MVar qualified as MVar
import Data.Aeson qualified as J
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Map.Strict qualified as OrdMap
import Database.PG.Query qualified as PG
import Hasura.Prelude
import Hasura.Server.Prometheus (PostgresPoolLabel (..), PostgresPoolMetrics (..), PrometheusMetrics (..), makeDummyPrometheusMetrics, postgresPoolObserver)
import System.Metrics.Prometheus.GaugeVector qualified as GaugeVector
import Test.Hspec

-- | What a pool reported, kept by a recording observer.
data Recorded = Recorded
  { recWaits :: IORef Int,
    recWaiting :: IORef Int,
    recWaitTimeouts :: IORef Int,
    recInUse :: IORef Int,
    recMaxInUse :: IORef Int,
    recMaxConnections :: IORef [Int],
    recConnectionsOpened :: IORef Int,
    recErrors :: IORef Int,
    recClosed :: IORef [PG.PGConnectionCloseReason]
  }

newRecorded :: IO Recorded
newRecorded = Recorded <$> newIORef 0 <*> newIORef 0 <*> newIORef 0 <*> newIORef 0 <*> newIORef 0 <*> newIORef [] <*> newIORef 0 <*> newIORef 0 <*> newIORef []

recordingObserver :: Recorded -> PG.PGPoolObserver
recordingObserver Recorded {..} =
  PG.PGPoolObserver
    { PG.ppoWaitTime = const (bump recWaits),
      PG.ppoRequestWaiting = bump recWaiting,
      PG.ppoRequestServed = atomicModifyIORef' recWaiting (\n -> (n - 1, ())),
      PG.ppoWaitTimeout = bump recWaitTimeouts,
      PG.ppoConnectionTaken = do
        n <- atomicModifyIORef' recInUse (\n -> (n + 1, n + 1))
        atomicModifyIORef' recMaxInUse (\m -> (max m n, ())),
      PG.ppoConnectionReturned = atomicModifyIORef' recInUse (\n -> (n - 1, ())),
      PG.ppoMaxConnections = \n -> atomicModifyIORef' recMaxConnections (\ns -> (ns <> [n], ())),
      PG.ppoConnectionInitTime = const (bump recConnectionsOpened),
      PG.ppoConnectionError = bump recErrors,
      PG.ppoConnectionClosed = \reason -> atomicModifyIORef' recClosed (\rs -> (rs <> [reason], ()))
    }
  where
    bump ref = atomicModifyIORef' ref (\n -> (n + 1, ()))

-- | Whether the condition holds within 5 seconds, checked every 10 ms.
eventually :: IO Bool -> IO Bool
eventually condition = go (500 :: Int)
  where
    go attempts = do
      holds <- condition
      if holds || attempts <= 0 then pure holds else threadDelay 10000 >> go (attempts - 1)

suite :: PG.ConnInfo -> Spec
suite connInfo = describe "Postgres pool observer" $ do
  let newPool recorded connParams = PG.initPGPoolWithObserver (recordingObserver recorded) connInfo (J.String "test") connParams (\_ -> pure ())
      runQuery :: PG.PGPool -> Text -> IO (Either PG.PGExecErr ())
      runQuery pool query = runExceptT $ PG.runTx pool (PG.ReadCommitted, Nothing) do
        PG.Discard () <- PG.rawQE PG.PGExecErrTx (PG.fromText query) [] False
        pure ()
      -- Takes a connection from the pool, says so on "held" and keeps it until "release" is
      -- filled, so a test knows when the connection is taken instead of guessing with a sleep.
      holdConnection :: PG.PGPool -> MVar.MVar () -> MVar.MVar () -> IO (Either PG.PGExecErr ())
      holdConnection pool held release = runExceptT $ PG.runTx pool (PG.ReadCommitted, Nothing) do
        PG.Discard () <- PG.rawQE PG.PGExecErrTx (PG.fromText "SELECT 1") [] False
        liftIO $ MVar.putMVar held () >> MVar.readMVar release
      newHolder pool = do
        (held, release) <- (,) <$> MVar.newEmptyMVar <*> MVar.newEmptyMVar
        holder <- Async.async $ holdConnection pool held release
        MVar.takeMVar held
        pure (holder, MVar.putMVar release ())

  it "reports the maximum, the wait, the connection it opens and its return" $ do
    recorded <- newRecorded
    pool <- newPool recorded PG.defaultConnParams {PG.cpConns = 2}
    runQuery pool "SELECT 1" `shouldReturn` Right ()
    readIORef (recMaxConnections recorded) `shouldReturn` [2]
    readIORef (recWaits recorded) `shouldReturn` 1
    readIORef (recConnectionsOpened recorded) `shouldReturn` 1
    readIORef (recMaxInUse recorded) `shouldReturn` 1
    readIORef (recInUse recorded) `shouldReturn` 0
    readIORef (recWaiting recorded) `shouldReturn` 0
    PG.destroyPGPool pool

  it "reports the maximum across the stripes, since the limit is per stripe" $ do
    recorded <- newRecorded
    pool <- newPool recorded PG.defaultConnParams {PG.cpStripes = 2, PG.cpConns = 3}
    PG.resizePGPool pool 5
    readIORef (recMaxConnections recorded) `shouldReturn` [6, 10]
    PG.destroyPGPool pool

  it "counts a request waiting while the pool is exhausted" $ do
    recorded <- newRecorded
    pool <- newPool recorded PG.defaultConnParams {PG.cpConns = 1}
    (holder, release) <- newHolder pool
    waiter <- Async.async $ runQuery pool "SELECT 1"
    eventually ((== 1) <$> readIORef (recWaiting recorded)) `shouldReturn` True
    release
    Async.wait holder `shouldReturn` Right ()
    Async.wait waiter `shouldReturn` Right ()
    readIORef (recWaiting recorded) `shouldReturn` 0
    readIORef (recWaits recorded) `shouldReturn` 2
    PG.destroyPGPool pool

  it "a waiter cancelled while the pool is exhausted leaves nothing waiting or in use" $ do
    recorded <- newRecorded
    pool <- newPool recorded PG.defaultConnParams {PG.cpConns = 1}
    (holder, release) <- newHolder pool
    waiter <- Async.async $ runQuery pool "SELECT 1"
    eventually ((== 1) <$> readIORef (recWaiting recorded)) `shouldReturn` True
    Async.cancel waiter
    readIORef (recWaiting recorded) `shouldReturn` 0
    release
    Async.wait holder `shouldReturn` Right ()
    readIORef (recWaiting recorded) `shouldReturn` 0
    readIORef (recInUse recorded) `shouldReturn` 0
    PG.destroyPGPool pool

  it "a request that times out waiting for a connection leaves nothing waiting" $ do
    recorded <- newRecorded
    -- The pool rounds its timeout to whole seconds.
    pool <- newPool recorded PG.defaultConnParams {PG.cpConns = 1, PG.cpTimeout = Just 1}
    (holder, release) <- newHolder pool
    timedOut <- runQuery pool "SELECT 1"
    either (const True) (const False) timedOut `shouldBe` True
    readIORef (recWaiting recorded) `shouldReturn` 0
    readIORef (recWaitTimeouts recorded) `shouldReturn` 1
    release
    Async.wait holder `shouldReturn` Right ()
    readIORef (recInUse recorded) `shouldReturn` 0
    PG.destroyPGPool pool

  it "counts a connection closed past its lifetime as lifetime, and the rest as other" $ do
    recorded <- newRecorded
    pool <- newPool recorded PG.defaultConnParams {PG.cpConns = 1, PG.cpMbLifetime = Just 0.2}
    runQuery pool "SELECT 1" `shouldReturn` Right ()
    -- Well past the 0.2 s lifetime, so the second query finds the connection stale.
    threadDelay 700000
    runQuery pool "SELECT 1" `shouldReturn` Right ()
    readIORef (recClosed recorded) `shouldReturn` [PG.PGConnectionClosedLifetime]
    readIORef (recConnectionsOpened recorded) `shouldReturn` 2
    PG.destroyPGPool pool
    readIORef (recClosed recorded) `shouldReturn` [PG.PGConnectionClosedLifetime, PG.PGConnectionClosedOther]

  it "the engine's open connections gauge follows the connections the pool opens and closes" $ do
    metrics <- makeDummyPrometheusMetrics
    let label = PostgresPoolLabel "test"
        openConnections = OrdMap.findWithDefault 0 label <$> GaugeVector.read (ppmConnectionsOpen (pmPostgresPoolMetrics metrics))
    observer <- postgresPoolObserver metrics label
    pool <- PG.initPGPoolWithObserver observer connInfo (J.String "test") PG.defaultConnParams {PG.cpConns = 2} (\_ -> pure ())
    openConnections `shouldReturn` 0
    -- Two connections held at once make the pool open two.
    (first, releaseFirst) <- newHolder pool
    (second, releaseSecond) <- newHolder pool
    openConnections `shouldReturn` 2
    releaseFirst >> releaseSecond
    Async.wait first `shouldReturn` Right ()
    Async.wait second `shouldReturn` Right ()
    -- Idle connections stay open in the pool until it closes them.
    openConnections `shouldReturn` 2
    PG.destroyPGPool pool
    openConnections `shouldReturn` 0

  it "counts a connection that cannot be opened as an error" $ do
    recorded <- newRecorded
    let unreachable = PG.ConnInfo 0 (PG.CDDatabaseURI "postgres://nobody@127.0.0.1:1/nothing")
    pool <- PG.initPGPoolWithObserver (recordingObserver recorded) unreachable (J.String "test") PG.defaultConnParams (\_ -> pure ())
    result <- runQuery pool "SELECT 1"
    either (const True) (const False) result `shouldBe` True
    readIORef (recErrors recorded) `shouldReturn` 1
    readIORef (recWaiting recorded) `shouldReturn` 0
    readIORef (recInUse recorded) `shouldReturn` 0
