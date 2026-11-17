module Hasura.GraphQL.Execute.Subscription.Poll.LiveQuerySpec (spec) where

import Control.Concurrent.STM qualified as STM
import Data.ByteString qualified as BS
import Data.Map.Strict qualified as Map
import Data.UUID.V4 qualified as UUID
import Hasura.GraphQL.Execute.Subscription.Plan (mkCohortVariables, mkUnsafeValidateVariables)
import Hasura.GraphQL.Execute.Subscription.Poll.Common hiding (Cohort (..), CohortMap, CohortSnapshot (..))
import Hasura.GraphQL.Execute.Subscription.Poll.Common qualified as C
import Hasura.GraphQL.Execute.Subscription.Poll.LiveQuery (pushResultToCohort)
import Hasura.GraphQL.Transport.HTTP.Protocol (GQExecError (..), OperationName (..))
import Hasura.GraphQL.Transport.WebSocket.Protocol (unsafeMkOperationId)
import Hasura.GraphQL.Transport.WebSocket.Server qualified as WS
import Hasura.Prelude
import Hasura.Server.Prometheus
import Hasura.Server.Types (RequestId (..))
import Language.GraphQL.Draft.Syntax qualified as G
import System.Metrics.Prometheus.CounterVector qualified as CounterVector
import Test.Hspec

spec :: Spec
spec = describe "Results pushed to the subscribers of a live query cohort, by operation" do
  let payload = "{\"users\" : [1, 2, 3]}" :: BS.ByteString
      firstOp = Just (OperationName (G.unsafeMkName "First"))
      secondOp = Just (OperationName (G.unsafeMkName "Second"))
      -- A cohort with two existing subscribers (First and Second) and one new subscriber of
      -- First, whose previous response is the given one. Returns the counts and bytes by
      -- operation and changed, and what each subscriber received.
      push result previousResponse = do
        metrics <- makeDummyPrometheusMetrics
        received <- STM.newTVarIO ([] :: [Text])
        let subscriber name opName = do
              subscriberId <- newSubscriberId
              wsId <- WS.mkUnsafeWSId <$> UUID.nextRandom
              pure $ Subscriber subscriberId (mkSubscriberMetadata wsId (unsafeMkOperationId name) opName (RequestId name)) (RequestId name) opName (const (STM.atomically (STM.modifyTVar' received (name :))))
        existing <- sequence [subscriber "a" firstOp, subscriber "b" secondOp]
        new <- sequence [subscriber "c" firstOp]
        responseRef <- STM.newTVarIO previousResponse
        let snapshot = C.CohortSnapshot (mkCohortVariables mempty mempty (mkUnsafeValidateVariables mempty) (mkUnsafeValidateVariables mempty) (mkUnsafeValidateVariables mempty)) responseRef existing new
        _ <- pushResultToCohort metrics result (Just (mkRespHash payload)) (SubscriptionMetadata 0) snapshot
        let SubscriptionMetrics {..} = pmSubscriptionMetrics metrics
            series vector = do
              values <- CounterVector.read vector
              pure
                $ sort
                  [ (name, changed, value)
                  | (SubscriptionChangedLabel (SubscriptionLabel _ (Just (DynamicGraphqlOperationLabel (OperationNameLabel name)))) changed, value) <- Map.toList values
                  ]
        (,,) <$> series submSubscribersServed <*> series submSentBytes <*> (sort <$> STM.readTVarIO received)
      size = fromIntegral (BS.length payload)

  it "a changed result counts every subscriber under its own operation, with changed=true" do
    (served, bytes, received) <- push (Right payload) Nothing
    received `shouldBe` ["a", "b", "c"]
    served `shouldBe` [("First", True, 2), ("Second", True, 1)]
    bytes `shouldBe` [("First", True, 2 * size), ("Second", True, size)]

  it "an unchanged result counts only the new subscribers it is pushed to, with changed=false" do
    (served, bytes, received) <- push (Right payload) (Just (mkRespHash payload))
    received `shouldBe` ["c"]
    served `shouldBe` [("First", False, 1)]
    bytes `shouldBe` [("First", False, size)]

  it "an error result goes to every subscriber, counted as changed and with 0 bytes" do
    -- An error is pushed even when the hash is the previous one.
    (served, bytes, received) <- push (Left (GQExecError [])) (Just (mkRespHash payload))
    received `shouldBe` ["a", "b", "c"]
    served `shouldBe` [("First", True, 2), ("Second", True, 1)]
    bytes `shouldBe` [("First", True, 0), ("Second", True, 0)]
