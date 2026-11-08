-- Added by the BigBlueButton project (https://github.com/bigbluebutton/hasura-graphql-engine):
-- tests for the EKG-to-Prometheus text conversion. See BBB-CHANGES.md.

-- | Tests for 'ekgToPrometheus': metric names must stay unique after
-- sanitization (the known hazard is the @rts.gc.*@ subtree colliding with the
-- cumulative @rts.gc_*@ metrics once dots become underscores), every exported
-- name must match the Prometheus name grammar, and distributions must expand
-- to a summary plus gauges.
module Hasura.Server.Prometheus.EkgExportSpec (spec) where

import Data.Char (isAlpha, isAlphaNum)
import Data.HashMap.Strict qualified as HashMap
import Data.Text qualified as T
import Hasura.Prelude
import Hasura.Server.Prometheus.EkgExport (ekgToPrometheus)
import System.Metrics qualified as EKG
import System.Metrics.Distribution.Internal qualified as Dist
import Test.Hspec

spec :: Spec
spec = describe "ekgToPrometheus" $ do
  let mkId name = EKG.Identifier name HashMap.empty
      distribution =
        Dist.Stats
          { Dist.mean = 1.5,
            Dist.variance = 0.25,
            Dist.count = 2,
            Dist.sum = 3.0,
            Dist.min = 1.0,
            Dist.max = 2.0
          }
      sample :: EKG.Sample
      sample =
        HashMap.fromList
          [ (mkId "rts.gc.elapsed_ns", EKG.Counter 1),
            (mkId "rts.gc_elapsed_ns", EKG.Counter 2),
            (mkId "rts.gcs", EKG.Counter 3),
            (mkId "ekg.server_timestamp_ms", EKG.Counter 4),
            (mkId "some.gauge", EKG.Gauge 5),
            (mkId "a.label", EKG.Label "ignored"),
            (mkId "events_fetched_per_batch", EKG.Distribution distribution)
          ]
      rendered = ekgToPrometheus sample
      renderedLines = T.lines rendered
      typedNames = mapMaybe (fmap (T.takeWhile (/= ' ')) . T.stripPrefix "# TYPE ") renderedLines
      isValidName name = case T.unpack name of
        [] -> False
        (c : cs) -> (isAlpha c || c == '_' || c == ':') && all (\x -> isAlphaNum x || x == '_' || x == ':') cs

  it "keeps metric families unique after sanitization" $ do
    let sorted = sort typedNames
    and (zipWith (/=) sorted (drop 1 sorted)) `shouldBe` True
    -- the two rts.gc variants must not merge into one family
    ("rts_gcdetails_elapsed_ns" `elem` typedNames) `shouldBe` True
    ("rts_gc_elapsed_ns" `elem` typedNames) `shouldBe` True

  it "emits only names that match the Prometheus name grammar" $ do
    typedNames `shouldSatisfy` all isValidName
    typedNames `shouldSatisfy` (not . any (T.isInfixOf "."))

  it "expands a distribution into a summary plus gauges" $ do
    ("# TYPE events_fetched_per_batch summary" `elem` renderedLines) `shouldBe` True
    ("events_fetched_per_batch_sum 3.0" `elem` renderedLines) `shouldBe` True
    ("events_fetched_per_batch_count 2" `elem` renderedLines) `shouldBe` True
    ("# TYPE events_fetched_per_batch_mean gauge" `elem` renderedLines) `shouldBe` True

  it "drops EKG labels and keeps counters and gauges" $ do
    (T.isInfixOf "a_label" rendered) `shouldBe` False
    ("rts_gcs 3" `elem` renderedLines) `shouldBe` True
    ("some_gauge 5" `elem` renderedLines) `shouldBe` True
