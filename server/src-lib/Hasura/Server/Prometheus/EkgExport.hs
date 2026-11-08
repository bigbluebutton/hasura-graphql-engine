-- Added by the BigBlueButton project (https://github.com/bigbluebutton/hasura-graphql-engine):
-- conversion of the EKG metric sample to the Prometheus text format, used by
-- the GET /v1/metrics endpoint. See BBB-CHANGES.md.

-- | Convert EKG metrics to the Prometheus text exposition format.
-- See: https://prometheus.io/docs/instrumenting/exposition_formats/
module Hasura.Server.Prometheus.EkgExport
  ( ekgToPrometheus,
  )
where

import Data.HashMap.Strict qualified as HashMap
import Data.Text qualified as T
import Hasura.Prelude
import System.Metrics qualified as EKG
import System.Metrics.Distribution qualified as Dist

-- | Render an EKG sample in the Prometheus text format.
--
-- Metric names and label keys have their dots replaced by underscores to
-- satisfy the Prometheus name grammar. The @rts.gc.@ subtree
-- (statistics of the most recent GC, GHC's GCDetails struct) is renamed to
-- @rts_gcdetails_@ first: a plain dot replacement would collide with the
-- cumulative @rts.gc_*@ metrics (for example, @rts.gc.elapsed_ns@ and
-- @rts.gc_elapsed_ns@ would both become @rts_gc_elapsed_ns@, an invalid
-- duplicate family).
--
-- EKG counters are exported as untyped time series rather than counters:
-- their names (e.g. @rts_gcs@) do not follow the Prometheus @_total@ counter
-- naming convention, and renaming them would break consumers of the existing
-- EKG names. Each series carries @# HELP@ and @# TYPE@ lines. A distribution
-- is exported as a quantile-less summary (@_sum@ and @_count@) plus gauges
-- with the @_mean@, @_variance@, @_min@ and @_max@ suffixes.
ekgToPrometheus :: EKG.Sample -> Text
ekgToPrometheus sample =
  T.concat $ mapMaybe formatMetric $ HashMap.toList sample
  where
    formatMetric :: (EKG.Identifier, EKG.Value) -> Maybe Text
    formatMetric (EKG.Identifier name tags, value) =
      let sanitizedName = T.replace "." "_" (T.replace "rts.gc." "rts.gcdetails." name)
          -- Format labels as key="value" pairs
          labelPairs = HashMap.toList tags
          labelsText =
            if null labelPairs
              then ""
              else
                "{"
                  <> T.intercalate "," (map (\(k, v) -> T.replace "." "_" k <> "=\"" <> escapeLabel v <> "\"") labelPairs)
                  <> "}"
          serie typ suffix val =
            T.unlines
              [ "# HELP " <> sanitizedName <> suffix <> " Metric exported from the EKG store",
                "# TYPE " <> sanitizedName <> suffix <> " " <> typ,
                sanitizedName <> suffix <> labelsText <> " " <> val
              ]
       in case value of
            EKG.Counter n -> Just $ serie "untyped" "" (tshow n)
            EKG.Gauge n -> Just $ serie "gauge" "" (tshow n)
            EKG.Label _ -> Nothing -- Labels are metadata, not metrics in Prometheus
            EKG.Distribution stats ->
              -- Export a distribution as a quantile-less Prometheus summary
              -- (the _sum and _count series), plus separate gauges for the
              -- statistics a summary cannot carry
              Just
                $ T.unlines
                  [ "# HELP " <> sanitizedName <> " Metric exported from the EKG store",
                    "# TYPE " <> sanitizedName <> " summary",
                    sanitizedName <> "_sum" <> labelsText <> " " <> tshow (Dist.sum stats),
                    sanitizedName <> "_count" <> labelsText <> " " <> tshow (Dist.count stats)
                  ]
                <> T.concat
                  [ serie "gauge" "_mean" (tshow (Dist.mean stats)),
                    serie "gauge" "_variance" (tshow (Dist.variance stats)),
                    serie "gauge" "_min" (tshow (Dist.min stats)),
                    serie "gauge" "_max" (tshow (Dist.max stats))
                  ]

    -- Escape special characters in label values according to Prometheus format
    escapeLabel :: Text -> Text
    escapeLabel = T.replace "\\" "\\\\" . T.replace "\"" "\\\"" . T.replace "\n" "\\n"
