-- Modified by the BigBlueButton project (https://github.com/bigbluebutton/hasura-graphql-engine):
-- tests the version of a test build (scripts/build-v2.sh --dev-version). See BBB-CHANGES.md.
module Hasura.Server.VersionSpec (spec) where

import Data.Text qualified as T
import Data.Text.Conversions (FromText (..), ToText (..))
import Hasura.Prelude
import Hasura.Server.Version
import Test.Hspec

versions :: [(Text, Text)]
versions =
  [ ("v1.2.4", "channel/stable/v1.2"),
    ("2.3.4", "channel/stable/v2.3"),
    ("v1.0.0-alpha45", "channel/alpha/v1.0"),
    ("v1.0.0-beta.01", "channel/beta/v1.0"),
    ("v2.3.1-rc.11", "channel/rc/v2.3"),
    ("v1.2.0-rj.1", "channel/rj/v1.2"),
    -- Apparently this one was generated in a different way?
    -- ("v2.0.0-pro.9", "channel/stable/v2.0"),
    ("v2.0.0-alpha.pro.9", "channel/alpha/v2.0"),
    ("v2.12.0-ce", "channel/versioned/v2.12.0-ce"),
    ("v2.12.1-ce", "channel/versioned/v2.12.1-ce"),
    ("v2.12.0-beta.1-ce", "channel/versioned/v2.12.0-beta.1-ce"),
    ("v2.12.0-beta.2-ce", "channel/versioned/v2.12.0-beta.2-ce"),
    ("v2.12.0", "channel/stable/v2.12"),
    ("v2.12.1", "channel/stable/v2.12"),
    ("v2.12.0-beta.1", "channel/beta/v2.12"),
    ("v2.12.0-beta.2", "channel/beta/v2.12"),
    ("v2.12.0-alpha.1", "channel/alpha/v2.12"),
    ("v2.12.0-alpha.2", "channel/alpha/v2.12")
    -- The Haskell server does not seem to be responsible for generating version strings for Cloud
    -- ("v2.12.0-cloud.1", "channel/versioned/v2.12.0-cloud.1")
    -- ("v2.0.0-cloud.9", "channel/stable/v2.0"),
    -- ("v2.0.0-alpha.cloud.9", "channel/alpha/v2.0"),
  ]

spec :: Spec
spec = do
  testBuildVersionSpec
  consoleAssetsVersionSpec

-- | A test build reports its base version followed by "-dev.g", the first 8 hex digits of the
-- commit it was built from and, when the tree had uncommitted changes, ".dirty".
testBuildVersionSpec :: Spec
testBuildVersionSpec = describe "test build version" do
  for_ ["v2.50.7-bbb2-dev.g94082f14", "v2.50.7-bbb2-dev.g01234567", "v2.50.7-bbb2-dev.g01234567.dirty"] \devVersion ->
    it ("is a release version that reads back unchanged: " <> T.unpack devVersion) do
      case fromText devVersion of
        VersionRelease _ -> pure ()
        other -> expectationFailure ("not read as a release version: " <> show other)
      toText (fromText devVersion :: Version) `shouldBe` devVersion
  it "needs the g: 8 decimal digits with a leading zero do not read back unchanged" do
    let undecorated = "v2.50.7-bbb2-dev.01234567" :: Text
    toText (fromText undecorated :: Version) `shouldNotBe` undecorated
  it "this binary's version, when it is a test build, ends in -dev.g, 8 hex digits and maybe .dirty" do
    let version = toText currentVersion
        (_, devSuffix) = T.breakOn "-dev." version
        isStamp suffix =
          let (hash, rest) = T.splitAt 8 (T.drop 6 suffix)
           in T.take 6 suffix
                == "-dev.g"
                && T.length hash
                == 8
                && T.all (`elem` ("0123456789abcdef" :: String)) hash
                && (rest == "" || rest == ".dirty")
    unless (T.null devSuffix) $ devSuffix `shouldSatisfy` isStamp

consoleAssetsVersionSpec :: Spec
consoleAssetsVersionSpec = describe "console assets version" do
  parallel
    $ for_ versions \(input, output) ->
      it ("versionToAssetsVersion returns expected output for " <> T.unpack input)
        $ versionToAssetsVersion (fromText input)
        `shouldBe` output
