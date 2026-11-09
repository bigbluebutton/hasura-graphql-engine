-- Modified by the BigBlueButton project (https://github.com/bigbluebutton/hasura-graphql-engine):
-- the operation map entry carries a per-operation liveness flag used by the websocket
-- transport (https://github.com/hasura/graphql-engine/issues/10952). See BBB-CHANGES.md.
module Hasura.GraphQL.Transport.WebSocket.Types
  ( ErrRespType (ERTGraphqlCompliant, ERTLegacy),
    WSConn,
    WSConnData (WSConnData, _wscOpMap, _wscUser),
    WSConnState (CSInitError, CSInitialised, CSNotInitialised),
    WSServerEnv (..),
    WsClientState (WsClientState, wscsIpAddress, wscsReqHeaders, wscsTokenExpTime, wscsUserInfo),
    WsHeaders (WsHeaders, unWsHeaders),
    SubscriberType (..),
  )
where

import Control.Concurrent.STM qualified as STM
import Data.Time.Clock qualified as TC
import Hasura.Authentication.User (UserInfo)
import Hasura.GraphQL.Execute qualified as E
import Hasura.GraphQL.Execute.Subscription.State qualified as ES
import Hasura.GraphQL.Transport.HTTP.Protocol
import Hasura.GraphQL.Transport.Instances ()
import Hasura.GraphQL.Transport.WebSocket.Protocol
import Hasura.GraphQL.Transport.WebSocket.Server qualified as WS
import Hasura.Logging qualified as L
import Hasura.Prelude
import Hasura.Server.AppStateRef
import Hasura.Server.Cors
import Hasura.Server.Init.Config (KeepAliveDelay (..))
import Hasura.Server.Logging (LoggingSettings (..))
import Hasura.Server.Metrics (ServerMetrics (..))
import Hasura.Server.Prometheus (PrometheusMetrics (..))
import Hasura.Server.Types (ReadOnlyMode (..))
import Hasura.Tracing qualified as Tracing
import Network.HTTP.Client qualified as HTTP
import Network.HTTP.Types qualified as HTTP
import Network.Wai.Extended qualified as Wai
import StmContainers.Map qualified as STMMap

newtype WsHeaders = WsHeaders {unWsHeaders :: [HTTP.Header]}
  deriving (Show, Eq)

data ErrRespType
  = ERTLegacy
  | ERTGraphqlCompliant
  deriving (Show)

data WSConnState
  = -- | headers and IP address from the client for websockets
    CSNotInitialised !WsHeaders !Wai.IpAddress
  | CSInitError !Text
  | CSInitialised !WsClientState
  deriving (Show)

data WsClientState = WsClientState
  { -- | the 'UserInfo' required to execute the GraphQL query
    wscsUserInfo :: !UserInfo,
    -- | the JWT/token expiry time, if any
    wscsTokenExpTime :: !(Maybe TC.UTCTime),
    -- | headers from the client (in conn params) to forward to the remote schema
    wscsReqHeaders :: ![HTTP.Header],
    -- | IP address required for 'MonadGQLAuthorization'
    wscsIpAddress :: !Wai.IpAddress
  }
  deriving (Show)

data WSConnData = WSConnData
  -- the role and headers are set only on connection_init message
  { _wscUser :: !(STM.TVar WSConnState),
    -- we only care about subscriptions,
    -- the other operations (query/mutations)
    -- are not tracked here
    _wscOpMap :: !OperationMap,
    _wscErrRespTy :: !ErrRespType,
    _wscAPIType :: !E.GraphQLQueryType
  }

data WSServerEnv impl = WSServerEnv
  { _wseLogger :: !(L.Logger L.Hasura),
    _wseSubscriptionState :: !ES.SubscriptionsState,
    _wseAppStateRef :: AppStateRef impl,
    _wseHManager :: !HTTP.Manager,
    _wseCorsPolicy :: IO CorsPolicy,
    _wseReadOnlyMode :: ReadOnlyMode,
    _wseServer :: !WSServer,
    _wseKeepAliveDelay :: !KeepAliveDelay,
    _wseServerMetrics :: !ServerMetrics,
    _wsePrometheusMetrics :: !PrometheusMetrics,
    _wseTraceSamplingPolicy :: !Tracing.SamplingPolicy,
    _wseLoggingSettings :: !LoggingSettings
  }

data SubscriberType
  = LiveQuerySubscriber !ES.SubscriberDetails
  | StreamingQuerySubscriber !ES.SubscriberDetails

-- | The 'STM.TVar Bool' is the operation's liveness flag, created before the
-- subscriber is handed to the poller and cleared in 'claimAndStopSubscription'.
type OperationMap =
  STMMap.Map OperationId (SubscriberType, Maybe OperationName, STM.TVar Bool)

type WSServer = WS.WSServer WSConnData

type WSConn = WS.WSConn WSConnData
