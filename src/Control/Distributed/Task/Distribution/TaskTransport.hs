{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, OverlappingInstances #-} --overlapping due to serialization
module Control.Distributed.Task.Distribution.TaskTransport where

import Control.Distributed.Process (ProcessId, NodeId)
import Control.Distributed.Process.Serializable (Serializable)
import Data.Binary (Binary)
import Data.ByteString.Lazy (ByteString)
import Data.Time.Calendar (Day(..))
import Data.Time.Clock (UTCTime(..), NominalDiffTime)
import Data.Typeable (Typeable)
import GHC.Generics (Generic)

import Control.Distributed.Task.TaskSpawning.TaskDefinition

data TaskTransport = TaskTransport {
  _masterProcess :: ProcessId,
  _taskMetaData :: TaskMetaData,
  _taskDef :: TaskDef,
  _dataDef :: DataDef,
  _resultDef :: ResultDef
  } deriving (Typeable, Generic)
instance Binary TaskTransport
instance Serializable TaskTransport

type TimeStamp = (Integer, Rational)

data TaskMetaData = TaskMetaData {
  _taskName :: String,
  _slaveNodeId :: NodeId,
  _taskDistributionStartTime :: TimeStamp
  } deriving (Typeable, Generic)
instance Binary TaskMetaData
instance Serializable TaskMetaData

data RemoteRunStat = RemoteRunStat {
--  _remoteAcceptTime :: TimeStamp,
--  _remoteProcessingDoneTime :: TimeStamp,
  _remoteTotalDuration :: Rational,
  _remoteDataLoadDuration :: Rational,
  _remoteTaskLoadDuration :: Rational,
  _remoteExecTaskDuration :: Rational
  } deriving (Typeable, Generic)
instance Binary RemoteRunStat
instance Serializable RemoteRunStat

serializeTime :: UTCTime -> TimeStamp
serializeTime (UTCTime (ModifiedJulianDay d) f) = (d, toRational f)

deserializeTime :: TimeStamp -> UTCTime
deserializeTime (d, f) = UTCTime (ModifiedJulianDay d) (fromRational f)

serializeTimeDiff :: NominalDiffTime -> Rational
serializeTimeDiff = toRational

deserializeTimeDiff :: Rational -> NominalDiffTime
deserializeTimeDiff = fromRational

type QuerySlavePreparationRequest = (ProcessId, Int)
data QuerySlavePreparationResponse = Prepared | Unprepared deriving (Typeable, Generic)
instance Binary QuerySlavePreparationResponse
instance Serializable QuerySlavePreparationResponse

type PrepareSlaveRequest = (ProcessId, Int, ByteString)
data PrepareSlaveResponse = PreparationFinished deriving (Typeable, Generic)
instance Binary PrepareSlaveResponse
instance Serializable PrepareSlaveResponse
