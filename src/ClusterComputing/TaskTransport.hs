{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, OverlappingInstances #-} --overlapping due to serialization
module ClusterComputing.TaskTransport where

import Control.Distributed.Process (ProcessId, NodeId)
import Control.Distributed.Process.Serializable (Serializable)
import Data.Binary (Binary)
import Data.Typeable (Typeable)
import GHC.Generics (Generic)

import TaskSpawning.TaskTypes

data TaskTransport = TaskTransport {
  _masterProcess :: ProcessId,
  _taskMetaData :: TaskMetaData,
  _taskDef :: TaskDef,
  _dataDef :: DataDef,
  _resultDef :: ResultDef
  } deriving (Typeable, Generic)
instance Binary TaskTransport
instance Serializable TaskTransport

data TaskMetaData = TaskMetaData {
  _taskName :: String,
  _workerNodeId :: NodeId
  } deriving (Typeable, Generic)
instance Binary TaskMetaData
instance Serializable TaskMetaData

