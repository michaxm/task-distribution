{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, OverlappingInstances #-} --overlapping due to serialization
module ClusterComputing.TaskTransport where

import Control.Distributed.Process (ProcessId)
import Control.Distributed.Process.Serializable (Serializable)
import Data.Binary (Binary)
import Data.Typeable (Typeable)
import GHC.Generics (Generic)

import TaskSpawning.TaskTypes

data TaskTransport = TaskTransport {
  _masterProcess :: ProcessId,
  _taskName :: String,
  _taskDef :: TaskDef,
  _dataSpec :: DataDef
  } deriving (Typeable, Generic)
instance Binary TaskTransport
instance Serializable TaskTransport
