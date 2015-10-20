{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, OverlappingInstances #-}
module ClusterComputing.TaskTransport where

import Control.Distributed.Process (ProcessId)
import Control.Distributed.Process.Serializable (Serializable)
import Data.Binary (Binary)
import Data.Typeable (Typeable)
import GHC.Generics (Generic)

data TaskTransport = TaskTransport {
  _masterProcess :: ProcessId,
  _taskName :: String,
  _taskDef :: TaskDef,
  _dataSpec :: DataSpec
  } deriving (Typeable, Generic)
instance Binary TaskTransport
instance Serializable TaskTransport

data TaskDef = SourceCodeModule {
  _moduleText :: String
  } deriving (Typeable, Generic)
instance Binary TaskDef
instance Serializable TaskDef

data DataSpec =
  HdfsData {
    _filePath :: String
    }
  | PseudoDB {
    _numDB :: Int
    } deriving (Typeable, Generic)
instance Binary DataSpec
instance Serializable DataSpec
