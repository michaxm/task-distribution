{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, OverlappingInstances #-}
module ClusterComputing.TaskTransport (TaskTransport(..)) where

import Control.Distributed.Process (ProcessId)
import Control.Distributed.Process.Serializable (Serializable)
import Data.Binary (Binary)
import Data.Typeable (Typeable)
import GHC.Generics (Generic)

data TaskTransport = TaskTransport {
  _masterProcess :: ProcessId,
  _modulePath :: String,
  _numDB :: Int
  } deriving (Typeable, Generic)
instance Binary TaskTransport
instance Serializable TaskTransport
