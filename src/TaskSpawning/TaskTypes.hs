{-# LANGUAGE DeriveDataTypeable, DeriveGeneric #-}
module TaskSpawning.TaskTypes where
-- TODO clean up module top level hierarchy, this is common, maybe put TaskDef/DataSpec in separate module

import Control.Distributed.Process.Serializable (Serializable)
import Data.Binary (Binary)
import Data.Typeable (Typeable)
import GHC.Generics (Generic)

type TaskInput = [String]
type TaskResult = [String]

data TaskDef = SourceCodeModule {
  _moduleName :: String,
  _moduleContent :: String
  } deriving (Typeable, Generic)
instance Binary TaskDef
instance Serializable TaskDef

type HdfsConfig = (String, Int)
data DataSpec =
  HdfsData {
    _config :: HdfsConfig,
    _filePath :: String
    }
  | PseudoDB {
    _numDB :: Int
    } deriving (Typeable, Generic)
instance Binary DataSpec
instance Serializable DataSpec
