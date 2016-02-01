{-# LANGUAGE DeriveDataTypeable, DeriveGeneric #-}
module Control.Distributed.Task.TaskSpawning.TaskDefinition where

import Control.Distributed.Process.Serializable (Serializable)
import Data.Binary (Binary)
import Data.ByteString.Lazy (ByteString)
import Data.Typeable (Typeable)
import GHC.Generics (Generic)

import Control.Distributed.Task.Types.HdfsConfigTypes

{-
  The way how code is to be distributed. The approaches have different disadvantages:

  - source code: very limited type checking, breaks integration of code in regular program
  - function serialization: special entry point in Main necessary
  - object code distribution: requires compilation intermediate result, cumbersome to add required libs/modules
-}
data TaskDef
 = SourceCodeModule {
   _moduleName :: String,
   _moduleContent :: String
   }
 | DeployFullBinary {
   _deployable :: ByteString,
   _taskInputMode :: TaskInputMode
   }
 | PreparedDeployFullBinary {
   _preparedFullBinaryHash :: Int,
   _taskInputMode :: TaskInputMode
   }
 | UnevaluatedThunk {
   _unevaluatedThunk :: ByteString,
   _deployable :: ByteString
   }
 | ObjectCodeModule {
   _objectCode :: ByteString
   } deriving (Typeable, Generic)
instance Binary TaskDef
instance Serializable TaskDef

-- how input is given to task as an external program
data TaskInputMode
  = FileInput
  | StreamInput
  deriving (Typeable, Generic)
instance Binary TaskInputMode
instance Serializable TaskInputMode

{-
 Where data comes from:

 - hdfs data source
 - very simple file format for testing purposes, files with numbers expected relative to work directory
-}
data DataDef
  = HdfsData {
    _hdfsInputPath :: HdfsPath
    }
  | PseudoDB {
    _numDB :: Int
    } deriving (Typeable, Generic)
instance Binary DataDef
instance Serializable DataDef

{-
 Where calculation results go:

 - simply respond the answer, aggregation happens on master application
 - only num results: for testing/benchmarking purposes
-}
data ResultDef
 = ReturnAsMessage
 | HdfsResult {
   _outputPrefix :: String,
   _outputSuffix :: String
   }
 | ReturnOnlyNumResults
 deriving (Typeable, Generic)
instance Binary ResultDef
instance Serializable ResultDef
