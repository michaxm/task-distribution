module Control.Distributed.Task.TaskSpawning.TaskDescription where

import Control.Distributed.Task.TaskSpawning.TaskDefinition

class Describable a where
  describe :: a -> String
instance Describable TaskDef where
  describe (SourceCodeModule n _) = n
  describe (DeployFullBinary _ _) = "user function defined in main"
  describe (PreparedDeployFullBinary _ _) = "user function defined in main (prepared)"
  describe (UnevaluatedThunk _ _) = "unevaluated user function"
  describe (ObjectCodeModule _) = "object code module"
instance Describable DataDef where
  describe (HdfsData l) = l
  describe (PseudoDB n) = show n
