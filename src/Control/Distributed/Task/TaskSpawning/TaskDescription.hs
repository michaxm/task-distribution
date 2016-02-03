module Control.Distributed.Task.TaskSpawning.TaskDescription where

import Control.Distributed.Task.TaskSpawning.TaskDefinition

class Describable a where
  describe :: a -> String
instance Describable TaskDef where
  describe (SourceCodeModule n _) = n
  describe (DeployFullBinary _) = "user function defined in main"
  describe (PreparedDeployFullBinary _) = "user function defined in main (prepared)"
  describe (UnevaluatedThunk _ _) = "unevaluated user function"
  describe (ObjectCodeModule _) = "object code module"
instance Describable DataDef where
  describe (HdfsData p) = p
  describe (PseudoDB p) = p
