module Control.Distributed.Task.Types.TaskTypes where

import qualified Data.ByteString.Lazy as BLC

-- | This is a small common ground for some assumptions: input is a list of sth., deserialization is the responsibility of the task itself.
type TaskInput = [BLC.ByteString]
type TaskResult = TaskInput
type Task = TaskInput -> TaskResult
