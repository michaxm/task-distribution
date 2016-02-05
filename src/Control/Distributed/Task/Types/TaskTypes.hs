module Control.Distributed.Task.Types.TaskTypes where

import qualified Data.ByteString.Lazy as BLC

type TaskInput = [BLC.ByteString]
type TaskResult = [BLC.ByteString]
type Task = TaskInput -> TaskResult
