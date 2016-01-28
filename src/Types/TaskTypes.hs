module Types.TaskTypes where

import Data.ByteString.Lazy (ByteString)

type TaskInput = [ByteString]
type TaskResult = [ByteString]
type Task = TaskInput -> TaskResult
