module Types.TaskTypes where

type TaskInput = [String]
type TaskResult = [String]
type Task = TaskInput -> TaskResult
