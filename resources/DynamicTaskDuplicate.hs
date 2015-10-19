module DynamicTaskDuplicate where

import TaskSpawning.TaskTypes

task :: TaskInput -> TaskResult
task = map (\s -> s ++ " - " ++ s)
