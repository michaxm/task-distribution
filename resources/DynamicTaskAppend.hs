module DynamicTaskAppend where

import TaskSpawning.TaskTypes

task :: TaskInput -> TaskResult
task = map (++" appended")
