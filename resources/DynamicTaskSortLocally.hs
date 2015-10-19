module DynamicTaskSortLocally where

import Data.List (sort)

import TaskSpawning.TaskTypes

task :: TaskInput -> TaskResult
task = sort
