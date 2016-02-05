module DynamicTaskSortLocally where

import Data.ByteString.Lazy.Char8 as BLC
import Data.List (sort)

import Control.Distributed.Task.Types.TaskTypes

task :: Task
task = sort
