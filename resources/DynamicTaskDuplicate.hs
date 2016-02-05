module DynamicTaskDuplicate where

import Data.ByteString.Lazy.Char8 as BLC

import Control.Distributed.Task.Types.TaskTypes

task :: Task
task = Prelude.map (\s -> s `BLC.append` (BLC.pack " - ") `BLC.append` s)
