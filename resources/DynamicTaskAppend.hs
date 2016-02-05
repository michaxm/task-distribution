module DynamicTaskAppend where

import Data.ByteString.Lazy.Char8 as BLC

import Control.Distributed.Task.Types.TaskTypes

task :: Task
task = Prelude.map (Prelude.flip BLC.append (BLC.pack " appended"))
