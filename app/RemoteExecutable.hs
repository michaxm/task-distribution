module RemoteExecutable (remoteExecutable) where

import qualified Data.ByteString.Lazy.Char8 as BLC

import Control.Distributed.Task.Types.TaskTypes

remoteExecutable :: Task
remoteExecutable = map $ flip BLC.append $ BLC.pack " appended"
