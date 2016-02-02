module Control.Distributed.Task.TaskSpawning.DeploySerializedThunk (deployAndRunSerializedThunk, serializedThunkExecution, acceptAndExecuteSerializedThunk) where

import qualified Data.ByteString.Lazy as BL
import System.FilePath ()

import Control.Distributed.Task.TaskSpawning.DeployFullBinary (deployAndRunExternalBinary, fullBinaryExecution)
import Control.Distributed.Task.TaskSpawning.FunctionSerialization (deserializeFunction)
import Control.Distributed.Task.TaskSpawning.TaskSpawningTypes
import Control.Distributed.Task.Types.TaskTypes

{-
 Deploys a given binary and executes it with the arguments defined by convention, including the serialized closure runnable in that program.

 The execution does not include error handling, this should be done on master/client.
-}
deployAndRunSerializedThunk :: String -> BL.ByteString -> IOHandling -> BL.ByteString -> IO [CompleteTaskResult]
deployAndRunSerializedThunk mainArg taskFunction = deployAndRunExternalBinary [mainArg, show taskFunction]

{-
 Accepts the distributed, serialized closure as part of the spawned program and executes it.

 Parameter handling is done via simple serialization.
-}
--TODO refactor module borders to have fully unterstandable resonsibilities, shis should go up to have nothing to do with serialization of thunks?
acceptAndExecuteSerializedThunk :: BL.ByteString -> IO (TaskInput -> TaskResult)
acceptAndExecuteSerializedThunk taskFn = (deserializeFunction taskFn :: IO (TaskInput -> TaskResult)) >>= (\f -> return (take 10 . f))

serializedThunkExecution ::  IOHandling -> (TaskInput -> TaskResult) -> IO ()
serializedThunkExecution = fullBinaryExecution -- nothing conceptually different at this point
