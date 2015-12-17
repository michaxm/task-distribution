module TaskSpawning.DeploySerializedThunk (deployAndRunSerializedThunk, serializedThunkExecution, acceptAndExecuteSerializedThunk) where

-- FIXME really lazy? rather use strict???
import qualified Data.ByteString.Lazy as BL
import Data.Time.Clock (NominalDiffTime)
import System.FilePath ()

import TaskSpawning.DeployFullBinary (deployAndRunExternalBinary, fullBinaryExecution)
import TaskSpawning.FunctionSerialization (deserializeFunction)
import TaskSpawning.TaskTypes -- TODO ugly to be referenced explicitely here - generalization possible?

{-
 Deploys a given binary and executes it with the arguments defined by convention, including the serialized closure runnable in that program.

 The execution does not include error handling, this should be done on master/client.
-}
deployAndRunSerializedThunk :: String -> BL.ByteString -> BL.ByteString -> TaskInput -> IO (FilePath, NominalDiffTime, NominalDiffTime)
deployAndRunSerializedThunk mainArg taskFunction =
  -- note: although it seems a bit fishy, read/show serialization between ByteString and String seems to be working just fine for the serialized closure
  deployAndRunExternalBinary [mainArg, show taskFunction]

{-
 Accepts the distributed, serialized closure as part of the spawned program and executes it.

 Parameter handling is done via simple serialization.
-}
--TODO refactor module borders to have fully unterstandable resonsibilities, shis should go up to have nothing to do with serialization of thunks?
acceptAndExecuteSerializedThunk :: BL.ByteString -> IO (TaskInput -> TaskResult)
acceptAndExecuteSerializedThunk taskFn = (deserializeFunction taskFn :: IO (TaskInput -> TaskResult)) >>= (\f -> return (take 10 . f))

serializedThunkExecution :: (TaskInput -> TaskResult) -> FilePath -> FilePath -> IO ()
serializedThunkExecution = fullBinaryExecution -- nothing different at this point
