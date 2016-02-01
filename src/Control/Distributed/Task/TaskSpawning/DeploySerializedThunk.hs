module Control.Distributed.Task.TaskSpawning.DeploySerializedThunk (deployAndRunSerializedThunk, serializedThunkExecution, acceptAndExecuteSerializedThunk) where

import qualified Data.ByteString.Lazy as BL
import Data.Time.Clock (NominalDiffTime)
import System.FilePath ()

import Control.Distributed.Task.TaskSpawning.DeployFullBinary (deployAndRunExternalBinary, fullBinaryExecution, DataModes(..), InputMode(..), OutputMode(..), ZipOutput)
import Control.Distributed.Task.TaskSpawning.FunctionSerialization (deserializeFunction)
import Control.Distributed.Task.Types.TaskTypes -- TODO ugly to be referenced explicitely here - generalization possible?

{-
 Deploys a given binary and executes it with the arguments defined by convention, including the serialized closure runnable in that program.

 The execution does not include error handling, this should be done on master/client.
-}
deployAndRunSerializedThunk :: String -> BL.ByteString -> ZipOutput -> BL.ByteString -> TaskInput -> IO (FilePath, NominalDiffTime, NominalDiffTime)
deployAndRunSerializedThunk mainArg taskFunction zipOutput =
  -- note: although it seems a bit fishy, read/show serialization between ByteString and String seems to be working just fine for the serialized closure
  deployAndRunExternalBinary (DataModes FileInput (FileOutput zipOutput)) [mainArg, show taskFunction]

{-
 Accepts the distributed, serialized closure as part of the spawned program and executes it.

 Parameter handling is done via simple serialization.
-}
--TODO refactor module borders to have fully unterstandable resonsibilities, shis should go up to have nothing to do with serialization of thunks?
acceptAndExecuteSerializedThunk :: BL.ByteString -> IO (TaskInput -> TaskResult)
acceptAndExecuteSerializedThunk taskFn = (deserializeFunction taskFn :: IO (TaskInput -> TaskResult)) >>= (\f -> return (take 10 . f))

-- nothing different at this point, TODO not all options (streaming?) implemented there?
serializedThunkExecution ::  DataModes -> (TaskInput -> TaskResult) -> FilePath -> FilePath -> IO ()
serializedThunkExecution = fullBinaryExecution
