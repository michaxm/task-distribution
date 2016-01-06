{-|
  Catches expected entry points for full binary deployment / thunk serialization.
  These modes deploy the itself as a program and are called remote with different arguments, which is handled here.
|-}
module ClusterComputing.RemoteExecutionSupport where

import System.Environment (getArgs)

import TaskSpawning.TaskSpawning (executeFullBinaryArg, executionWithinSlaveProcessForFullBinaryDeployment, executeSerializedThunkArg, executionWithinSlaveProcessForThunkSerialization)
import Types.TaskTypes

withRemoteExecutionSupport :: (TaskInput -> TaskResult) -> IO () -> IO ()
withRemoteExecutionSupport fn = withSerializedThunkRemoteExecutionSupport . withFullBinaryRemoteExecutionSupport fn

withFullBinaryRemoteExecutionSupport :: (TaskInput -> TaskResult) -> IO () -> IO ()
withFullBinaryRemoteExecutionSupport fn mainAction = do
  args <- getArgs
  case args of
   [mode, taskInputFilePath, taskOutputFilePath] -> if mode == executeFullBinaryArg then executionWithinSlaveProcessForFullBinaryDeployment fn taskInputFilePath taskOutputFilePath else mainAction
   _ -> mainAction

withSerializedThunkRemoteExecutionSupport :: IO () -> IO ()
withSerializedThunkRemoteExecutionSupport mainAction = do
  args <- getArgs
  case args of
   [mode, taskFn, taskInputFilePath, taskOutputFilePath] -> if mode == executeSerializedThunkArg then executionWithinSlaveProcessForThunkSerialization taskFn taskInputFilePath taskOutputFilePath else mainAction
   _ -> mainAction
