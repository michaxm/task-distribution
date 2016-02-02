{-|
  Catches expected entry points for full binary deployment / thunk serialization.
  These modes deploy the itself as a program and are called remote with different arguments, which is handled here.
|-}
module Control.Distributed.Task.TaskSpawning.RemoteExecutionSupport (
  withRemoteExecutionSupport,
  withFullBinaryRemoteExecutionSupport,
  withSerializedThunkRemoteExecutionSupport
  ) where

import qualified System.Log.Logger as L
import System.Environment (getArgs, getExecutablePath)

import Control.Distributed.Task.TaskSpawning.DeployFullBinary (unpackIOHandling)
import Control.Distributed.Task.TaskSpawning.TaskSpawning (executeFullBinaryArg, executionWithinSlaveProcessForFullBinaryDeployment, executeSerializedThunkArg, executionWithinSlaveProcessForThunkSerialization)
import Control.Distributed.Task.Types.TaskTypes
import Control.Distributed.Task.Util.Configuration
import Control.Distributed.Task.Util.Logging

withRemoteExecutionSupport :: (TaskInput -> TaskResult) -> IO () -> IO ()
withRemoteExecutionSupport fn = withSerializedThunkRemoteExecutionSupport . withFullBinaryRemoteExecutionSupport fn

withFullBinaryRemoteExecutionSupport :: (TaskInput -> TaskResult) -> IO () -> IO ()
withFullBinaryRemoteExecutionSupport fn mainAction = do
  args <- getArgs
  case args of
   [mode, ioHandling] ->
     if mode == executeFullBinaryArg
     then
       initTaskLogging >>
       executionWithinSlaveProcessForFullBinaryDeployment (unpackIOHandling ioHandling) fn
     else mainAction
   _ -> mainAction

withSerializedThunkRemoteExecutionSupport :: IO () -> IO ()
withSerializedThunkRemoteExecutionSupport mainAction = do
  args <- getArgs
  case args of
   [mode, taskFn, ioHandling] ->
     if mode == executeSerializedThunkArg
     then
       initTaskLogging >>
       executionWithinSlaveProcessForThunkSerialization (unpackIOHandling ioHandling) taskFn
     else mainAction
   _ -> mainAction

initTaskLogging :: IO ()
initTaskLogging = do
  conf <- getConfiguration
  initLogging L.ERROR L.INFO (_taskLogFile conf)
  self <- getExecutablePath
  logInfo $ "started task execution for: "++self
