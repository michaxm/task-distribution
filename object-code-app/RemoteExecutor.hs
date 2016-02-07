import qualified Data.ByteString.Char8 as BLC
import System.Environment (getArgs, getExecutablePath)
import qualified System.Log.Logger as L

import Control.Distributed.Task.TaskSpawning.DeployFullBinary
import Control.Distributed.Task.Types.TaskTypes
import Control.Distributed.Task.Util.Configuration
import Control.Distributed.Task.Util.Logging

import RemoteExecutable

main :: IO ()
main = do
  args <- getArgs
  case args of
   [ioHandling] -> executeFunction (unpackIOHandling ioHandling)
   _ -> error "Syntax for relinked task: <ioHandling>\n"

executeFunction :: IOHandling -> IO ()
executeFunction ioHandling = do
  initTaskLogging
  fullBinaryExecution ioHandling executeF
  where
    executeF :: Task
    executeF = remoteExecutable

initTaskLogging :: IO ()
initTaskLogging = do
  conf <- getConfiguration
  initLogging L.ERROR L.INFO (_taskLogFile conf)
  self <- getExecutablePath
  logInfo $ "started task execution for: "++self
