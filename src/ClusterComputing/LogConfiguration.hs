module ClusterComputing.LogConfiguration (
  initLogging,
  initDefaultLogging -- TODO take loglevels from configuration
  ) where

import System.IO (stdout)
import qualified System.Log.Logger as L
import qualified System.Log.Handler as L (setFormatter)
import qualified System.Log.Handler.Simple as L
import qualified System.Log.Formatter as L

initLogging :: L.Priority -> L.Priority -> FilePath -> IO ()
initLogging stdoutLogLevel fileLogLevel logfile = do
  L.updateGlobalLogger L.rootLoggerName (L.removeHandler)
  L.updateGlobalLogger L.rootLoggerName (L.setLevel $ max' stdoutLogLevel fileLogLevel)
  addHandler' $ L.fileHandler logfile fileLogLevel
  addHandler' $ L.streamHandler stdout stdoutLogLevel
  where
    max' a b = if fromEnum a <= fromEnum b then a else b
    addHandler' logHandlerM = do
      logHandler <- logHandlerM
      h <- return $ L.setFormatter logHandler (L.simpleLogFormatter "[$time : $loggername : $prio] $msg")
      L.updateGlobalLogger L.rootLoggerName (L.addHandler h)

initDefaultLogging :: String -> IO ()
initDefaultLogging suffix = do
--  progName <- getExecutablePath
  initLogging L.WARNING L.INFO logfile --TODO logging relative to $CLUSTER_COMPUTING_HOME
    where
      logfile = ("log/task-distribution" ++ (if null suffix then "" else "-"++suffix) ++".log")
