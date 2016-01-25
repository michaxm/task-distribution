module ClusterComputing.LogConfiguration (
  initLogging,
  initDefaultLogging -- TODO take loglevels from configuration
  ) where

import qualified System.Log.Logger as L

import Util.Logging (initLogging)

initDefaultLogging :: String -> IO ()
initDefaultLogging suffix = do
--  progName <- getExecutablePath
  initLogging L.WARNING L.INFO logfile --TODO logging relative to $CLUSTER_COMPUTING_HOME
    where
      logfile = ("log/task-distribution" ++ (if null suffix then "" else "-"++suffix) ++".log")
