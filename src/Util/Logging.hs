module Util.Logging (logError, logWarn, logInfo, logDebug, logTrace, initLogging) where

import System.IO (stdout)
import qualified System.Log.Logger as L
import qualified System.Log.Handler as L (setFormatter)
import qualified System.Log.Handler.Simple as L
import qualified System.Log.Formatter as L

logError, logWarn, logInfo, logDebug, logTrace :: String -> IO ()
logError = simpleLog L.errorM
logWarn = simpleLog L.warningM
logInfo = simpleLog L.infoM
-- as hslogger logging does not seem to be that performant when there is nothing is to log, debugLogging is "configured" here to a hard off
logDebug _ = return () --simpleLog L.debugM
logTrace _ = return () --simpleLog L.debugM

simpleLog :: (String -> String -> IO ()) -> String -> IO ()
simpleLog levelLogger = levelLogger L.rootLoggerName

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
