module Util.Logging (logDebug, logInfo, logWarn, logError) where

import qualified System.Log.Logger as L

logError, logWarn, logInfo, logDebug :: String -> IO ()
logError = simpleLog L.errorM
logWarn = simpleLog L.warningM
logInfo = simpleLog L.infoM
-- as hslogger logging does not seem to be that performant when there is nothing is to log, debugLogging is "configured" here to a hard off
logDebug _ = return () --simpleLog L.debugM

simpleLog :: (String -> String -> IO ()) -> String -> IO ()
simpleLog levelLogger = levelLogger L.rootLoggerName
