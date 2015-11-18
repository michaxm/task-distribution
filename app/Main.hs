module Main where

import Data.List (intersperse, isInfixOf)
import Data.List.Split (splitOn)
import System.IO (stdout)
import System.Console.GetOpt (getOpt, OptDescr(..), ArgOrder(..), ArgDescr(..))
import System.Environment (getArgs, getExecutablePath)
import qualified System.Log.Logger as L
import qualified System.Log.Handler as L (setFormatter)
import qualified System.Log.Handler.Simple as L
import qualified System.Log.Handler.Syslog as L
import qualified System.Log.Formatter as L

import ClusterComputing.RunComputation
import ClusterComputing.TaskDistribution (startWorkerNode, showWorkerNodes, showWorkerNodesWithData, shutdownWorkerNodes)
import TaskSpawning.TaskSpawning (executeFullBinaryArg, fullDeploymentExecute)
import TaskSpawning.TaskTypes

main :: IO ()
main = do
  initDefaultLogging
  args <- getArgs
  case args of
   ("master" : masterArgs) -> runMaster (parseMasterOpts masterArgs) resultProcessor
   ["worker", workerHost, workerPort] -> startWorkerNode (workerHost, (read workerPort))
   ["showworkers"] -> showWorkerNodes localConfig
   ["workerswithhdfsdata", host, port, hdfsFilePath] -> showWorkerNodesWithData localConfig (host, read port) hdfsFilePath
   ["shutdown"] -> shutdownWorkerNodes localConfig
   -- this is an expected entry point for every client program using full binary serialization (as demonstrated in "fullbinarydemo")
   [executeFullBinaryArg, taskFn, taskInputFilePath] -> fullDeploymentExecute taskFn taskInputFilePath
   _ -> userSyntaxError "unknown mode"

localConfig :: HdfsConfig
localConfig = ("localhost", 44440)

userSyntaxError :: String -> undefined
userSyntaxError reason = error $ usageInfo ++ reason ++ "\n"

usageInfo :: String
usageInfo = "Syntax: master <host> <port> <module:<module path>|fullbinarydemo:<demo function>:<demo arg>> <simpledata:numDBs|hdfs:<thrift server port>:<file path>\n"
            ++ "| worker <worker host> <worker port>\n"
            ++ "| showworkers\n"
            ++ "| workerswithhdfsdata <thrift host> <thrift port> <hdfs file path>\n"
            ++ "| shutdown\n"

parseMasterOpts :: [String] -> MasterOptions
parseMasterOpts args =
  case args of
   [masterHost, port, taskSpec, dataSpec] -> MasterOptions masterHost (read port) (parseTaskSpec taskSpec) (parseDataSpec masterHost dataSpec)
   _ -> userSyntaxError "wrong number of master options"
  where
    parseTaskSpec :: String -> TaskSpec
    parseTaskSpec args =
      case splitOn ":" args of
       ["module", modulePath] -> SourceCodeSpec modulePath
       ["fullbinarydemo", demoFunction, demoArg] -> mkBinaryDemoArgs demoFunction demoArg
       _ -> userSyntaxError $ "unknown task specification: " ++ args
       where
        mkBinaryDemoArgs :: String -> String -> TaskSpec
        mkBinaryDemoArgs "append" demoArg = FullBinaryDeployment (map (++ (" "++demoArg)))
        mkBinaryDemoArgs "filter" demoArg = FullBinaryDeployment (filter (demoArg `isInfixOf`))
    parseDataSpec :: String -> String -> DataSpec
    parseDataSpec masterHost args =
      case splitOn ":" args of
       ["simpledata", numDBs] -> SimpleDataSpec $ read numDBs
       ["hdfs", thriftPort, hdfsPath] -> HdfsDataSpec (masterHost, read thriftPort) hdfsPath
       _ -> userSyntaxError $ "unknown data specification: " ++ args

resultProcessor :: TaskResult -> IO ()
resultProcessor res = do
 putStrLn $ joinStrings "\n" $ map show res
 putStrLn $ "got " ++ (show $ length res) ++ " results in total"

joinStrings :: String -> [String] -> String
joinStrings separator = concat . intersperse separator

initDefaultLogging :: IO ()
initDefaultLogging = do
  progName <- getExecutablePath
  initLogging L.WARNING L.DEBUG "log/cluster-computing.log" --TODO logging relative to $CLUSTER_COMPUTING_HOME

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
