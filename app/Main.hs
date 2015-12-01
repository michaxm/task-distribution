module Main where

import Data.List (intersperse, isInfixOf)
import Data.List.Split (splitOn)
import System.Environment (getArgs, getExecutablePath)
import qualified System.Log.Logger as L

import ClusterComputing.LogConfiguration (initDefaultLogging)
import ClusterComputing.RunComputation
import ClusterComputing.TaskDistribution (startWorkerNode, showWorkerNodes, showWorkerNodesWithData, shutdownWorkerNodes)
import TaskSpawning.FullBinaryTaskSpawningInterface (executeFullBinaryArg, fullExecutionWithinWorkerProcess)
import TaskSpawning.TaskTypes

import RemoteExecutable

main :: IO ()
main = do
  initDefaultLogging ""
  args <- getArgs
  case args of
   ("master" : masterArgs) -> runMaster (parseMasterOpts masterArgs)
   ["worker", workerHost, workerPort] -> startWorkerNode (workerHost, (read workerPort))
   ["showworkers"] -> showWorkerNodes localConfig
   ["workerswithhdfsdata", host, port, hdfsFilePath] -> showWorkerNodesWithData localConfig (host, read port) hdfsFilePath
   ["shutdown"] -> shutdownWorkerNodes localConfig
   -- this is an expected entry point for every client program using full binary serialization (as demonstrated in "fullbinarydemo")
   [executeFullBinaryArg, taskFn, taskInputFilePath] -> fullExecutionWithinWorkerProcess taskFn taskInputFilePath
   _ -> userSyntaxError "unknown mode"

localConfig :: HdfsConfig
localConfig = ("localhost", 44440)

userSyntaxError :: String -> undefined
userSyntaxError reason = error $ usageInfo ++ reason ++ "\n"

usageInfo :: String
usageInfo = "Syntax: master <host> <port> <module:<module path>|fullbinarydemo:<demo function>:<demo arg>|objectcodedemo> <simpledata:numDBs|hdfs:<thrift server port>:<file path> <collectonmaster>\n"
            ++ "| worker <worker host> <worker port>\n"
            ++ "| showworkers\n"
            ++ "| workerswithhdfsdata <thrift host> <thrift port> <hdfs file path>\n"
            ++ "| shutdown\n"

parseMasterOpts :: [String] -> MasterOptions
parseMasterOpts args =
  case args of
   [masterHost, port, taskSpec, dataSpec, resultSpec] -> MasterOptions masterHost (read port) (parseTaskSpec taskSpec) (parseDataSpec masterHost dataSpec) (parseResultSpec resultSpec)
   _ -> userSyntaxError "wrong number of master options"
  where
    parseTaskSpec :: String -> TaskSpec
    parseTaskSpec args =
      case splitOn ":" args of
       ["module", modulePath] -> SourceCodeSpec modulePath
       ["fullbinarydemo", demoFunction, demoArg] -> mkBinaryDemoArgs demoFunction demoArg
       ["objectcodedemo"] -> ObjectCodeModuleDeployment remoteExecutable
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
    parseResultSpec args =
      case splitOn ":" args of
       ["collectonmaster"] -> CollectOnMaster resultProcessor
       _ -> userSyntaxError $ "unknown result specification: " ++ args

resultProcessor :: TaskResult -> IO ()
resultProcessor res = do
 putStrLn $ joinStrings "\n" $ map show res
 putStrLn $ "got " ++ (show $ length res) ++ " results in total"

joinStrings :: String -> [String] -> String
joinStrings separator = concat . intersperse separator
