module Main where

--import qualified Data.ByteString.Lazy as BL
import Data.List (intersperse, isInfixOf)
import Data.List.Split (splitOn)
import System.Console.GetOpt (getOpt, OptDescr(..), ArgOrder(..), ArgDescr(..))
import System.Environment (getArgs, getProgName)

import ClusterComputing.RunComputation
import ClusterComputing.TaskDistribution (startWorkerNode, showWorkerNodes, shutdownWorkerNodes)
import TaskSpawning.TaskSpawning (executeFullBinaryArg, fullDeploymentExecute)
import TaskSpawning.TaskTypes

main :: IO ()
main = do
  args <- getArgs
  case args of
   ("master" : masterArgs) -> runMaster (parseMasterOpts masterArgs) resultProcessor
   ["worker", workerHost, workerPort] -> startWorkerNode (workerHost, (read workerPort))
   ["showworkers"] -> showWorkerNodes ("localhost", 44440)
   ["shutdown"] -> shutdownWorkerNodes ("localhost", 44440)
   -- this is an expected entry point for every client program using full binary serialization (as demonstrated in "fullbinarydemo")
   [executeFullBinaryArg, taskFn, taskInput] -> fullDeploymentExecute (read taskFn) (read taskInput)
   _ -> userSyntaxError "unknown mode"

userSyntaxError :: String -> undefined
userSyntaxError reason = error $ usageInfo ++ reason ++ "\n"

usageInfo :: String
usageInfo = "Syntax: master <host> <port> <module:<module path>|fullbinarydemo:<demo function>:<demo arg>> <simpledata:numDBs|hdfs:<thrift server port>:<file path>\n"
            ++ "| worker <worker host> <worker port>\n"
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
