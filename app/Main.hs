module Main where

import Data.List (intersperse)
import Data.List.Split (splitOn)
import Data.Strings (strReplace)
import System.Environment (getArgs, getExecutablePath)
import qualified System.Log.Logger as L

import ClusterComputing.LogConfiguration (initDefaultLogging)
import ClusterComputing.RunComputation
import ClusterComputing.TaskDistribution (startSlaveNode, showSlaveNodes, showSlaveNodesWithData, shutdownSlaveNodes)
import TaskSpawning.RemoteExecutionSupport
import Types.HdfsConfigTypes
import Types.TaskTypes

import FullBinaryExamples
import RemoteExecutable
import VisitCalculation

main :: IO ()
main = withRemoteExecutionSupport calculateVisits $ do
  initDefaultLogging ""
  args <- getArgs
  case args of
   ("master" : masterArgs) -> runMaster (parseMasterOpts masterArgs)
   ["slave", slaveHost, slavePort] -> startSlaveNode (slaveHost, (read slavePort))
   ["showslaves"] -> showSlaveNodes localConfig
   ["slaveswithhdfsdata", host, port, hdfsFilePath] -> showSlaveNodesWithData localConfig (host, read port) hdfsFilePath
   ["shutdown"] -> shutdownSlaveNodes localConfig
   _ -> userSyntaxError "unknown mode"

localConfig :: HdfsConfig
localConfig = ("localhost", 44440)

userSyntaxError :: String -> undefined
userSyntaxError reason = error $ usageInfo ++ reason ++ "\n"

usageInfo :: String
usageInfo = "Syntax: master"
            ++ " <host>"
            ++ " <port>"
            ++ " <module:<module path>|fullbinary[-stream-input]|serializethunkdemo:<demo function>:<demo arg>|objectcodedemo>"
            ++ " <simpledata:numDBs|hdfs:<thrift server port>:<file path>"
            ++ " <collectonmaster|discard|storeinhdfs:<outputprefix>>\n"
            ++ "| slave <slave host> <slave port>\n"
            ++ "| showslaves\n"
            ++ "| slaveswithhdfsdata <thrift host> <thrift port> <hdfs file path>\n"
            ++ "| shutdown\n"
            ++ "\n"
            ++ "demo functions (with demo arg description): append:<suffix> | filter:<infixfilter> | visitcalc:unused \n"

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
       ["fullbinary"] -> FullBinaryDeployment File
       ["fullbinary-stream-input"] -> FullBinaryDeployment Stream
       ["serializethunkdemo", demoFunction, demoArg] -> mkSerializeThunkDemoArgs demoFunction demoArg
       ["objectcodedemo"] -> ObjectCodeModuleDeployment remoteExecutable
       _ -> userSyntaxError $ "unknown task specification: " ++ args
       where
        mkSerializeThunkDemoArgs :: String -> String -> TaskSpec
        mkSerializeThunkDemoArgs "append" demoArg = SerializedThunk (appendDemo demoArg)
        mkSerializeThunkDemoArgs "filter" demoArg = SerializedThunk (filterDemo demoArg)
        mkSerializeThunkDemoArgs "visitcalc" _ = SerializedThunk calculateVisits
        mkSerializeThunkDemoArgs d a = userSyntaxError $ "unknown demo: " ++ d ++ ":" ++ a
    parseDataSpec :: String -> String -> DataSpec
    parseDataSpec masterHost args =
      case splitOn ":" args of
       ["simpledata", numDBs] -> SimpleDataSpec $ read numDBs
       ["hdfs", thriftPort, hdfsPath] -> HdfsDataSpec ((masterHost, read thriftPort), hdfsPath)
       _ -> userSyntaxError $ "unknown data specification: " ++ args
    parseResultSpec args =
      case splitOn ":" args of
       ["collectonmaster"] -> CollectOnMaster resultProcessor
       ["storeinhdfs", outputPrefix] -> StoreInHdfs outputPrefix
       ["discard"] -> Discard
       _ -> userSyntaxError $ "unknown result specification: " ++ args

resultProcessor :: TaskResult -> IO ()
resultProcessor res = do
 putStrLn $ joinStrings "\n" $ map show res
 putStrLn $ "got " ++ (show $ length res) ++ " results in total"

joinStrings :: String -> [String] -> String
joinStrings separator = concat . intersperse separator
