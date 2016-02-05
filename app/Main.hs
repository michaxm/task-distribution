module Main where

import Data.List (intersperse)
import Data.List.Split (splitOn)
import System.Environment (getArgs)

import Control.Distributed.Task.Distribution.LogConfiguration (initDefaultLogging)
import Control.Distributed.Task.Distribution.RunComputation
import Control.Distributed.Task.Distribution.TaskDistribution (startSlaveNode, showSlaveNodes, showSlaveNodesWithData, shutdownSlaveNodes)
import Control.Distributed.Task.TaskSpawning.RemoteExecutionSupport
import Control.Distributed.Task.Types.HdfsConfigTypes
import Control.Distributed.Task.Types.TaskTypes

import FullBinaryExamples
import RemoteExecutable
import DemoTask

main :: IO ()
main = withRemoteExecutionSupport calculateRatio $ do
  initDefaultLogging ""
  args <- getArgs
  case args of
   ("master" : masterArgs) -> runMaster (parseMasterOpts masterArgs)
   ["slave", slaveHost, slavePort] -> startSlaveNode (slaveHost, (read slavePort))
   ["showslaves"] -> showSlaveNodes localConfig
   ["slaveswithhdfsdata", host, port, hdfsFilePath] -> showSlaveNodesWithData (host, read port) hdfsFilePath
   ["shutdown"] -> shutdownSlaveNodes localConfig
   _ -> userSyntaxError "unknown mode"

-- note: assumes no nodes with that configuration, should be read as parameters
localConfig :: HdfsConfig
localConfig = ("localhost", 44440)

userSyntaxError :: String -> undefined
userSyntaxError reason = error $ usageInfo ++ reason ++ "\n"

usageInfo :: String
usageInfo = "Syntax: master"
            ++ " <host>"
            ++ " <port>"
            ++ " <module:<module path>|fullbinary|serializethunkdemo:<demo function>:<demo arg>|objectcodedemo>"
            ++ " <simpledata:numDBs|hdfs:<file path>[:<subdir depth>[:<filename filter prefix>]]"
            ++ " <collectonmaster|discard|storeinhdfs:<outputprefix>[:<outputsuffix>]>\n"
            ++ "| slave <slave host> <slave port>\n"
            ++ "| showslaves\n"
            ++ "| slaveswithhdfsdata <thrift host> <thrift port> <hdfs file path>\n"
            ++ "| shutdown\n"
            ++ "\n"
            ++ "demo functions (with demo arg description): append:<suffix> | filter:<infixfilter> | visitcalc:unused \n"

parseMasterOpts :: [String] -> MasterOptions
parseMasterOpts args =
  case args of
   [masterHost, port, taskSpec, dataSpec, resultSpec] -> MasterOptions masterHost (read port) (parseTaskSpec taskSpec) (parseDataSpec dataSpec) (parseResultSpec resultSpec)
   _ -> userSyntaxError "wrong number of master options"
  where
    parseTaskSpec :: String -> TaskSpec
    parseTaskSpec taskArgs =
      case splitOn ":" taskArgs of
       ["module", modulePath] -> SourceCodeSpec modulePath
       ["fullbinary"] -> FullBinaryDeployment
       ["serializethunkdemo", demoFunction, demoArg] -> mkSerializeThunkDemoArgs demoFunction demoArg
       ["objectcodedemo"] -> ObjectCodeModuleDeployment remoteExecutable
       _ -> userSyntaxError $ "unknown task specification: " ++ taskArgs
       where
        mkSerializeThunkDemoArgs :: String -> String -> TaskSpec
        mkSerializeThunkDemoArgs "append" demoArg = SerializedThunk (appendDemo demoArg)
        mkSerializeThunkDemoArgs "filter" demoArg = SerializedThunk (filterDemo demoArg)
        mkSerializeThunkDemoArgs "visitcalc" _ = SerializedThunk calculateRatio
        mkSerializeThunkDemoArgs d a = userSyntaxError $ "unknown demo: " ++ d ++ ":" ++ a
    parseDataSpec :: String -> DataSpec
    parseDataSpec inputArgs =
      case splitOn ":" inputArgs of
       ["simpledata", numDBs] -> SimpleDataSpec $ read numDBs
       ("hdfs": hdfsPath: rest) -> HdfsDataSpec hdfsPath depth filter'
         where depth = if length rest > 0 then read (rest !! 0) else 0; filter' = if length rest > 1 then Just (rest !! 1) else Nothing
       _ -> userSyntaxError $ "unknown data specification: " ++ inputArgs
    parseResultSpec resultArgs =
      case splitOn ":" resultArgs of
       ["collectonmaster"] -> CollectOnMaster resultProcessor
       ("storeinhdfs":outputPrefix:rest) -> StoreInHdfs outputPrefix $ if null rest then "" else head rest
       ["discard"] -> Discard
       _ -> userSyntaxError $ "unknown result specification: " ++ resultArgs

resultProcessor :: [TaskResult] -> IO ()
resultProcessor res = do
 putStrLn $ joinStrings "\n" $ map show $ concat res
 putStrLn $ "got " ++ (show $ length $ concat res) ++ " results in total"

joinStrings :: String -> [String] -> String
joinStrings separator = concat . intersperse separator
