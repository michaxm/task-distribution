module Main where

import qualified Data.ByteString.Lazy as BL
import Data.List (intersperse)
import Data.List.Split (splitOn)
import System.Console.GetOpt (getOpt, OptDescr(..), ArgOrder(..), ArgDescr(..))
import System.Environment (getArgs, getProgName, getExecutablePath)

import ClusterComputing.TaskDistribution
import TaskSpawning.TaskTypes
import TaskSpawning.TaskSpawning (executeFullBinaryArg, fullDeploymentSerialize, fullDeploymentExecute)

main :: IO ()
main = do
  args <- getArgs
  case args of
   ("master" : masterArgs) -> runMaster $ parseMasterOpts masterArgs
   ["worker", workerHost, workerPort] -> startWorkerNode (workerHost, (read workerPort))
   ["showworkers"] -> showWorkerNodes ("localhost", 44440)
   ["shutdown"] -> shutdownWorkerNodes ("localhost", 44440)
   -- this is an expected entry point for every client program using full binary serialization (as demonstrated in "fullbinarydemo")
   [executeFullBinaryArg, taskFn, taskInput] -> fullDeploymentExecute (read taskFn) (read taskInput)
   _ -> userSyntaxError "unknown mode"

userSyntaxError :: String -> undefined
userSyntaxError reason = error $ usageInfo ++ reason ++ "\n"

usageInfo :: String
usageInfo = "Syntax: master <host> <port> <module:module path|fullbinarydemo> <simpledata:numDBs|hdfs:<thrift server port>:<file path>\n"
            ++ "| worker <worker host> <worker port>\n"
            ++ "| shutdown\n"

data MasterOptions = MasterOptions {
  _host :: String,
  _port :: Int,
  _taskSpec :: TaskSpec,
  _dataSpecs :: [DataSpec]
  }

data TaskSpec = SourceCodeSpec String
              | FullBinaryDeployment (TaskInput -> TaskResult)

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
       ["fullbinarydemo"] -> FullBinaryDeployment (map (++ " append dynamic over binary transport"))
       _ -> userSyntaxError $ "unknown task specification: " ++ args
    parseDataSpec :: String -> String -> [DataSpec]
    parseDataSpec masterHost args =
      case splitOn ":" args of
       ["simpledata", numDBs] -> mkSimpleDataSpecs $ read numDBs
       ["hdfs", thriftPort, hdfsPath] -> mkHdfsDataSpec masterHost (read thriftPort) hdfsPath
       _ -> userSyntaxError $ "unknown data specification: " ++ args
      where
        mkSimpleDataSpecs :: Int -> [DataSpec]
        mkSimpleDataSpecs 0 = []
        mkSimpleDataSpecs n = PseudoDB n : (mkSimpleDataSpecs (n-1))
        mkHdfsDataSpec :: String -> Int -> String -> [DataSpec]
        mkHdfsDataSpec host port path = [HdfsData (host, port) path]

runMaster :: MasterOptions -> IO ()
runMaster (MasterOptions masterHost masterPort taskSpec dataSpecs) = do
  taskDef <- buildTaskDef taskSpec
  executeDistributed (masterHost, masterPort) taskDef dataSpecs resultProcessor
    where
      buildTaskDef :: TaskSpec -> IO TaskDef
      buildTaskDef (SourceCodeSpec modulePath) = do
        moduleContent <- readFile modulePath
        return $ mkSourceCodeModule modulePath moduleContent
      buildTaskDef (FullBinaryDeployment function) = do
        selfPath <- getExecutablePath
        fullDeploymentSerialize selfPath function

-- TODO streamline, move to lib?
mkSourceCodeModule :: String -> String -> TaskDef
mkSourceCodeModule modulePath moduleContent = SourceCodeModule (strippedModuleName modulePath) moduleContent
  where
    strippedModuleName = reverse . takeWhile (/= '/') . drop 1 . dropWhile (/= '.') . reverse

-- FIXME type annotation has nothing to do with type safety here!!!
resultProcessor :: TaskResult -> IO ()
resultProcessor = putStrLn . join "\n" . map show

join :: String -> [String] -> String
join separator = concat . intersperse separator
