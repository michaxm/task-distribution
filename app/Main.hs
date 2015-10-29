module Main where

import Data.List (intersperse)
import Data.List.Split (splitOn)
import System.Console.GetOpt (getOpt, OptDescr(..), ArgOrder(..), ArgDescr(..))
import System.Environment (getArgs)

import ClusterComputing.TaskDistribution
import TaskSpawning.TaskTypes

main :: IO ()
main = do
  args <- getArgs
  case args of
   ("master" : masterArgs) -> runMaster $ parseMasterOpts masterArgs
   ["worker", workerHost, workerPort] -> startWorkerNode (workerHost, (read workerPort))
   ["showworkers"] -> showWorkerNodes ("localhost", 44440)
   ["shutdownlocal"] -> shutdownWorkerNodes ("localhost", 44440)
   _ -> putStrLn usageInfo

usageInfo :: String
usageInfo = "Syntax: master <host> <port> <module path> <simpledata:numDBs|hdfs:<thrift server port>:<file path>\n"
            ++ "| worker <worker host> <worker port>\n"
            ++ "| shutdownlocal"

data MasterOptions = MasterOptions {
  _host :: String,
  _port :: Int,
  _modulePath :: String,
  _dataSpecs :: [DataSpec]
  }

parseMasterOpts :: [String] -> MasterOptions
parseMasterOpts args =
  case args of
   [masterHost, port, modulePath, dataSpec] -> MasterOptions masterHost (read port) modulePath (parseDataSpec masterHost dataSpec)
   _ -> error $ usageInfo ++ "\n wrong number of master options"
  where
    parseDataSpec :: String -> String -> [DataSpec]
    parseDataSpec masterHost args =
      case splitOn ":" args of
       ["simpledata", numDBs] -> mkSimpleDataSpecs $ read numDBs
       ["hdfs", thriftPort, hdfsPath] -> mkHdfsDataSpec masterHost (read thriftPort) hdfsPath
      where
        mkSimpleDataSpecs :: Int -> [DataSpec]
        mkSimpleDataSpecs 0 = []
        mkSimpleDataSpecs n = PseudoDB n : (mkSimpleDataSpecs (n-1))
        mkHdfsDataSpec :: String -> Int -> String -> [DataSpec]
        mkHdfsDataSpec host port path = [HdfsData (host, port) path]

runMaster :: MasterOptions -> IO ()
runMaster (MasterOptions masterHost masterPort modulePath dataSpecs) = do
     moduleContent <- readFile modulePath
     executeDistributed (masterHost, masterPort) (mkSourceCodeModule modulePath moduleContent) dataSpecs resultProcessor

-- TODO streamline, move to lib?
mkSourceCodeModule :: String -> String -> TaskDef
mkSourceCodeModule modulePath moduleContent = SourceCodeModule (strippedModuleName modulePath) moduleContent
  where
    strippedModuleName = reverse . takeWhile (/= '/') . drop 1 . dropWhile (/= '.') . reverse

-- FIXME type annotation has nothing to do with type safety here!!!
resultProcessor :: TaskResult -> IO ()
resultProcessor = putStrLn . concat . intersperse "\n" . map show
