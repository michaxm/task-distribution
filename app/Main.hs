module Main where

import Data.List (intersperse)
import System.Environment (getArgs)

import ClusterComputing.TaskDistribution
import TaskSpawning.TaskTypes

main :: IO ()
main = do
  args <- getArgs
  case args of
   ["master", "simpledata", masterHost, masterPort, numDBs, modulePath] ->
     runMaster masterHost masterPort (mkSimpleDataSpecs $ read numDBs) modulePath
   ["master", "hdfs", masterHost, masterPort, hdfsPath, thriftPort, modulePath] ->
     runMaster masterHost masterPort (mkHdfsDataSpec masterHost (read thriftPort) hdfsPath) modulePath
   ["worker", workerHost, workerPort] -> startWorkerNode (workerHost, workerPort)
   ["showworkers"] -> showWorkerNodes ("localhost", "44440")
   ["shutdownlocal"] -> shutdownWorkerNodes ("localhost", "44440")
   _ -> putStrLn ("Syntax: master simpledata <master hostname> <master port> <numDBs> <module path>\n"
                  ++ "| master hdfs <master hostname> <master port> <hdfs path> <thrift server port> <module path>\n"
                  ++ "| worker <worker host> <worker port>\n"
                  ++ "| shutdownlocal")

runMaster :: String -> String -> [DataSpec] -> String -> IO ()
runMaster masterHost masterPort dataSpecs modulePath = do
     moduleContent <- readFile modulePath
     executeDistributed (masterHost, masterPort) (mkSourceCodeModule modulePath moduleContent) dataSpecs resultProcessor
  

-- TODO streamline, move to lib
mkSourceCodeModule :: String -> String -> TaskDef
mkSourceCodeModule modulePath moduleContent = SourceCodeModule (strippedModuleName modulePath) moduleContent
  where
    strippedModuleName = reverse . takeWhile (/= '/') . drop 1 . dropWhile (/= '.') . reverse

mkSimpleDataSpecs :: Int -> [DataSpec]
mkSimpleDataSpecs 0 = []
mkSimpleDataSpecs n = PseudoDB n : (mkSimpleDataSpecs (n-1))

mkHdfsDataSpec :: String -> Int -> String -> [DataSpec]
mkHdfsDataSpec host port path = [HdfsData (host, port) path]

-- FIXME type annotation has nothing to do with type safety here!!!
resultProcessor :: TaskResult -> IO ()
resultProcessor = putStrLn . concat . intersperse "\n" . map show
