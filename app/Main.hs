module Main where

import Data.List (intersperse)
import System.Environment (getArgs)

import ClusterComputing.TaskDistribution
import TaskSpawning.TaskTypes

main :: IO ()
main = do
  args <- getArgs
  case args of
   ["master", modulePath] -> do
     moduleContent <- readFile modulePath
     executeDistributed (mkSourceCodeModule modulePath moduleContent) {-(mkSimpleDataSpecs 3)-} mkHdfsDataSpec resultProcessor
   ["worker", workerNumber] -> do
     startWorkerNode workerNumber
   ["shutdown"] -> do
     shutdownWorkerNodes
   _ -> putStrLn $ "Syntax: master <module path> | worker <number> | shutdown"

-- TODO streamline, move to lib
mkSourceCodeModule :: String -> String -> TaskDef
mkSourceCodeModule modulePath moduleContent = SourceCodeModule (strippedModuleName modulePath) moduleContent
  where
    strippedModuleName = reverse . takeWhile (/= '/') . drop 1 . dropWhile (/= '.') . reverse

mkSimpleDataSpecs :: Int -> [DataSpec]
mkSimpleDataSpecs 0 = []
mkSimpleDataSpecs n = PseudoDB n : (mkSimpleDataSpecs (n-1))

mkHdfsDataSpec = [HdfsData "/testfile"]

-- FIXME type annotation has nothing to do with type safety here!!!
resultProcessor :: TaskResult -> IO ()
resultProcessor = putStrLn . concat . intersperse "\n" . map show
