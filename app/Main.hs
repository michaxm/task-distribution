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
     executeDistributed 3 modulePath resultProcessor
   ["worker", workerNumber] -> do
     startWorkerNode workerNumber
   ["shutdown"] -> do
     shutdownWorkerNodes
   _ -> putStrLn $ "Syntax: master <module path> | worker <number> | shutdown"

-- FIXME type annotation has nothing to do with type safety here!!!
resultProcessor :: TaskResult -> IO ()
resultProcessor = putStrLn . concat . intersperse "\n" . map show
