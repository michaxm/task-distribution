module Main where

import Data.List (intersperse)
import System.Environment (getArgs)

import ClusterComputing.TaskDistribution

main :: IO ()
main = do
  args <- getArgs
  case args of
   ["master", modulePath] -> do
     executeDistributed 1 modulePath resultProcessor
   ["worker", workerNumber] -> do
     startWorkerNode workerNumber
   ["shutdown"] -> do
     shutdownWorkerNodes
   _ -> putStrLn $ "Syntax: master <module path> | worker <number> | shutdown"

--FIXME
type DataEntry = String

resultProcessor :: [DataEntry] -> IO ()
resultProcessor = putStrLn . concat . intersperse "\n" . map show
