{-# LANGUAGE BangPatterns #-}
module Control.Distributed.Task.TaskSpawning.DeployFullBinary (
  deployAndRunFullBinary, deployAndRunExternalBinary, fullBinaryExecution, runExternalBinary,
  packIOHandling, unpackIOHandling, IOHandling(..)
  ) where

import qualified Codec.Compression.GZip as GZip
import Control.Concurrent.Async (async, wait)
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.ByteString.Lazy as BL
import Data.List (intersperse)
import Data.List.Split (splitOn)
import System.Process (readProcessWithExitCode)

import Control.Distributed.Task.DataAccess.DataSource
import Control.Distributed.Task.TaskSpawning.ExecutionUtil
import Control.Distributed.Task.TaskSpawning.TaskDefinition
import Control.Distributed.Task.TaskSpawning.TaskDescription
import Control.Distributed.Task.TaskSpawning.TaskSpawningTypes
import Control.Distributed.Task.Types.TaskTypes
import Control.Distributed.Task.Util.FileUtil
import Control.Distributed.Task.Util.Logging
import Control.Distributed.Task.Util.SerializationUtil

deployAndRunFullBinary :: String -> IOHandling -> BL.ByteString -> IO [CompleteTaskResult]
deployAndRunFullBinary mainArg = deployAndRunExternalBinary [mainArg]

deployAndRunExternalBinary :: [String] -> IOHandling -> BL.ByteString -> IO [CompleteTaskResult]
deployAndRunExternalBinary programBaseArgs ioHandling program =
  withTempBLFile "distributed-program" program $ runExternalBinary programBaseArgs ioHandling

{-|
 Makes an external system call, parameters must match those read in RemoteExecutionSupport.
-}
 -- should setting the executable flag rather be a part of the binary storage?
runExternalBinary :: [String] -> IOHandling -> FilePath -> IO [CompleteTaskResult]
runExternalBinary programBaseArgs ioHandling executablePath = do
  readProcessWithExitCode "chmod" ["+x", executablePath] "" >>= expectSilentSuccess
  logInfo $ "worker: spawning external process: "++executablePath++" with baseArgs: "++show programBaseArgs
  processOutput <- executeExternal executablePath (programBaseArgs ++ [packIOHandling ioHandling])
  logInfo $ "worker: external process finished: "++executablePath
  logTrace $ "got result from stdout : "++processOutput
  return $ parseResults processOutput

{-|
 Some methods parse the contents of stdout, thus these will fail in the case of logging to it (only ERROR at the moment).
-}
fullBinaryExecution :: IOHandling -> Task -> IO ()
fullBinaryExecution (IOHandling dataDefs resultDef) task = do
  logInfo $ "external binary: processing "++show (length dataDefs)++" tasks"
  results <- mapM (async . runSingle task resultDef) dataDefs
  output <- mapM wait results
  writeResults output
  logInfo $ "result finished"

runSingle :: Task -> ResultDef -> DataDef -> IO CompleteTaskResult
runSingle task resultDef dataDef = do
  logInfo $ "reading data for: "++describe dataDef
  (taskInput, dataLoadTime) <- measureDuration $ loadData dataDef
  logTrace $ show $ taskInput
  logInfo $ "calculating result"
  (result, !execTime) <- measureDuration $! return $ task taskInput
  processResult result (dataLoadTime, execTime)
  where
    processResult ::TaskResult -> SingleTaskRunStatistics -> IO CompleteTaskResult
    processResult taskResult runStat =
      case resultDef of
       ReturnAsMessage -> return (DirectResult taskResult, runStat)
       ReturnOnlyNumResults -> return (DirectResult [BLC.pack $ show $ length taskResult], runStat)
       (HdfsResult pre suf z) -> do
         _ <- writeToHdfs
         return (StoredRemote, runStat)
           where
             hdfsPath (HdfsData p) = pre++"/"++p++"/"++suf
             hdfsPath _ = error "implemented only for hdfs output"
             writeToHdfs = do
               logInfo $ "copying result to: "++hdfsPath dataDef
               withTempBLCFile "move-to-hdfs" fileContent $ \tempFileName ->
                 let (dirPart, filePart) = splitBasePath $ hdfsPath dataDef
                 in copyToHdfs tempFileName dirPart filePart
                 where
                   fileContent = (if z then GZip.compress else id) $ BLC.concat $ intersperse (BLC.pack "\n") taskResult
                   copyToHdfs localFile destPath destFilename = do
                     _ <- executeExternal "hdfs" ["dfs", "-mkdir", "-p", destPath] -- hadoop-rpc does not yet support writing, the external system call is an acceptable workaround
                     executeExternal "hdfs" ["dfs", "-copyFromLocal", localFile, destPath ++ "/" ++ destFilename]

writeResults :: [CompleteTaskResult] -> IO ()
writeResults = mapM_ writeResult

writeResult :: CompleteTaskResult -> IO ()
writeResult (DirectResult res, runStat) = putStr ("DirectResult|"++packRunStat runStat) >> mapM_ (putStr . ("|"++) . BLC.unpack) res >> putStrLn ""
writeResult (StoredRemote, runStat) = putStrLn $ "StoredRemote|"++show runStat

parseResults :: String ->[CompleteTaskResult]
parseResults = map parseResult . lines

parseResult :: String -> CompleteTaskResult
parseResult resultLine =
  let fields = splitOn "|" resultLine 
      resultType = if length fields < 2 then error ("invalid result: "++resultLine) else head fields
      runStat = fields !! 1
  in case resultType of
      "StoredRemote" -> (StoredRemote, unpackRunStat runStat)
      "DirectResult" -> (DirectResult (map BLC.pack (drop 2 fields)), unpackRunStat runStat)
      _ -> error $ "no parse for result type: "++resultLine

packRunStat :: SingleTaskRunStatistics -> String
packRunStat (a, b) = show $ (serializeTimeDiff a, serializeTimeDiff b)
unpackRunStat :: String -> SingleTaskRunStatistics
unpackRunStat s = let (a, b) = read s in (deserializeTimeDiff a, deserializeTimeDiff b)
