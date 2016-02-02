module Control.Distributed.Task.TaskSpawning.DeployFullBinary (
  deployAndRunFullBinary, deployAndRunExternalBinary, fullBinaryExecution, runExternalBinary,
  packIOHandling, unpackIOHandling, IOHandling(..)
  ) where

import qualified Codec.Compression.GZip as GZip
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

deployAndRunFullBinary :: String -> IOHandling -> BL.ByteString -> IO CompleteTaskResult
deployAndRunFullBinary mainArg = deployAndRunExternalBinary [mainArg]

deployAndRunExternalBinary :: [String] -> IOHandling -> BL.ByteString -> IO CompleteTaskResult
deployAndRunExternalBinary programBaseArgs ioHandling program =
  withTempBLFile "distributed-program" program $ runExternalBinary programBaseArgs ioHandling

 -- should setting the executable flag rather be a part of the binary storage?
runExternalBinary :: [String] -> IOHandling -> FilePath -> IO CompleteTaskResult
runExternalBinary programBaseArgs ioHandling executablePath = do
  readProcessWithExitCode "chmod" ["+x", executablePath] "" >>= expectSilentSuccess
  logInfo $ "running: "++executablePath
  processOutput <- executeExternal executablePath (programBaseArgs ++ [packIOHandling ioHandling])
  return $ parseResult processOutput

{-|
 Some methods parse the contents of stdout, thus these will fail in the case of logging to it (only ERROR at the moment).
|-}
fullBinaryExecution :: IOHandling -> (TaskInput -> TaskResult) -> IO ()
fullBinaryExecution (IOHandling dataDef resultDef) function = do
  logInfo $ "reading data for: "++describe dataDef
  (taskInput, dataLoadTime) <- measureDuration $ loadData dataDef
  logTrace $ show $ taskInput
  logInfo $ "calculating result"
  (result, execTime) <- measureDuration $! return $ function taskInput
  logTrace $ "printing result: " ++ show result
  returnResult resultDef result (dataLoadTime, execTime)
  logInfo $ "returned result"
  where
    returnResult :: ResultDef -> TaskResult -> RunStat -> IO ()
    returnResult ReturnAsMessage taskResult runStat = writeResult (DirectResult taskResult, runStat)
    returnResult ReturnOnlyNumResults taskResult runStat = writeResult (DirectResult [BLC.pack $ show $ length taskResult], runStat)
    returnResult (HdfsResult pre suf z) taskResult runStat = writeToHdfs >> writeResult (StoredRemote, runStat)
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
              _ <- executeExternal "hdfs" ["dfs", "-mkdir", "-p", destPath]
              executeExternal "hdfs" ["dfs", "-copyFromLocal", localFile, destPath ++ "/" ++ destFilename]

writeResult :: CompleteTaskResult -> IO ()
writeResult (DirectResult res, runStat) = putStrLn ("DirectResult|"++packRunStat runStat) >> mapM_ (putStrLn . BLC.unpack) res
writeResult (StoredRemote, runStat) = putStrLn $ "StoredRemote|"++show runStat

parseResult :: String -> CompleteTaskResult
parseResult result  =
  let resLines = lines result
      firstLine = if length resLines < 1 then error "empty result" else head resLines
  in let es = splitOn "|" firstLine
     in if (length es) /= 2
        then error $ "unknown value: "++firstLine
        else case es !! 0 of
              "StoredRemote" -> (StoredRemote, unpackRunStat $ es !! 1)
              "DirectResult" -> (DirectResult (map BLC.pack (tail resLines)), unpackRunStat $ es !! 1)
              _ -> error $ "no parse for result type: "++firstLine

packRunStat :: RunStat -> String
packRunStat (a, b) = show $ (serializeTimeDiff a, serializeTimeDiff b)
unpackRunStat :: String -> RunStat
unpackRunStat s = let (a, b) = read s in (deserializeTimeDiff a, deserializeTimeDiff b)
