module Control.Distributed.Task.TaskSpawning.DeployFullBinary (
  deployAndRunFullBinary, deployAndRunExternalBinary, fullBinaryExecution, runExternalBinary,
  packIOHandling, unpackIOHandling, IOHandling(..)
  ) where

import qualified Codec.Compression.GZip as GZip
import Control.Concurrent.Async (async, wait)
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.ByteString.Lazy as BL
import Data.List (intersperse)
import System.Process (readProcessWithExitCode)

import Control.Distributed.Task.DataAccess.DataSource
import Control.Distributed.Task.TaskSpawning.ExecutionUtil
import Control.Distributed.Task.TaskSpawning.TaskDefinition
import Control.Distributed.Task.TaskSpawning.TaskDescription
import Control.Distributed.Task.TaskSpawning.TaskSpawningTypes
import Control.Distributed.Task.Types.TaskTypes
import Control.Distributed.Task.Util.FileUtil
import Control.Distributed.Task.Util.Logging

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
  (results, runtime) <- measureDuration $ do
    readProcessWithExitCode "chmod" ["+x", executablePath] "" >>= expectSilentSuccess
    logInfo $ "worker: spawning external process: "++executablePath++" with baseArgs: "++show programBaseArgs
    processOutput <- executeExternal executablePath (programBaseArgs ++ [packIOHandling ioHandling])
    logInfo $ "worker: external process finished: "++executablePath
    return $ consumeResults processOutput
  return $ map (\r -> (DirectResult r, (fromIntegral (0 :: Integer), runtime))) results
  -- TODO measure data load times seperately

{-|
 Some methods parse the contents of stdout, thus these will fail in the case of logging to it (only ERROR at the moment).
-}
fullBinaryExecution :: IOHandling -> Task -> IO ()
fullBinaryExecution (IOHandling dataDefs resultDef) task = do
  logInfo $ "external binary: processing "++(concat $ intersperse ", " $ map describe dataDefs)
  results <- mapM (async . runSingle task resultDef) dataDefs
   -- reminder: trying to collect results here and submit them collectively makes it harder to force evaluation into the parallel part
  _output <- mapM wait results
  logInfo $ "external binary: tasks completed"

runSingle :: Task -> ResultDef -> DataDef -> IO ()
runSingle task resultDef dataDef = do
  taskInput <- loadData dataDef
  let result = task taskInput in do
    processedResult <- processResult result
    emitResult processedResult
  where
    processResult :: TaskResult -> IO TaskResult
    processResult taskResult =
      case resultDef of
       ReturnAsMessage -> return taskResult
       ReturnOnlyNumResults -> return [BLC.pack $ show $ length taskResult]
       (HdfsResult pre suf z) -> writeToHdfs >> return []
         where
           hdfsPath (HdfsData p) = pre++"/"++p++"/"++suf
           hdfsPath _ = error "implemented only for hdfs output"
           writeToHdfs =
             withTempBLCFile "move-to-hdfs" fileContent $ \tempFileName ->
               let (dirPart, filePart) = splitBasePath $ hdfsPath dataDef
               in copyToHdfs tempFileName dirPart filePart
               where
                 fileContent = (if z then GZip.compress else id) $ BLC.concat $ intersperse (BLC.pack "\n") taskResult
                 copyToHdfs localFile destPath destFilename = do
                   logInfo $ "external binary: copying "++localFile++" to "++destPath++"/"++destFilename
                   _ <- executeExternal "hdfs" ["dfs", "-mkdir", "-p", destPath] -- hadoop-rpc does not yet support writing, the external system call is an acceptable workaround
                   executeExternal "hdfs" ["dfs", "-copyFromLocal", localFile, destPath++"/"++destFilename]

emitResult :: TaskResult -> IO ()
emitResult = BLC.putStrLn . BLC.concat . intersperse (BLC.pack "|")

consumeResults :: String -> [TaskResult]
consumeResults = map (BLC.split '|') . BLC.lines . BLC.pack
