module TaskSpawning.DeployFullBinary (deployAndRunFullBinary, deployAndRunExternalBinary, fullBinaryExecution, runExternalBinary) where

import qualified Data.ByteString.Char8 as BLC
import qualified Data.ByteString.Lazy as BL
import Data.List (intersperse)
import Data.Time.Clock (NominalDiffTime)
import System.FilePath ()
import System.Process (readProcessWithExitCode)

import TaskSpawning.ExecutionUtil
import TaskSpawning.TaskTypes -- TODO ugly to be referenced explicitely here - generalization possible?
import Util.Logging

deployAndRunFullBinary :: String -> BL.ByteString -> TaskInput -> IO (FilePath, NominalDiffTime, NominalDiffTime)
deployAndRunFullBinary mainArg = deployAndRunExternalBinary [mainArg]

deployAndRunExternalBinary :: [String] -> BL.ByteString -> TaskInput -> IO (FilePath, NominalDiffTime, NominalDiffTime)
deployAndRunExternalBinary programBaseArgs program taskInput = do
  ((res, execDur), totalDur) <- measureDuration $ withTempBLFile "distributed-program" program $ runExternalBinary programBaseArgs taskInput
  return (res, (totalDur - execDur), execDur)

runExternalBinary :: [String] -> TaskInput -> FilePath -> IO (FilePath, NominalDiffTime)
runExternalBinary programBaseArgs taskInput filePath = do
  readProcessWithExitCode "chmod" ["+x", filePath] "" >>= expectSilentSuccess
  putStrLn $ "running " ++ filePath ++ "... "
  ((executionOutput, outFilePath), execDur) <- measureDuration $ withTempBLCFile "distributed-program-data" (serializeTaskInput taskInput) (
    \taskInputFilePath -> do
      taskOutputFilePath <- return $ taskInputFilePath ++ ".out"
      -- note: although it seems a bit fishy, read/show serialization between ByteString and String seems to be working just fine for the serialized closure
      processOutput <- readProcessWithExitCode filePath (programBaseArgs ++ [taskInputFilePath, taskOutputFilePath]) ""
      return (processOutput, taskOutputFilePath)
    )
  putStrLn $ "... run completed" -- TODO trace logging ++ (show executionOutput)
  stdOut <- expectSuccess executionOutput
  logDebug stdOut
  return (outFilePath, execDur)

fullBinaryExecution :: (TaskInput -> TaskResult) -> FilePath -> FilePath -> IO ()
fullBinaryExecution function taskInputFilePath taskOutputFilePath = do
  --TODO real logging: may not be written to stdout, since that is what we consume as result
  logDebug $ "reading data from: " ++ taskInputFilePath
  fileContents <- BLC.readFile taskInputFilePath
  logTrace $ show fileContents
  taskInput <- deserializeTaskInput fileContents
  logTrace $ show $ taskInput
  logDebug $ "calculating result"
  result <- return $ function taskInput
  logDebug $ "printing result"
  writeFile taskOutputFilePath $ concat $ intersperse "\n" result
