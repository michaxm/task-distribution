module TaskSpawning.DeployFullBinary (deployAndRunFullBinary, deployAndRunExternalBinary, fullBinaryExecution) where

import qualified Data.ByteString.Char8 as BLC
import qualified Data.ByteString.Lazy as BL
import Data.Time.Clock (NominalDiffTime)
import System.FilePath ()
import System.Process (readProcessWithExitCode)

import TaskSpawning.ExecutionUtil
import TaskSpawning.TaskTypes -- TODO ugly to be referenced explicitely here - generalization possible?
import Util.Logging

deployAndRunFullBinary :: String -> BL.ByteString -> TaskInput -> IO (TaskResult, NominalDiffTime, NominalDiffTime)
deployAndRunFullBinary mainArg = deployAndRunExternalBinary [mainArg]

deployAndRunExternalBinary :: [String] -> BL.ByteString -> TaskInput -> IO (TaskResult, NominalDiffTime, NominalDiffTime)
deployAndRunExternalBinary programBaseArgs program taskInput = do
  ((res, execDur), totalDur) <- measureDuration doRun
  return (res, (totalDur - execDur), execDur)
  where
    doRun =
      withTempBLFile "distributed-program" program (
        \filePath -> do
          readProcessWithExitCode "chmod" ["+x", filePath] "" >>= expectSilentSuccess
          putStrLn $ "running " ++ filePath ++ "... "
          (executionOutput, execDur) <- measureDuration $ withTempBLCFile "distributed-program-data" (serializeTaskInput taskInput) (
            \taskInputFilePath -> do
              -- note: although it seems a bit fishy, read/show serialization between ByteString and String seems to be working just fine for the serialized closure
              readProcessWithExitCode filePath (programBaseArgs ++ [taskInputFilePath]) "")
          putStrLn $ "... run completed" -- TODO trace logging ++ (show executionOutput)
          result <- expectSuccess executionOutput
          resParsed <- parseResult result
          return (resParsed, execDur))

fullBinaryExecution :: (TaskInput -> TaskResult) -> FilePath -> IO ()
fullBinaryExecution function taskInputFilePath = do
  --TODO real logging: may not be written to stdout, since that is what we consume as result
  logDebug $ "reading data from: " ++ taskInputFilePath
  fileContents <- BLC.readFile taskInputFilePath
  logTrace $ show fileContents
  taskInput <- deserializeTaskInput fileContents
  logTrace $ show $ taskInput
  logDebug $ "calculating result"
  result <- return $ function taskInput
  logDebug $ "printing result"
  print result
