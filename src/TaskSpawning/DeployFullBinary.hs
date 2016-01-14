module TaskSpawning.DeployFullBinary (deployAndRunFullBinary, deployAndRunExternalBinary, fullBinaryExecution, runExternalBinary, InputMode(..)) where

import qualified Data.ByteString.Char8 as BLC
import qualified Data.ByteString.Lazy as BL
import Data.List (intersperse)
import Data.Time.Clock (NominalDiffTime)
import System.Exit (ExitCode(..))
import System.FilePath ()
import System.Process (readProcessWithExitCode)

import TaskSpawning.ExecutionUtil
import TaskSpawning.StreamToExecutableUtil
import Types.TaskTypes -- TODO ugly to be referenced explicitely here - generalization possible?
import Util.Logging

data InputMode
  = FileInput
  | StreamInput
  deriving (Read, Show)

deployAndRunFullBinary :: InputMode -> String -> BL.ByteString -> TaskInput -> IO (FilePath, NominalDiffTime, NominalDiffTime)
deployAndRunFullBinary inputMode mainArg = deployAndRunExternalBinary inputMode [mainArg]

deployAndRunExternalBinary :: InputMode -> [String] -> BL.ByteString -> TaskInput -> IO (FilePath, NominalDiffTime, NominalDiffTime)
deployAndRunExternalBinary inputMode programBaseArgs program taskInput = do
  ((res, execDur), totalDur) <- measureDuration $ withTempBLFile "distributed-program" program $ runExternalBinary inputMode programBaseArgs taskInput
  return (res, (totalDur - execDur), execDur)

runExternalBinary :: InputMode -> [String] -> TaskInput -> FilePath -> IO (FilePath, NominalDiffTime)
runExternalBinary inputMode programBaseArgs taskInput filePath = do
  readProcessWithExitCode "chmod" ["+x", filePath] "" >>= expectSilentSuccess
  putStrLn $ "running " ++ filePath ++ "... "
  ((executionOutput, outFilePath), execDur) <- measureDuration $ runExternalBinaryForInputMode inputMode (programBaseArgs++[show inputMode]) taskInput filePath
  putStrLn $ "... run completed" -- TODO trace logging ++ (show executionOutput)
  stdOut <- expectSuccess executionOutput
  logDebug stdOut
  return (outFilePath, execDur)

runExternalBinaryForInputMode :: InputMode -> [String] -> TaskInput -> FilePath -> IO ((ExitCode, String, String), FilePath)
runExternalBinaryForInputMode FileInput programBaseArgs taskInput filePath = do
  withTempBLCFile "distributed-program-data" (serializeTaskInput taskInput) (
    \taskInputFilePath -> do
      taskOutputFilePath <- return $ taskInputFilePath ++ ".out"
      -- note: although it seems a bit fishy, read/show serialization between ByteString and String seems to be working just fine for the serialized closure
      processOutput <- readProcessWithExitCode filePath (programBaseArgs ++ [taskInputFilePath, taskOutputFilePath]) ""
      return (processOutput, taskOutputFilePath)
    )
runExternalBinaryForInputMode StreamInput programBaseArgs taskInput filePath = do
  taskOutputFilePath <- createTempFilePath "distributed-program-task.out"
  res <- executeExternalWritingToStdIn filePath (programBaseArgs ++ ["", taskOutputFilePath]) taskInput
  return (res, taskOutputFilePath)

fullBinaryExecution :: InputMode -> (TaskInput -> TaskResult) -> FilePath -> FilePath -> IO ()
fullBinaryExecution inputMode function taskInputFilePath taskOutputFilePath = do
  --TODO real logging: may not be written to stdout, since that is what we consume as result
  logDebug $ "reading data from: " ++ taskInputFilePath
  taskInput <- getInput inputMode
  logTrace $ show $ taskInput
  logDebug $ "calculating result"
  result <- return $ function taskInput
  logDebug $ "printing result"
  writeFile taskOutputFilePath $ concat $ intersperse "\n" result
  where
    getInput FileInput = do
      fileContents <- BLC.readFile taskInputFilePath
      logTrace $ show fileContents
      deserializeTaskInput fileContents
    getInput StreamInput = readStdTillEOF
