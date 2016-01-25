module TaskSpawning.DeployFullBinary (
  deployAndRunFullBinary, deployAndRunExternalBinary, fullBinaryExecution, runExternalBinary,
  unpackDataModes, DataModes(..), InputMode(..), OutputMode(..), ZipOutput) where

import qualified Codec.Compression.GZip as GZip
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.ByteString.Lazy as BL
import Data.List (intersperse)
import Data.List.Split (splitOn)
import Data.Time.Clock (NominalDiffTime)
import System.FilePath ()
import System.Process (readProcessWithExitCode)

import TaskSpawning.ExecutionUtil
import TaskSpawning.StreamToExecutableUtil
import Types.TaskTypes -- TODO ugly to be referenced explicitely here - generalization possible?
import Util.Logging

data DataModes =
  DataModes InputMode OutputMode

data InputMode
  = FileInput
  | StreamInput

data OutputMode
  = FileOutput ZipOutput

packDataModes :: DataModes -> String
packDataModes (DataModes inputMode outputMode) = (packInputMode inputMode)++":"++(packOutputMode outputMode)
unpackDataModes :: String -> DataModes
unpackDataModes s = let es = splitOn ":" s
                    in if (length es) /= 2
                       then error $ "unknown value: " ++ s
                       else DataModes (unpackInputMode $ es !! 0) (unpackOutputMode $ es !! 1)

packInputMode :: InputMode -> String
packInputMode FileInput = "FileInput"
packInputMode StreamInput = "StreamInput"
unpackInputMode :: String -> InputMode
unpackInputMode "FileInput" = FileInput
unpackInputMode "StreamInput" = StreamInput
unpackInputMode s = error $ "unknown value: " ++ s
packOutputMode :: OutputMode -> String
packOutputMode (FileOutput z) = "FileOutput" ++ (if z then "-zipped" else "")
unpackOutputMode :: String -> OutputMode
unpackOutputMode "FileOutput" = FileOutput False
unpackOutputMode "FileOutput-zipped" = FileOutput True
unpackOutputMode s = error $ "unknown value: " ++ s

type ZipOutput = Bool

deployAndRunFullBinary :: DataModes -> String -> BL.ByteString -> TaskInput -> IO (FilePath, NominalDiffTime, NominalDiffTime)
deployAndRunFullBinary dataModes mainArg = deployAndRunExternalBinary dataModes [mainArg]

deployAndRunExternalBinary :: DataModes -> [String] -> BL.ByteString -> TaskInput -> IO (FilePath, NominalDiffTime, NominalDiffTime)
deployAndRunExternalBinary dataModes programBaseArgs program taskInput = do
  ((res, execDur), totalDur) <- measureDuration $ withTempBLFile "distributed-program" program $ runExternalBinary dataModes programBaseArgs taskInput
  return (res, (totalDur - execDur), execDur)

runExternalBinary :: DataModes -> [String] -> TaskInput -> FilePath -> IO (FilePath, NominalDiffTime)
runExternalBinary dataModes@(DataModes inputMode outputMode) programBaseArgs taskInput filePath = do
  readProcessWithExitCode "chmod" ["+x", filePath] "" >>= expectSilentSuccess
  putStrLn $ "running " ++ filePath ++ "... "
  ((stdOut, outFilePath), execDur) <- measureDuration $ runExternalBinaryForMode inputMode outputMode (programBaseArgs++[packDataModes dataModes]) taskInput filePath
  putStrLn $ "... run completed"
  logDebug stdOut
  return (outFilePath, execDur)

runExternalBinaryForMode :: InputMode -> OutputMode -> [String] -> TaskInput -> FilePath -> IO (String, FilePath)
runExternalBinaryForMode FileInput (FileOutput _) programBaseArgs taskInput filePath = do
  withTempBLCFile "distributed-program-data" (serializeTaskInput taskInput) (
    \taskInputFilePath -> do
      taskOutputFilePath <- return $ taskInputFilePath ++ ".out"
      -- note: although it seems a bit fishy, read/show serialization between ByteString and String seems to be working just fine for the serialized closure
      processOutput <- executeExternal filePath (programBaseArgs ++ [taskInputFilePath, taskOutputFilePath])
      return (processOutput, taskOutputFilePath)
    )
runExternalBinaryForMode StreamInput (FileOutput _) programBaseArgs taskInput filePath = do
  taskOutputFilePath <- createTempFilePath "distributed-program-task.out"
  res <- executeExternalWritingToStdIn filePath (programBaseArgs ++ ["", taskOutputFilePath]) taskInput
  return (res, taskOutputFilePath)

fullBinaryExecution :: DataModes -> (TaskInput -> TaskResult) -> FilePath -> FilePath ->  IO ()
fullBinaryExecution (DataModes inputMode (FileOutput zipOutput)) function taskInputFilePath taskOutputFilePath = do
  --TODO real logging: may not be written to stdout, since that is what we consume as result
  logDebug $ "reading data from: " ++ taskInputFilePath
  taskInput <- getInput inputMode
  logTrace $ show $ taskInput
  logDebug $ "calculating result"
  result <- return $ function taskInput
  logDebug $ "printing result"
  let output = BLC.pack $ concat $ intersperse "\n" result
    in let fileOutput = if zipOutput then GZip.compress output else output
       in BL.writeFile taskOutputFilePath fileOutput
  where
    getInput FileInput = do
      fileContents <- BLC.readFile taskInputFilePath
      logTrace $ show fileContents
      deserializeTaskInput fileContents
    getInput StreamInput = readStdTillEOF
