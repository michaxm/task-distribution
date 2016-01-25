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

import DataAccess.HdfsWriter (writeEntriesToHdfs)
import TaskSpawning.ExecutionUtil
import TaskSpawning.StreamToExecutableUtil
import Types.HdfsConfigTypes
import Types.TaskTypes -- TODO ugly to be referenced explicitely here - generalization possible?
import Util.Logging

data DataModes =
  DataModes InputMode OutputMode

data InputMode
  = FileInput
  | StreamInput

data OutputMode
  = FileOutput ZipOutput
  | HdfsOutput HdfsLocation ZipOutput
    --writeToHdfs $ writeEntriesToFile config (outputPrefix ++ "/" ++ (stripHDFSPartOfPath path)++outputSuffix)

packDataModes :: DataModes -> String
packDataModes (DataModes inputMode outputMode) = (packInputMode inputMode)++"|"++(packOutputMode outputMode)
unpackDataModes :: String -> DataModes
unpackDataModes s = let es = splitOn "|" s
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
packOutputMode (FileOutput z) = "FileOutput" ++ (if z then ":zipped" else "")
packOutputMode (HdfsOutput ((h, p), l) z) = "HdfsOutput:"++(if z then "zipped:" else "")++h++":"++(show p)++":"++l
unpackOutputMode :: String -> OutputMode
unpackOutputMode "FileOutput" = FileOutput False
unpackOutputMode "FileOutput:zipped" = FileOutput True
unpackOutputMode s = let es = splitOn ":" s
                     in case es of
                     ["FileOutput"] -> FileOutput False
                     ["FileOutput", "zipped"] -> FileOutput True
                     ["HdfsOutput", h, p, l] -> HdfsOutput ((h, (read p)), l) False
                     ["HdfsOutput", "zipped", h, p, l] -> HdfsOutput ((h, (read p)), l) True
                     _ -> error $ "unknown value: " ++ s

type ZipOutput = Bool

deployAndRunFullBinary :: DataModes -> String -> BL.ByteString -> TaskInput -> IO (FilePath, NominalDiffTime, NominalDiffTime)
deployAndRunFullBinary dataModes mainArg = deployAndRunExternalBinary dataModes [mainArg]

deployAndRunExternalBinary :: DataModes -> [String] -> BL.ByteString -> TaskInput -> IO (FilePath, NominalDiffTime, NominalDiffTime)
deployAndRunExternalBinary dataModes programBaseArgs program taskInput = do
  ((res, execDur), totalDur) <- measureDuration $ withTempBLFile "distributed-program" program $ runExternalBinary dataModes programBaseArgs taskInput
  return (res, (totalDur - execDur), execDur)

runExternalBinary :: DataModes -> [String] -> TaskInput -> FilePath -> IO (FilePath, NominalDiffTime)
runExternalBinary dataModes@(DataModes inputMode _) programBaseArgs taskInput filePath = do
  readProcessWithExitCode "chmod" ["+x", filePath] "" >>= expectSilentSuccess
  putStrLn $ "running " ++ filePath ++ "... "
  ((stdOut, outFilePath), execDur) <- measureDuration $ runExternalBinaryForInputMode inputMode (programBaseArgs++[packDataModes dataModes]) taskInput filePath
  putStrLn $ "... run completed"
  logDebug stdOut
  return (outFilePath, execDur)

runExternalBinaryForInputMode :: InputMode -> [String] -> TaskInput -> FilePath -> IO (String, FilePath)
runExternalBinaryForInputMode FileInput programBaseArgs taskInput filePath = do
  withTempBLCFile "distributed-program-data" (serializeTaskInput taskInput) (
    \taskInputFilePath -> do
      taskOutputFilePath <- return $ taskInputFilePath ++ ".out"
      -- note: although it seems a bit fishy, read/show serialization between ByteString and String seems to be working just fine for the serialized closure
      processOutput <- executeExternal filePath (programBaseArgs ++ [taskInputFilePath, taskOutputFilePath])
      return (processOutput, taskOutputFilePath)
    )
runExternalBinaryForInputMode StreamInput programBaseArgs taskInput filePath = do
  taskOutputFilePath <- createTempFilePath "distributed-program-task.out"
  res <- executeExternalWritingToStdIn filePath (programBaseArgs ++ ["", taskOutputFilePath]) taskInput
  return (res, taskOutputFilePath)

{-|
 Some methods parse the contents of stdout, thus these will fail in the case of logging to it (only ERROR at the moment).
|-}
fullBinaryExecution :: DataModes -> (TaskInput -> TaskResult) -> FilePath -> FilePath ->  IO ()
fullBinaryExecution (DataModes inputMode outputMode) function taskInputFilePath taskOutputFilePath = do
  logDebug $ "reading data from: " ++ taskInputFilePath
  taskInput <- getInput inputMode
  logTrace $ show $ taskInput
  logDebug $ "calculating result"
  result <- return $ function taskInput
  logTrace $ "printing result: " ++ show result
  writeData outputMode result
  where
    getInput FileInput = do
      fileContents <- BLC.readFile taskInputFilePath
      logTrace $ show fileContents
      deserializeTaskInput fileContents
    getInput StreamInput = readStdTillEOF
    writeData (FileOutput z) = (>> logInfo ("wrote result to file: "++taskOutputFilePath)) . BL.writeFile taskOutputFilePath . (if z then GZip.compress else id) . BLC.pack . concat . intersperse "\n"
    writeData (HdfsOutput l z) = uncurry (writeEntriesToHdfs z) l
