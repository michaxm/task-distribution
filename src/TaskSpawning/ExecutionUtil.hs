module TaskSpawning.ExecutionUtil where

import Control.Exception.Base (bracket)
-- FIXME really lazy? rather use strict???
import qualified Data.ByteString.Char8 as BLC
import qualified Data.ByteString.Lazy as BL
import Data.List (intersperse)
import Data.Time.Clock (diffUTCTime, NominalDiffTime, getCurrentTime)
import System.Directory (removeFile)
import System.Environment (lookupEnv, setEnv, unsetEnv)
import System.Exit (ExitCode(..))
import System.FilePath ()
import System.IO.Error (catchIOError)
import System.IO.Temp (withSystemTempFile)
import System.Process (readProcessWithExitCode)

import TaskSpawning.TaskTypes (TaskInput, TaskResult)
import Util.ErrorHandling
import Util.Logging

withEnv :: String -> String -> IO a -> IO a
withEnv key value action = do
  old <- lookupEnv key
  setEnv key value
  res <- action
  maybe (unsetEnv key) (\o -> setEnv key o) old
  return res

withTempBLFile :: FilePath -> BL.ByteString -> (FilePath -> IO result) -> IO result
withTempBLFile = withTempFile BL.writeFile

withTempBLCFile :: FilePath -> BLC.ByteString -> (FilePath -> IO result) -> IO result
withTempBLCFile = withTempFile BLC.writeFile

withTempFile :: (FilePath -> dataType -> IO ()) -> FilePath -> dataType -> (FilePath -> IO result) -> IO result
withTempFile writer filePathTemplate fileContent =
  bracket
    (do
      filePath <- createTempFilePath filePathTemplate
      writer filePath fileContent
      return filePath)
    (\filePath -> ignoreIOExceptions $ removeFile filePath)

ignoreIOExceptions :: IO () -> IO ()
ignoreIOExceptions io = io `catchIOError` (\_ -> return ())

expectSilentSuccess :: (ExitCode, String, String) -> IO ()
expectSilentSuccess executionOutput= expectSuccess executionOutput >>= \res -> case res of
  "" -> return ()
  _ -> error $ "no output expected, but got: " ++ res

expectSuccess :: (ExitCode, String, String) -> IO String
expectSuccess (ExitSuccess, result, []) = return result
expectSuccess output = error $ "command exited with unexpected status: " ++ (show output)

createTempFilePath :: String -> IO FilePath
createTempFilePath template = do
  -- a bit hackish, we only care about the generated file name, we have to do the file handler handling ourselves ...
  withSystemTempFile template (\ f _ -> return f)

serializeTaskInput :: TaskInput -> BLC.ByteString
serializeTaskInput = BLC.pack . show

deserializeTaskInput :: BLC.ByteString -> IO TaskInput
deserializeTaskInput s = withErrorAction logError "Could not read input data" $ return $ read $ BLC.unpack s

-- TODO move out of here
parseResult :: String -> IO TaskResult
parseResult s = withErrorPrefix ("Cannot parse result: "++s) $ return $! (lines s :: TaskResult)

executeExternal :: FilePath -> [String] -> IO String
executeExternal executable args = do
  logInfo $ "executing: " ++ executable ++ " " ++ (concat $ intersperse " " args)
  result <- withErrorAction logError ("Could not run [" ++ (show executable) ++ "] successfully: ") (readProcessWithExitCode executable args "")
  expectSuccess result

measureDuration :: IO a -> IO (a, NominalDiffTime)
measureDuration action = do
  before <- getCurrentTime
  res <- action -- TODO eager enough?
  after <- getCurrentTime
  return (res, diffUTCTime after before)
