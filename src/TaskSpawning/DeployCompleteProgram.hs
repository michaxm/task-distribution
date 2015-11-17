module TaskSpawning.DeployCompleteProgram (executeFullBinary, deployAndRunFullBinary, binaryExecution) where

import Control.Exception.Base (bracket)
-- FIXME really lazy? rather use strict???
import qualified Data.ByteString.Char8 as BLC
import qualified Data.ByteString.Lazy as BL
import System.Directory (removeFile)
import System.Exit (ExitCode(..))
import System.FilePath ()
import System.Process (readProcessWithExitCode)
import System.IO.Error (catchIOError)
import System.IO.Temp (withSystemTempFile)

import TaskSpawning.FunctionSerialization (deserializeFunction)
import TaskSpawning.TaskTypes -- TODO ugly to be referenced explicitely here - generalization possible?
import Util.ErrorHandling

{-
 Deploys a given binary and executes it with the arguments defined by convention, including the serialized closure runnable in that program.

 The execution does not include error handling, this should be done on master/client.
-}
deployAndRunFullBinary :: BL.ByteString -> BL.ByteString -> String -> TaskInput -> IO TaskResult
deployAndRunFullBinary program taskFunction mainArg taskInput =
  withTempBLFile "distributed-program" program (
    \filePath -> do
      readProcessWithExitCode "chmod" ["+x", filePath] "" >>= expectSilentSuccess
      putStrLn $ "running " ++ filePath ++ "... "
      executionOutput <- withTempBLCFile "distributed-program-data" (toByteString taskInput) (
        \taskInputFilePath -> do
            -- note: although it seems a bit fishy, read/show serialization between ByteString and String seems to be working just fine for the serialized closure
            readProcessWithExitCode filePath [mainArg, show taskFunction, taskInputFilePath] "")
      putStrLn $ "... run completed" -- TODO trace logging ++ (show executionOutput)
      result <- expectSuccess executionOutput
      parseResult result
    )
  where
    parseResult :: String -> IO TaskResult
    parseResult s = addErrorPrefix ("Cannot parse result: "++s) $ return (read s :: TaskResult)

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
  
{-
 Accepts the distributed, serialized closure as part of the spawned program and executes it.

 Parameter handling is done via simple serialization.
-}
--TODO refactor module borders to have fully unterstandable resonsibilities, shis should go up to have nothing to do with serialization of thunks?
executeFullBinary :: BL.ByteString -> IO (TaskInput -> TaskResult)
executeFullBinary taskFn = (deserializeFunction taskFn :: IO (TaskInput -> TaskResult)) >>= (\f -> return (take 10 . f))

binaryExecution :: (TaskInput -> TaskResult) -> FilePath -> IO ()
binaryExecution function taskInputFilePath = do
  --TODO real logging
--  putStrLn $ "reading data from: " ++ taskInputFilePath
  fileContents <- BLC.readFile taskInputFilePath
--  print fileContents
  taskInput <- fromByteString fileContents
--  print taskInput
--  putStrLn "calculating result"
  result <- return $ function taskInput
--  putStrLn "printing result"
  print result

toByteString :: TaskInput -> BLC.ByteString
toByteString = BLC.pack . show

fromByteString :: BLC.ByteString -> IO TaskInput
fromByteString s = addErrorPrefix "Could not read input data" $ return $ read $ BLC.unpack s
