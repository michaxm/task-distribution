module TaskSpawning.DeployCompleteProgram (deployAndRunFullBinary, executeFullBinary) where

import Control.Exception.Base (bracket)
-- FIXME really lazy? rather use strict???
import qualified Data.ByteString.Lazy as BL
import System.Directory (removeFile)
import System.Exit (ExitCode(..))
import System.FilePath ()
import System.Process (readProcessWithExitCode)
import System.IO.Error (catchIOError)
import System.IO.Temp (withSystemTempFile)

import TaskSpawning.FunctionSerialization (deserializeFunction)
import TaskSpawning.TaskTypes -- TODO ugly to be referenced explicitely here - generalization possible?

{-
 Deploys a given binary and executes it with the arguments defined by convention, including the serialized closure runnable in that program.

 The execution does not include error handling, this should be done on master/client.
-}
deployAndRunFullBinary :: BL.ByteString -> BL.ByteString -> String -> TaskInput -> IO TaskResult
deployAndRunFullBinary program taskFunction mainArg taskInput =
  withTempFile "distributed-program" program (
    \filePath -> do
      readProcessWithExitCode "chmod" ["+x", filePath] "" >>= expectSilentSuccess
      putStrLn $ "running " ++ filePath
      executionOutput <- readProcessWithExitCode filePath [mainArg, show taskFunction, show taskInput] "" -- FIXME string/bytestring conversion
      result <- expectSuccess executionOutput
      return $ read result
    )

withTempFile :: FilePath -> BL.ByteString -> (FilePath -> IO a) -> IO a
withTempFile filePathTemplate fileContent =
  bracket
    (do
      filePath <- createTempFilePath filePathTemplate
      BL.writeFile filePath fileContent
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
executeFullBinary :: BL.ByteString -> IO (TaskInput -> TaskResult)
executeFullBinary taskFn = deserializeFunction taskFn :: IO (TaskInput -> TaskResult)
