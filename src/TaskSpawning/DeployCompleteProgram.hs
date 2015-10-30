module TaskSpawning.DeployCompleteProgram (deployAndRun) where

import Control.Exception.Base (bracket)
--import qualified Data.ByteString.Char8 as BC (unpack)
-- FIXME really lazy? rather use strict???
import qualified Data.ByteString.Lazy as BL
import System.Directory (removeFile)
import System.Exit (ExitCode(..))
import System.FilePath ()
import System.Process (readProcessWithExitCode)
import System.IO.Error (catchIOError)
import System.IO.Temp (withSystemTempFile)

import TaskSpawning.TaskTypes -- TODO ugly - generalization possible?

-- this code doesn't care about exception handling - what fails, fails
deployAndRun :: BL.ByteString -> BL.ByteString -> IO (TaskInput -> TaskResult)
deployAndRun taskFunction program =
  withTempFile "distributed-prod" program (
    \filePath -> do
      readProcessWithExitCode "chmod" ["+x", filePath] "" >>= expectSilentSuccess
      putStrLn $ "running " ++ filePath
      (code, result, stderr) <- readProcessWithExitCode filePath ["executebinary", show taskFunction] "" -- FIXME string/bytestring conversion
      print (code, result, stderr) -- TODO remove debug code
      return $ (\_ -> read result) -- TODO dummy "evaluation"
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
expectSilentSuccess (ExitSuccess, [], []) = return ()
expectSilentSuccess output = error $ "command exited with unexpected status: " ++ (show output)

createTempFilePath :: String -> IO FilePath
createTempFilePath template = do
  -- a bit hackish, we only care about the generated file name, we have to do the file handler handling ourselves ...
  withSystemTempFile template (\ f _ -> return f)
  
