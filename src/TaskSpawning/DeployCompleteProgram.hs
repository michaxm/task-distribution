module TaskSpawning.DeployCompleteProgram (deployAndRun) where

-- FIXME really lazy? rather use strict???
import qualified Data.ByteString.Lazy as BL
import System.Directory (removeFile)
import System.Exit (ExitCode(..))
import System.FilePath ()
import System.Process (readProcessWithExitCode)
import System.IO.Temp (withSystemTempFile)

import TaskSpawning.TaskTypes -- TODO ugly - generalization possible?

-- this code doesn't care about exception handling - what fails, fails
deployAndRun :: BL.ByteString -> IO (TaskInput -> TaskResult)
deployAndRun program = do
  filepath <- createTempFilePath "distributed-prog"
  BL.writeFile filepath program
  readProcessWithExitCode "chmod" ["+x", filepath] "" >>= expectSilentSuccess
  (code, stdout, stderr) <- readProcessWithExitCode filepath [] ""
  removeFile filepath
  print (code, stdout, stderr)
  return $ map (++ " dummy append")

expectSilentSuccess :: (ExitCode, String, String) -> IO ()
expectSilentSuccess (ExitSuccess, [], []) = return ()
expectSilentSuccess output = error $ "command exited with unexpected status: " ++ (show output)

createTempFilePath :: String -> IO FilePath
createTempFilePath template = do
  -- a bit hackish, we only care about the generated file name, we have to do the file handler handling ourselves ...
  withSystemTempFile template (\ f _ -> return f)
  
