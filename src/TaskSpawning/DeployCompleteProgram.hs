module TaskSpawning.DeployCompleteProgram (executeFullBinary, deployAndRunFullBinary, binaryExecution) where

-- FIXME really lazy? rather use strict???
import qualified Data.ByteString.Char8 as BLC
import qualified Data.ByteString.Lazy as BL
import System.FilePath ()
import System.Process (readProcessWithExitCode)

import TaskSpawning.ExecutionUtil
import TaskSpawning.FunctionSerialization (deserializeFunction)
import TaskSpawning.TaskTypes -- TODO ugly to be referenced explicitely here - generalization possible?

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
      executionOutput <- withTempBLCFile "distributed-program-data" (serializeTaskInput taskInput) (
        \taskInputFilePath -> do
            -- note: although it seems a bit fishy, read/show serialization between ByteString and String seems to be working just fine for the serialized closure
            readProcessWithExitCode filePath [mainArg, show taskFunction, taskInputFilePath] "")
      putStrLn $ "... run completed" -- TODO trace logging ++ (show executionOutput)
      result <- expectSuccess executionOutput
      parseResult result
    )

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
  taskInput <- deserializeTaskInput fileContents
--  print taskInput
--  putStrLn "calculating result"
  result <- return $ function taskInput
--  putStrLn "printing result"
  print result
