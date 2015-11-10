module TaskSpawning.TaskSpawning (processTask, executeFullBinaryArg, fullDeploymentSerialize, fullDeploymentExecute) where

import qualified Data.ByteString.Lazy as BL
import qualified Language.Haskell.Interpreter as I

import qualified DataAccess.DataSource as DS
import qualified DataAccess.SimpleDataSource as SDS
import qualified DataAccess.HdfsDataSource as HDS

import TaskSpawning.DeployCompleteProgram
import TaskSpawning.FunctionSerialization (serializeFunction, deserializeFunction)
import TaskSpawning.SourceCodeExecution (loadTask)
import TaskSpawning.TaskTypes

executeFullBinaryArg :: String
executeFullBinaryArg = "executefullbinary"

{-
 The process for collecting and executing the needed bits is slightly different for
 different task types:

 - source code execution via hint
   ... is happy to  TODO continue...
-}
processTask :: TaskDef -> DataDef -> IO TaskResult
processTask taskDef dataDef = do
  putStrLn "loading data"
  taskInput <- loadData dataDef
  putStrLn "applying to task"
  result <- applyTaskLogic taskDef taskInput
  putStrLn "returning result"
  return result

applyTaskLogic :: TaskDef -> TaskInput -> IO TaskResult
applyTaskLogic (SourceCodeModule moduleName moduleContent) taskInput = do
  putStrLn "compiling task from source code"
  taskFn <- loadTask (I.as :: TaskInput -> TaskResult) moduleName moduleContent
  putStrLn "applying data"
  return $ taskFn taskInput
-- Full binary deployment step 2: run within worker process to deploy the distributed task binary
applyTaskLogic (UnevaluatedThunk function program) taskInput = deployAndRunFullBinary program function executeFullBinaryArg taskInput

-- FIXME port not open (file not found?) error silently dropped
loadData :: DataDef -> IO TaskResult
loadData (HdfsData config filePath) = DS._loadEntries (HDS.dataSource config) filePath -- TODO distinguish String/Read by overlapping instances or otherwise?
loadData (PseudoDB numDB) = DS._loadEntries SDS.stringSource ("/home/axm/projects/thesis-distributed-calculation/cluster-computing/resources/pseudo-db/" ++ (show numDB)) -- TODO make simple usable for all (for basic example)

{-
 Full binary deployment step 1: run within the client/master process to serialize itself.
-}
fullDeploymentSerialize :: FilePath -> (TaskInput -> TaskResult) -> IO TaskDef
fullDeploymentSerialize programPath function = do
  program <- BL.readFile programPath
  taskFn <- serializeFunction function
  return $ UnevaluatedThunk taskFn program

{-
 Full binary deployment step 3: run within the spawned process for the distributed executable, applies data to distributed task.
-}
fullDeploymentExecute :: BL.ByteString -> TaskInput -> IO ()
fullDeploymentExecute taskFn taskInput = do
  --TODO real logging
--  putStrLn $ "deserializing task logic" ++ (show taskFn)
  function <- deserializeFunction taskFn :: IO (TaskInput -> TaskResult)
--  putStrLn "calculating result"
  result <- return $ function taskInput
--  putStrLn "printing result"
  print result
