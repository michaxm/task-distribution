module TaskSpawning.TaskSpawning (processTask, executeFullBinaryArg, fullDeploymentSerialize, fullDeploymentExecute) where

import qualified Data.ByteString.Lazy as BL
import qualified Language.Haskell.Interpreter as I

import qualified DataAccess.DataSource as DS
import qualified DataAccess.SimpleDataSource as SDS
import qualified DataAccess.HdfsDataSource as HDS

import qualified TaskSpawning.DeployCompleteProgram as CP
import TaskSpawning.FunctionSerialization (serializeFunction, deserializeFunction)
import TaskSpawning.SourceCodeExecution (loadTask)
import TaskSpawning.TaskTypes
import Util.ErrorHandling

executeFullBinaryArg :: String
executeFullBinaryArg = "executefullbinary"

processTask :: TaskDef -> DataDef -> IO TaskResult
processTask taskDef dataDef = do
-- TODO real logging  putStrLn "loading data"
  taskInput <- loadData dataDef
-- TODO real logging putStrLn "applying to task"
  result <- applyTaskLogic taskDef taskInput
-- TODO real logging putStrLn "returning result"
  return result

applyTaskLogic :: TaskDef -> TaskInput -> IO TaskResult
applyTaskLogic (SourceCodeModule moduleName moduleContent) taskInput = do
  putStrLn "compiling task from source code"
  taskFn <- loadTask (I.as :: TaskInput -> TaskResult) moduleName moduleContent
  putStrLn "applying data"
  return $ taskFn taskInput
-- Full binary deployment step 2: run within worker process to deploy the distributed task binary
applyTaskLogic (UnevaluatedThunk function program) taskInput = CP.deployAndRunFullBinary program function executeFullBinaryArg taskInput

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
fullDeploymentExecute :: String -> FilePath -> IO ()
fullDeploymentExecute taskFnArg taskInputFilePath = do
  -- TODO real logging
  taskFn <- addErrorPrefix ("Could not read task logic: " ++(show taskFnArg)) $ return $ (read taskFnArg :: BL.ByteString)
--  putStrLn $ "deserializing task logic" -- ++ (show taskFn)
  function <- deserializeFunction taskFn :: IO (TaskInput -> TaskResult)
--  serializeFunction function >>= \s -> putStrLn $ "task deserialization done for: " ++ (show $ BL.unpack s)
--  putStrLn $ "reading data from " ++ taskInputFilePath
  CP.binaryExecution function taskInputFilePath
