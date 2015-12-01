module TaskSpawning.TaskSpawning (
  processTask,
  fullDeploymentExecutionRemote, fullExecutionWithinWorkerProcess, executeFullBinaryArg,
  objectCodeExecutionRemote) where

import qualified Data.ByteString.Lazy as BL
import qualified Language.Haskell.Interpreter as I

import qualified DataAccess.DataSource as DS
import qualified DataAccess.SimpleDataSource as SDS
import qualified DataAccess.HdfsDataSource as HDS

import qualified TaskSpawning.DeployCompleteProgram as CP
import qualified TaskSpawning.ObjectCodeModuleDeployment as OC
import TaskSpawning.FunctionSerialization (serializeFunction, deserializeFunction)
import TaskSpawning.SourceCodeExecution (loadTask)
import TaskSpawning.TaskTypes
import Util.ErrorHandling
import Util.Logging

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
-- Full binary deployment step 2/3: run within worker process to deploy the distributed task binary
applyTaskLogic (UnevaluatedThunk function program) taskInput = CP.deployAndRunFullBinary program function executeFullBinaryArg taskInput
-- Partial binary deployment step 2/2: receive distribution on worker, prepare input data, link object file and spawn worker process, read its output
applyTaskLogic (ObjectCodeModule objectCode) taskInput = OC.codeExecutionOnWorker objectCode taskInput

-- FIXME port not open (file not found?) error silently dropped
loadData :: DataDef -> IO TaskResult
loadData (HdfsData (config, filePath)) = DS._loadEntries (HDS.dataSource config) filePath -- TODO distinguish String/Read by overlapping instances or otherwise?
loadData (PseudoDB numDB) = DS._loadEntries SDS.stringSource ("/home/axm/projects/thesis-distributed-calculation/cluster-computing/resources/pseudo-db/" ++ (show numDB)) -- TODO make simple usable for all (for basic example)

-- Full binary deployment step 1/3: run within the client/master process to serialize itself.
fullDeploymentExecutionRemote :: FilePath -> (TaskInput -> TaskResult) -> IO TaskDef
fullDeploymentExecutionRemote programPath function = do
  program <- BL.readFile programPath -- TODO ByteString serialization should be contained within CP module
  taskFn <- serializeFunction function
  return $ UnevaluatedThunk taskFn program

-- Full binary deployment step 3/3: run within the spawned process for the distributed executable, applies data to distributed task.
fullExecutionWithinWorkerProcess :: String -> FilePath -> IO ()
fullExecutionWithinWorkerProcess taskFnArg taskInputFilePath = do
  taskFn <- withErrorAction logError ("Could not read task logic: " ++(show taskFnArg)) $ return $ (read taskFnArg :: BL.ByteString)
  logInfo "deserializing task logic"
  logDebug $ "got this task function: " ++ (show taskFn)
  function <- deserializeFunction taskFn :: IO (TaskInput -> TaskResult)
  serializeFunction function >>= \s -> logDebug $ "task deserialization done for: " ++ (show $ BL.unpack s)
  CP.binaryExecution function taskInputFilePath

-- Partial binary deployment step 1/2: start distribution of task on master
objectCodeExecutionRemote :: IO TaskDef
objectCodeExecutionRemote = OC.loadObjectCode >>= \objectCode -> return $ ObjectCodeModule objectCode
