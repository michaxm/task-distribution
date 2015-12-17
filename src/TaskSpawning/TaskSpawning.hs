module TaskSpawning.TaskSpawning (
  processTask, RunStat,
  fullBinarySerializationOnMaster, executeFullBinaryArg, executionWithinWorkerProcessForFullBinaryDeployment,
  serializedThunkSerializationOnMaster, executeSerializedThunkArg, executionWithinWorkerProcessForThunkSerialization,
  objectCodeSerializationOnMaster) where

import qualified Data.ByteString.Lazy as BL
import Data.Time.Clock (NominalDiffTime)
import qualified Language.Haskell.Interpreter as I

import qualified DataAccess.DataSource as DS
import qualified DataAccess.SimpleDataSource as SDS
import qualified DataAccess.HdfsDataSource as HDS

import qualified TaskSpawning.BinaryStorage as RemoteStore
import qualified TaskSpawning.DeployFullBinary as DFB
import qualified TaskSpawning.DeploySerializedThunk as DST
import qualified TaskSpawning.ObjectCodeModuleDeployment as DOC
import TaskSpawning.ExecutionUtil (measureDuration)
import TaskSpawning.FunctionSerialization (serializeFunction, deserializeFunction)
import TaskSpawning.SourceCodeExecution (loadTask)
import TaskSpawning.TaskTypes
import Util.ErrorHandling
import Util.Logging

executeFullBinaryArg, executeSerializedThunkArg :: String
executeFullBinaryArg = "executefullbinary"
executeSerializedThunkArg = "executeserializedthunk"

type RunStat = (NominalDiffTime, NominalDiffTime, NominalDiffTime)

processTask :: TaskDef -> DataDef -> IO (TaskResult, RunStat)
processTask taskDef dataDef = do
-- TODO real logging  putStrLn "loading data"
  (taskInput, loadingDataDuration) <- measureDuration $ loadData dataDef
-- TODO real logging putStrLn "applying to task"
  (result, loadingTaskDuration, executionDuration)  <- applyTaskLogic taskDef taskInput
-- TODO real logging putStrLn "returning result"
  return (result, (loadingDataDuration, loadingTaskDuration, executionDuration))

applyTaskLogic :: TaskDef -> TaskInput -> IO (TaskResult, NominalDiffTime, NominalDiffTime)
applyTaskLogic (SourceCodeModule moduleName moduleContent) taskInput = do
  putStrLn "compiling task from source code"
  (taskFn, loadTaskDuration) <- measureDuration $ loadTask (I.as :: TaskInput -> TaskResult) moduleName moduleContent
  putStrLn "applying data"
  (result, execDuration) <- measureDuration $ return $ taskFn taskInput
  return (result, loadTaskDuration, execDuration)
-- Full binary deployment step 2/3: run within worker process to deploy the distributed task binary
applyTaskLogic (DeployFullBinary program) taskInput = DFB.deployAndRunFullBinary executeFullBinaryArg program taskInput
applyTaskLogic (PreparedDeployFullBinary hash) taskInput = do
  ((Just filePath), taskLoadDur) <- measureDuration $ RemoteStore.get hash --TODO catch unknown binary error nicer
  (res, execDur) <- DFB.runExternalBinary [executeFullBinaryArg] taskInput filePath
  return (res, taskLoadDur, execDur)
-- Serialized thunk deployment step 2/3: run within worker process to deploy the distributed task binary
applyTaskLogic (UnevaluatedThunk function program) taskInput = DST.deployAndRunSerializedThunk executeSerializedThunkArg function program taskInput
-- Partial binary deployment step 2/2: receive distribution on worker, prepare input data, link object file and spawn worker process, read its output
applyTaskLogic (ObjectCodeModule objectCode) taskInput = DOC.codeExecutionOnWorker objectCode taskInput

-- FIXME port not open (file not found?) error silently dropped
loadData :: DataDef -> IO TaskResult
loadData (HdfsData (config, filePath)) = DS._loadEntries (HDS.dataSource config) filePath -- TODO distinguish String/Read by overlapping instances or otherwise?
loadData (PseudoDB numDB) = DS._loadEntries SDS.stringSource ("resources/pseudo-db/" ++ (show numDB)) -- TODO make relative path configurable?

-- Full binary deployment step 1/3
fullBinarySerializationOnMaster :: FilePath -> IO TaskDef
fullBinarySerializationOnMaster programPath = BL.readFile programPath >>= return . DeployFullBinary

-- Serialized thunk deployment step 1/3: run within the client/master process to serialize itself.
serializedThunkSerializationOnMaster :: FilePath -> (TaskInput -> TaskResult) -> IO TaskDef
serializedThunkSerializationOnMaster programPath function = do
  program <- BL.readFile programPath -- TODO ByteString serialization should be contained within DST module
  taskFn <- serializeFunction function
  return $ UnevaluatedThunk taskFn program

-- Full binary deployment step 3/3: run within the spawned process for the distributed executable, applies data to distributed task.
executionWithinWorkerProcessForFullBinaryDeployment :: (TaskInput -> TaskResult) -> FilePath -> IO ()
executionWithinWorkerProcessForFullBinaryDeployment = DFB.fullBinaryExecution

-- Serialized thunk deployment step 3/3: run within the spawned process for the distributed executable, applies data to distributed task.
executionWithinWorkerProcessForThunkSerialization :: String -> FilePath -> IO ()
executionWithinWorkerProcessForThunkSerialization taskFnArg taskInputFilePath = do
  taskFn <- withErrorAction logError ("Could not read task logic: " ++(show taskFnArg)) $ return $ (read taskFnArg :: BL.ByteString)
  logInfo "worker: deserializing task logic"
  logDebug $ "worker: got this task function: " ++ (show taskFn)
  function <- deserializeFunction taskFn :: IO (TaskInput -> TaskResult)
  serializeFunction function >>= \s -> logDebug $ "task deserialization done for: " ++ (show $ BL.unpack s)
  DST.serializedThunkExecution function taskInputFilePath

-- Partial binary deployment step 1/2: start distribution of task on master
objectCodeSerializationOnMaster :: IO TaskDef
objectCodeSerializationOnMaster = DOC.loadObjectCode >>= \objectCode -> return $ ObjectCodeModule objectCode
