module TaskSpawning.TaskSpawning (
  processTask, RunStat,
  fullBinarySerializationOnMaster, executeFullBinaryArg, executionWithinSlaveProcessForFullBinaryDeployment,
  serializedThunkSerializationOnMaster, executeSerializedThunkArg, executionWithinSlaveProcessForThunkSerialization,
  objectCodeSerializationOnMaster) where

import qualified Data.ByteString.Lazy as BL
import Data.Time.Clock (NominalDiffTime)
import qualified Language.Haskell.Interpreter as I

import qualified DataAccess.SimpleDataSource as SDS
import qualified DataAccess.HdfsDataSource as HDS

import qualified TaskSpawning.BinaryStorage as RemoteStore
import qualified TaskSpawning.DeployFullBinary as DFB
import qualified TaskSpawning.DeploySerializedThunk as DST
import qualified TaskSpawning.ObjectCodeModuleDeployment as DOC
import TaskSpawning.ExecutionUtil (measureDuration)
import TaskSpawning.FunctionSerialization (serializeFunction, deserializeFunction)
import TaskSpawning.SourceCodeExecution (loadTask)
import TaskSpawning.TaskDefinition
import TaskSpawning.TaskDescription
import Types.TaskTypes
import Util.ErrorHandling
import Util.Logging

executeFullBinaryArg, executeSerializedThunkArg :: String
executeFullBinaryArg = "executefullbinary"
executeSerializedThunkArg = "executeserializedthunk"

type RunStat = (NominalDiffTime, NominalDiffTime, NominalDiffTime)

{-|
  Apply the task on the data, producing either a location, where the results are stored, or the results directly.
  Which of those depends on the distribution type, when external programs are spawned the former can be more efficient,
  but if there is no such intermediate step, a direct result is better.
|-}
processTask :: TaskDef -> DataDef -> IO (Either FilePath TaskResult, RunStat)
processTask taskDef dataDef = do
  logDebug $ "loading data for" ++ (describe dataDef)
  (taskInput, loadingDataDuration) <- measureDuration $ loadData dataDef
  logDebug $ "applying data to task: " ++ (describe taskDef)
  (result, loadingTaskDuration, executionDuration)  <- applyTaskLogic taskDef taskInput
  logDebug $ "returning result for " ++ (describe dataDef)
  return (result, (loadingDataDuration, loadingTaskDuration, executionDuration))

applyTaskLogic :: TaskDef -> TaskInput -> IO (Either FilePath TaskResult, NominalDiffTime, NominalDiffTime)
applyTaskLogic (SourceCodeModule moduleName moduleContent) taskInput = do
  putStrLn "compiling task from source code"
  (taskFn, loadTaskDuration) <- measureDuration $ loadTask (I.as :: TaskInput -> TaskResult) moduleName moduleContent
  putStrLn "applying data"
  (result, execDuration) <- measureDuration $ return $ taskFn taskInput
  return (result, loadTaskDuration, execDuration) >>= return . (onFirst Right)
-- Full binary deployment step 2/3: run within slave process to deploy the distributed task binary
applyTaskLogic (DeployFullBinary program inputMode) taskInput = DFB.deployAndRunFullBinary (convertInputMode inputMode) executeFullBinaryArg program taskInput >>= return . (onFirst Left)
applyTaskLogic (PreparedDeployFullBinary hash inputMode) taskInput = do
  ((Just filePath), taskLoadDur) <- measureDuration $ RemoteStore.get hash --TODO catch unknown binary error nicer
  (res, execDur) <- DFB.runExternalBinary (convertInputMode inputMode) [executeFullBinaryArg] taskInput filePath
  return (res, taskLoadDur, execDur) >>= return . (onFirst Left)
-- Serialized thunk deployment step 2/3: run within slave process to deploy the distributed task binary
applyTaskLogic (UnevaluatedThunk function program) taskInput = DST.deployAndRunSerializedThunk executeSerializedThunkArg function program taskInput >>= return . (onFirst Left)
-- Partial binary deployment step 2/2: receive distribution on slave, prepare input data, link object file and spawn slave process, read its output
applyTaskLogic (ObjectCodeModule objectCode) taskInput = DOC.codeExecutionOnSlave objectCode taskInput >>= return . (onFirst Right) -- TODO switch to location ("Left")

convertInputMode :: TaskInputMode -> DFB.InputMode
convertInputMode FileInput = DFB.FileInput
convertInputMode StreamInput = DFB.StreamInput

onFirst :: (a -> a') -> (a, b, c) -> (a', b, c)
onFirst f (a, b, c) = (f a, b, c)

-- FIXME port not open (file not found?) error silently dropped
loadData :: DataDef -> IO TaskResult
loadData (HdfsData hdfsLocation) = HDS.loadEntries hdfsLocation
loadData (PseudoDB numDB) = SDS.loadEntries ("resources/pseudo-db/" ++ (show numDB)) -- TODO make relative path configurable?

-- Full binary deployment step 1/3
fullBinarySerializationOnMaster :: TaskInputMode -> FilePath -> IO TaskDef
fullBinarySerializationOnMaster inputMode programPath = do
  currentExecutable <- BL.readFile programPath
  return $ DeployFullBinary currentExecutable inputMode

-- Serialized thunk deployment step 1/3: run within the client/master process to serialize itself.
serializedThunkSerializationOnMaster :: FilePath -> (TaskInput -> TaskResult) -> IO TaskDef
serializedThunkSerializationOnMaster programPath function = do
  program <- BL.readFile programPath -- TODO ByteString serialization should be contained within DST module
  taskFn <- serializeFunction function
  return $ UnevaluatedThunk taskFn program

-- Full binary deployment step 3/3: run within the spawned process for the distributed executable, applies data to distributed task.
executionWithinSlaveProcessForFullBinaryDeployment :: DFB.InputMode -> (TaskInput -> TaskResult) -> FilePath -> FilePath -> IO ()
executionWithinSlaveProcessForFullBinaryDeployment = DFB.fullBinaryExecution

-- Serialized thunk deployment step 3/3: run within the spawned process for the distributed executable, applies data to distributed task.
executionWithinSlaveProcessForThunkSerialization :: String -> FilePath -> FilePath -> IO ()
executionWithinSlaveProcessForThunkSerialization taskFnArg taskInputFilePath taskOutputFilePath = do
  taskFn <- withErrorAction logError ("Could not read task logic: " ++(show taskFnArg)) $ return $ (read taskFnArg :: BL.ByteString)
  logInfo "slave: deserializing task logic"
  logDebug $ "slave: got this task function: " ++ (show taskFn)
  function <- deserializeFunction taskFn :: IO (TaskInput -> TaskResult)
  serializeFunction function >>= \s -> logDebug $ "task deserialization done for: " ++ (show $ BL.unpack s)
  DST.serializedThunkExecution function taskInputFilePath taskOutputFilePath

-- Partial binary deployment step 1/2: start distribution of task on master
objectCodeSerializationOnMaster :: IO TaskDef
objectCodeSerializationOnMaster = DOC.loadObjectCode >>= \objectCode -> return $ ObjectCodeModule objectCode
