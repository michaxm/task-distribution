module Control.Distributed.Task.TaskSpawning.TaskSpawning (
  processTask, RunStat, TaskResultWrapper(..),
  fullBinarySerializationOnMaster, executeFullBinaryArg, executionWithinSlaveProcessForFullBinaryDeployment,
  serializedThunkSerializationOnMaster, executeSerializedThunkArg, executionWithinSlaveProcessForThunkSerialization,
  objectCodeSerializationOnMaster) where

import qualified Data.ByteString.Lazy as BL
import Data.List (isSuffixOf)
import Data.Time.Clock (NominalDiffTime)
import qualified Language.Haskell.Interpreter as I

import qualified Control.Distributed.Task.DataAccess.SimpleDataSource as SDS
import qualified Control.Distributed.Task.DataAccess.HdfsDataSource as HDS
import qualified Control.Distributed.Task.TaskSpawning.BinaryStorage as RemoteStore
import qualified Control.Distributed.Task.TaskSpawning.DeployFullBinary as DFB
import qualified Control.Distributed.Task.TaskSpawning.DeploySerializedThunk as DST
import qualified Control.Distributed.Task.TaskSpawning.ObjectCodeModuleDeployment as DOC
import Control.Distributed.Task.TaskSpawning.ExecutionUtil (measureDuration)
import Control.Distributed.Task.TaskSpawning.FunctionSerialization (serializeFunction, deserializeFunction)
import Control.Distributed.Task.TaskSpawning.SourceCodeExecution (loadTask)
import Control.Distributed.Task.TaskSpawning.TaskDefinition
import Control.Distributed.Task.TaskSpawning.TaskDescription
import Control.Distributed.Task.Types.TaskTypes
import Control.Distributed.Task.Util.Configuration
import Control.Distributed.Task.Util.ErrorHandling
import Control.Distributed.Task.Util.Logging

executeFullBinaryArg, executeSerializedThunkArg :: String
executeFullBinaryArg = "executefullbinary"
executeSerializedThunkArg = "executeserializedthunk"

data TaskResultWrapper = DirectResult TaskResult | ReadFromFile FilePath | Empty
type RunStat = (NominalDiffTime, NominalDiffTime, NominalDiffTime)

{-|
  Apply the task on the data, producing either a location, where the results are stored, or the results directly.
  Which of those depends on the distribution type, when external programs are spawned the former can be more efficient,
  but if there is no such intermediate step, a direct result is better.
|-}
processTask :: TaskDef -> DataDef -> ResultDef -> IO (TaskResultWrapper, RunStat)
processTask taskDef dataDef resultDef = do
  conf <- getConfiguration
  logInfo $ "loading data for: " ++ (describe dataDef)
  (taskInput, loadingDataDuration) <- measureDuration $ loadData conf dataDef
  logInfo $ "applying data to task: " ++ (describe taskDef)
  (result, loadingTaskDuration, executionDuration)  <- applyTaskLogic taskDef taskInput resultDef
  logDebug $ "returning result for " ++ (describe dataDef)
  return (result, (loadingDataDuration, loadingTaskDuration, executionDuration))

applyTaskLogic :: TaskDef -> TaskInput -> ResultDef -> IO (TaskResultWrapper, NominalDiffTime, NominalDiffTime)
applyTaskLogic (SourceCodeModule moduleName moduleContent) taskInput _ = do
  putStrLn "compiling task from source code"
  (taskFn, loadTaskDuration) <- measureDuration $ loadTask (I.as :: TaskInput -> TaskResult) moduleName moduleContent
  putStrLn "applying data"
  (result, execDuration) <- measureDuration $ return $ taskFn taskInput
  return (result, loadTaskDuration, execDuration) >>= return . (onFirst DirectResult)
-- Full binary deployment step 2/3: run within slave process to deploy the distributed task binary
applyTaskLogic (DeployFullBinary program inputMode) taskInput resultDef =
  DFB.deployAndRunFullBinary dataModes executeFullBinaryArg program taskInput >>= return . onFirst resultApplier
  where
    (dataModes, resultApplier) = buildDataModes inputMode resultDef
applyTaskLogic (PreparedDeployFullBinary hash inputMode) taskInput resultDef = do
  ((Just filePath), taskLoadDur) <- measureDuration $ RemoteStore.get hash --TODO catch unknown binary error nicer
  (res, execDur) <- DFB.runExternalBinary dataModes [executeFullBinaryArg] taskInput filePath
  return (res, taskLoadDur, execDur) >>= return . (onFirst resultApplier)
  where
    (dataModes, resultApplier) = buildDataModes inputMode resultDef
-- Serialized thunk deployment step 2/3: run within slave process to deploy the distributed task binary
applyTaskLogic (UnevaluatedThunk function program) taskInput resultDef = DST.deployAndRunSerializedThunk executeSerializedThunkArg function (shouldZipOutput resultDef) program taskInput
                                                                         >>= return . (onFirst ReadFromFile)
-- Partial binary deployment step 2/2: receive distribution on slave, prepare input data, link object file and spawn slave process, read its output
applyTaskLogic (ObjectCodeModule objectCode) taskInput _ = DOC.codeExecutionOnSlave objectCode taskInput >>= return . (onFirst DirectResult) -- TODO switch to location ("Left")

buildDataModes :: TaskInputMode -> ResultDef -> (DFB.DataModes, FilePath -> TaskResultWrapper)
buildDataModes inputMode resultDef =
  (DFB.DataModes (convertInputMode inputMode) outputMode, ReadFromFile)
  where
    outputMode = DFB.FileOutput $ shouldZipOutput resultDef
    convertInputMode :: TaskInputMode -> DFB.InputMode
    convertInputMode FileInput = DFB.FileInput
    convertInputMode StreamInput = DFB.StreamInput

shouldZipOutput :: ResultDef -> Bool
shouldZipOutput (HdfsResult _ s) = isZippedSuffix s
shouldZipOutput _ = False

isZippedSuffix :: FilePath -> Bool
isZippedSuffix = isSuffixOf ".gz"

onFirst :: (a -> a') -> (a, b, c) -> (a', b, c)
onFirst f (a, b, c) = (f a, b, c)

loadData :: Configuration -> DataDef -> IO TaskResult
loadData config (HdfsData hdfsLocation) = HDS.loadEntries (_hdfsConfig config, hdfsLocation)
loadData _ (PseudoDB numDB) = SDS.loadEntries ("resources/pseudo-db/" ++ (show numDB)) -- TODO make relative path configurable?

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
executionWithinSlaveProcessForFullBinaryDeployment :: DFB.DataModes -> (TaskInput -> TaskResult) -> FilePath -> FilePath -> IO ()
executionWithinSlaveProcessForFullBinaryDeployment = DFB.fullBinaryExecution

-- Serialized thunk deployment step 3/3: run within the spawned process for the distributed executable, applies data to distributed task.
executionWithinSlaveProcessForThunkSerialization :: DFB.DataModes -> String -> FilePath -> FilePath -> IO ()
executionWithinSlaveProcessForThunkSerialization dataModes taskFnArg taskInputFilePath taskOutputFilePath = do
  taskFn <- withErrorAction logError ("Could not read task logic: " ++(show taskFnArg)) $ return $ (read taskFnArg :: BL.ByteString)
  logInfo "slave: deserializing task logic"
  logDebug $ "slave: got this task function: " ++ (show taskFn)
  function <- deserializeFunction taskFn :: IO (TaskInput -> TaskResult)
  serializeFunction function >>= \s -> logDebug $ "task deserialization done for: " ++ (show $ BL.unpack s)
  DST.serializedThunkExecution dataModes function taskInputFilePath taskOutputFilePath

-- Partial binary deployment step 1/2: start distribution of task on master
objectCodeSerializationOnMaster :: IO TaskDef
objectCodeSerializationOnMaster = DOC.loadObjectCode >>= \objectCode -> return $ ObjectCodeModule objectCode
