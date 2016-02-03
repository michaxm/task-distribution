module Control.Distributed.Task.TaskSpawning.TaskSpawning (
  processTasks, TasksExecutionResult,
  fullBinarySerializationOnMaster, executeFullBinaryArg, executionWithinSlaveProcessForFullBinaryDeployment,
  serializedThunkSerializationOnMaster, executeSerializedThunkArg, executionWithinSlaveProcessForThunkSerialization,
  objectCodeSerializationOnMaster) where

import Control.Concurrent.Async (async, wait)
import qualified Data.ByteString.Lazy as BL
import Data.List (intersperse)
import qualified Language.Haskell.Interpreter as I

import Control.Distributed.Task.DataAccess.DataSource (loadData)
import qualified Control.Distributed.Task.TaskSpawning.BinaryStorage as RemoteStore
import qualified Control.Distributed.Task.TaskSpawning.DeployFullBinary as DFB
import qualified Control.Distributed.Task.TaskSpawning.DeploySerializedThunk as DST
import qualified Control.Distributed.Task.TaskSpawning.ObjectCodeModuleDeployment as DOC
import Control.Distributed.Task.TaskSpawning.ExecutionUtil (measureDuration)
import Control.Distributed.Task.TaskSpawning.FunctionSerialization (serializeFunction, deserializeFunction)
import Control.Distributed.Task.TaskSpawning.SourceCodeExecution (loadTask)
import Control.Distributed.Task.TaskSpawning.TaskDefinition
import Control.Distributed.Task.TaskSpawning.TaskDescription
import Control.Distributed.Task.TaskSpawning.TaskSpawningTypes
import Control.Distributed.Task.Types.TaskTypes
import Control.Distributed.Task.Util.ErrorHandling
import Control.Distributed.Task.Util.Logging

executeFullBinaryArg, executeSerializedThunkArg :: String
executeFullBinaryArg = "executefullbinary"
executeSerializedThunkArg = "executeserializedthunk"

type TasksExecutionResult = DFB.ExternalExecutionResult

{-|
  Apply the task on the data, producing either a location, where the results are stored, or the results directly.
  Which of those depends on the distribution type, when external programs are spawned the former can be more efficient,
  but if there is no such intermediate step, a direct result is better.
|-}
processTasks :: TaskDef -> [DataDef] -> ResultDef -> IO TasksExecutionResult
processTasks (SourceCodeModule moduleName moduleContent) dataDefs _ = processSourceCodeTasks moduleName moduleContent dataDefs
processTasks taskDef dataDefs resultDef = do
  logInfo $ "spawning task for: "++(concat $ intersperse ", " $ map describe dataDefs)
  spawnExternalTask taskDef dataDefs resultDef

{-|
 Source code distribution behaves a bit different and only supports collectonmaster
-}
processSourceCodeTasks :: String -> String -> [DataDef] -> IO TasksExecutionResult
processSourceCodeTasks moduleName moduleContent dataDefs =
  measureDuration $ do
    tasks <- mapM (async . runSourceCodeTask) dataDefs
    mapM wait tasks -- FIXME this probably does not parallelilize at all
    where
      runSourceCodeTask :: DataDef -> IO TaskResult
      runSourceCodeTask dataDef = do
        logInfo $ "loading data for: "++describe dataDef
        taskInput <- loadData dataDef
        logInfo $ "applying data to task:"++moduleName
        result <- applySourceCodeTaskLogic taskInput
        return result
        where
          applySourceCodeTaskLogic taskInput = do
            putStrLn "compiling task from source code"
            taskFn <- loadTask (I.as :: TaskInput -> TaskResult) moduleName moduleContent
            putStrLn "applying data"
            return $ taskFn taskInput

-- TODO additional measuring of the task loading (execution overhead): time for loading source code / complete execution time of external program with reported data load and exec times subtracted

spawnExternalTask :: TaskDef -> [DataDef] -> ResultDef -> IO DFB.ExternalExecutionResult
spawnExternalTask (SourceCodeModule _ _) _ _ = error "source code distribution is handled differently"
-- Full binary deployment step 2/3: run within slave process to deploy the distributed task binary
spawnExternalTask (DeployFullBinary program) dataDefs resultDef =
  DFB.deployAndRunFullBinary executeFullBinaryArg (IOHandling dataDefs resultDef) program
spawnExternalTask (PreparedDeployFullBinary hash) dataDefs resultDef = do
  filePath_ <- RemoteStore.get hash
  maybe (error $ "no such program: "++show hash) (DFB.runExternalBinary [executeFullBinaryArg] (IOHandling dataDefs resultDef)) filePath_
-- Serialized thunk deployment step 2/3: run within slave process to deploy the distributed task binary
spawnExternalTask (UnevaluatedThunk function program) dataDefs resultDef =
  DST.deployAndRunSerializedThunk executeSerializedThunkArg function (IOHandling dataDefs resultDef) program
-- Partial binary deployment step 2/2: receive distribution on slave, prepare input data, link object file and spawn slave process, read its output
spawnExternalTask (ObjectCodeModule _) _ _ = error $ "not implemented right now" --DOC.codeExecutionOnSlave objectCode taskInput >>= return . (onFirst DirectResult) -- TODO switch to location ("Left")

-- Full binary deployment step 1/3
fullBinarySerializationOnMaster :: FilePath -> IO TaskDef
fullBinarySerializationOnMaster programPath = do
  currentExecutable <- BL.readFile programPath
  return $ DeployFullBinary currentExecutable

-- Serialized thunk deployment step 1/3: run within the client/master process to serialize itself.
serializedThunkSerializationOnMaster :: FilePath -> (TaskInput -> TaskResult) -> IO TaskDef
serializedThunkSerializationOnMaster programPath function = do
  program <- BL.readFile programPath -- TODO ByteString serialization should be contained within DST module
  taskFn <- serializeFunction function
  return $ UnevaluatedThunk taskFn program

-- Full binary deployment step 3/3: run within the spawned process for the distributed executable, applies data to distributed task.
executionWithinSlaveProcessForFullBinaryDeployment :: IOHandling -> (TaskInput -> TaskResult) -> IO ()
executionWithinSlaveProcessForFullBinaryDeployment = DFB.fullBinaryExecution

-- Serialized thunk deployment step 3/3: run within the spawned process for the distributed executable, applies data to distributed task.
executionWithinSlaveProcessForThunkSerialization :: IOHandling -> String -> IO ()
executionWithinSlaveProcessForThunkSerialization ioHandling taskFnArg = do
  taskFn <- withErrorAction logError ("Could not read task logic: " ++(show taskFnArg)) $ return $ (read taskFnArg :: BL.ByteString)
  logInfo "slave: deserializing task logic"
  logDebug $ "slave: got this task function: " ++ (show taskFn)
  function <- deserializeFunction taskFn :: IO (TaskInput -> TaskResult)
  serializeFunction function >>= \s -> logDebug $ "task deserialization done for: " ++ (show $ BL.unpack s)
  DST.serializedThunkExecution ioHandling function

-- Partial binary deployment step 1/2: start distribution of task on master
objectCodeSerializationOnMaster :: IO TaskDef
objectCodeSerializationOnMaster = DOC.loadObjectCode >>= \objectCode -> return $ ObjectCodeModule objectCode
