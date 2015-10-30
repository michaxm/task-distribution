module TaskSpawning.TaskSpawning where

import qualified Language.Haskell.Interpreter as I

import qualified DataAccess.DataSource as DS
import qualified DataAccess.SimpleDataSource as SDS
import qualified DataAccess.HdfsDataSource as HDS

import TaskSpawning.SourceCodeExecution (loadTask)
import TaskSpawning.TaskTypes
import TaskSpawning.DeployCompleteProgram

processTask :: TaskDef -> DataSpec -> IO TaskResult
processTask taskDef dataSpec = do
  putStrLn "building task logic"
  func <- buildTaskLogic taskDef
  putStrLn "loading data"
  data' <- loadData dataSpec -- FIXME port not open (file not found?) error silently dropped
  putStrLn "calculating result"
  result <- return $ func data'
  putStrLn "returning result"
  return result

buildTaskLogic :: TaskDef -> IO (TaskInput -> TaskResult)
buildTaskLogic (SourceCodeModule moduleName moduleContent) =
  loadTask (I.as :: TaskInput -> TaskResult) moduleName moduleContent
buildTaskLogic (UnevaluatedThunk function program) = deployAndRun function program

loadData :: DataSpec -> IO TaskResult
loadData (HdfsData config filePath) = DS._loadEntries (HDS.dataSource config) filePath -- TODO distinguish String/Read by overlapping instances or otherwise?
loadData (PseudoDB numDB) = DS._loadEntries SDS.stringSource ("/home/axm/projects/thesis-distributed-calculation/cluster-computing/resources/pseudo-db/" ++ (show numDB)) -- TODO make simple usable for all (for basic example)
