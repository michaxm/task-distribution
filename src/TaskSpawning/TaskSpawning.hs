module TaskSpawning.TaskSpawning where

import qualified Language.Haskell.Interpreter as I

import ClusterComputing.TaskTransport
import qualified DataAccess.DataSource as DS
import qualified DataAccess.SimpleDataSource as SDS
import qualified DataAccess.HdfsDataSource as HDS
import TaskSpawning.DynamicLoading (loadTask)
import TaskSpawning.TaskTypes

processTask :: TaskDef -> DataSpec -> IO TaskResult
processTask taskDef dataSpec = do
  putStrLn "building task logic"
  func <- buildTaskLogic taskDef
  putStrLn "loading data"
  data' <- loadData dataSpec
  return $ func data'

buildTaskLogic :: TaskDef -> IO (TaskInput -> TaskResult)
buildTaskLogic (SourceCodeModule moduleText) =
  loadTask (I.as :: TaskInput -> TaskResult) moduleText

loadData :: DataSpec -> IO TaskResult
loadData (HdfsData filePath) = DS._loadEntries HDS.dataSource filePath
loadData (PseudoDB numDB) = DS._loadEntries SDS.dataSource ("resources/pseudo-db/" ++ (show numDB))
