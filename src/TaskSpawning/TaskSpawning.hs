module TaskSpawning.TaskSpawning where

import qualified Language.Haskell.Interpreter as I

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
  putStrLn "calculating result"
  result <- return $ func data'
  putStrLn "returning result"
  return result

buildTaskLogic :: TaskDef -> IO (TaskInput -> TaskResult)
buildTaskLogic (SourceCodeModule moduleName moduleContent) =
  loadTask (I.as :: TaskInput -> TaskResult) moduleName moduleContent

loadData :: DataSpec -> IO TaskResult
loadData (HdfsData filePath) = DS._loadEntries HDS.dataSource filePath -- TODO distinguish String/Read by overlapping instances or otherwise?
loadData (PseudoDB numDB) = DS._loadEntries SDS.stringSource ("/home/axm/projects/thesis-distributed-calculation/cluster-computing/resources/pseudo-db/" ++ (show numDB)) -- TODO make simple usable for all (for basic example)
