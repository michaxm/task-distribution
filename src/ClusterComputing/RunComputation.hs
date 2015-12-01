module ClusterComputing.RunComputation (
  MasterOptions(..),
  TaskSpec(..),
  DataSpec(..),
  ResultSpec(..),
  runMaster) where

import System.Environment (getExecutablePath)
import qualified System.HDFS.HDFSClient as HDFS --TODO ok to be referenced here? probably yes, but consider again later

import ClusterComputing.TaskDistribution
import TaskSpawning.TaskTypes
import TaskSpawning.TaskSpawning (fullDeploymentExecutionRemote, objectCodeExecutionRemote)

data MasterOptions = MasterOptions {
  _host :: String,
  _port :: Int,
  _taskSpec :: TaskSpec,
  _dataSpecs :: DataSpec,
  _resultSpec :: ResultSpec
  }

{-
 ObjectCodeModuleDeployment:
 - the function here is ignored, it only forces the compilation of the contained module
 - this could contain configurations for the object code file path etc. in the future
-}
data TaskSpec
 = SourceCodeSpec String
 | FullBinaryDeployment (TaskInput -> TaskResult)
 | ObjectCodeModuleDeployment (TaskInput -> TaskResult)
data DataSpec
  = SimpleDataSpec Int
  | HdfsDataSpec HdfsConfig String
data ResultSpec
  = CollectOnMaster (TaskResult -> IO ())

runMaster :: MasterOptions -> IO ()
runMaster (MasterOptions masterHost masterPort taskSpec dataSpec resultSpec) = do
  taskDef <- buildTaskDef taskSpec
  dataDefs <- expandDataSpec dataSpec
  (resultDef, resultProcessor) <- return $ buildResultDef resultSpec
  executeDistributed (masterHost, masterPort) taskDef dataDefs resultDef resultProcessor
    where
      buildTaskDef :: TaskSpec -> IO TaskDef
      buildTaskDef (SourceCodeSpec modulePath) = do
        moduleContent <- readFile modulePath
        return $ mkSourceCodeModule modulePath moduleContent
      buildTaskDef (FullBinaryDeployment function) = do
        selfPath <- getExecutablePath
        fullDeploymentExecutionRemote selfPath function
      buildTaskDef (ObjectCodeModuleDeployment _) = objectCodeExecutionRemote
      buildResultDef (CollectOnMaster resultProcessor) = (ReturnAsMessage, resultProcessor)

expandDataSpec :: DataSpec -> IO [DataDef]
expandDataSpec (HdfsDataSpec config path) = do
  putStrLn $ "looking for files at " ++ path
  paths <- HDFS.hdfsListFiles config path
  putStrLn $ "found " ++ (show paths)
  return $ map (HdfsData . (\p -> (config, p))) paths
expandDataSpec (SimpleDataSpec numDBs) = return $ mkSimpleDataSpecs numDBs
  where
    mkSimpleDataSpecs :: Int -> [DataDef]
    mkSimpleDataSpecs 0 = []
    mkSimpleDataSpecs n = PseudoDB n : (mkSimpleDataSpecs (n-1))

mkSourceCodeModule :: String -> String -> TaskDef
mkSourceCodeModule modulePath moduleContent = SourceCodeModule (strippedModuleName modulePath) moduleContent
  where
    strippedModuleName = reverse . takeWhile (/= '/') . drop 1 . dropWhile (/= '.') . reverse
