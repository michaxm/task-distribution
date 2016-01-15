module ClusterComputing.RunComputation (
  MasterOptions(..),
  TaskSpec(..),
  InputMode(..),
  DataSpec(..),
  ResultSpec(..),
  runMaster) where

import Data.List (isPrefixOf)
import Data.List.Split (splitOn)
import System.Environment (getExecutablePath)
import qualified System.HDFS.HDFSClient as HDFS --TODO ok to be referenced here? probably yes, but consider again later

import ClusterComputing.TaskDistribution
import TaskSpawning.TaskSpawning (fullBinarySerializationOnMaster, serializedThunkSerializationOnMaster, objectCodeSerializationOnMaster)
import TaskSpawning.TaskDefinition
import Types.HdfsConfigTypes
import Types.TaskTypes

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
 | FullBinaryDeployment InputMode
 | SerializedThunk (TaskInput -> TaskResult)
 | ObjectCodeModuleDeployment (TaskInput -> TaskResult)
data InputMode = File | Stream
data DataSpec
  = SimpleDataSpec Int
  | HdfsDataSpec HdfsLocation Int (Maybe String)
data ResultSpec
  = CollectOnMaster (TaskResult -> IO ())
  | StoreInHdfs String
  | Discard

runMaster :: MasterOptions -> IO ()
runMaster (MasterOptions masterHost masterPort taskSpec dataSpec resultSpec) = do
  taskDef <- buildTaskDef taskSpec
  dataDefs <- expandDataSpec dataSpec
  (resultDef, resultProcessor) <- return $ buildResultDef resultSpec
  executeDistributed (masterHost, masterPort) taskDef dataDefs resultDef resultProcessor
    where
      buildResultDef (CollectOnMaster resultProcessor) = (ReturnAsMessage, resultProcessor)
      buildResultDef (StoreInHdfs outputPrefix) = (HdfsResult outputPrefix, \_ -> putStrLn "result stored in hdfs")
      buildResultDef Discard = (ReturnOnlyNumResults, \num -> putStrLn $ (show num) ++ " results discarded")

buildTaskDef :: TaskSpec -> IO TaskDef
buildTaskDef (SourceCodeSpec modulePath) = do
  moduleContent <- readFile modulePath
  return $ mkSourceCodeModule modulePath moduleContent
buildTaskDef (FullBinaryDeployment inputMode) =
  getExecutablePath >>= fullBinarySerializationOnMaster (convertInputMode inputMode)
buildTaskDef (SerializedThunk function) = do
  selfPath <- getExecutablePath
  serializedThunkSerializationOnMaster selfPath function
buildTaskDef (ObjectCodeModuleDeployment _) = objectCodeSerializationOnMaster

convertInputMode :: InputMode -> TaskInputMode
convertInputMode File = FileInput
convertInputMode Stream = StreamInput

expandDataSpec :: DataSpec -> IO [DataDef]
expandDataSpec (HdfsDataSpec (config, path) depth filterPrefix) = do
  putStrLn $ "looking for files at " ++ path
  paths <- hdfsListFilesInSubdirsFiltering depth filterPrefix config path
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

{-|
 Like hdfsListFiles, but descending into subdirectories and filtering the file names. Note that for now this is a rather a quick hack
 for special needs than a full fledged shell expansion
|-}
hdfsListFilesInSubdirsFiltering :: Int -> Maybe String -> HDFS.Config -> String -> IO [String]
hdfsListFilesInSubdirsFiltering descendDepth fileNamePrefixFilter config path = do
  initialFiles <- HDFS.hdfsListFiles config path
  recursiveFiles <- recursiveDescent descendDepth initialFiles
  return $ maybe recursiveFiles (\prefix -> filter ((prefix `isPrefixOf`) . getFileNamePart) recursiveFiles) fileNamePrefixFilter
  where
    getFileNamePart path' = let parts = splitOn "/" path' in if null parts then "" else parts !! (length parts -1)
    recursiveDescent :: Int -> [String] -> IO [String]
    recursiveDescent 0 initialFiles = return initialFiles
    recursiveDescent n initialFiles = do
      expanded <- mapM (HDFS.hdfsListFiles config) initialFiles :: IO [[String]]
      flattened <- return $ concat expanded
      recursiveDescent (n-1) flattened
