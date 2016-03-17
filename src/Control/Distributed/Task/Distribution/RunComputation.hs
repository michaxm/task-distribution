{-|
  Defines a higher level interface to running calculations. Resolves HDFS input paths.
|-}
module Control.Distributed.Task.Distribution.RunComputation (
  MasterOptions(..),
  TaskSpec(..),
  DataSpec(..),
  ResultSpec(..),
  runMaster) where

import Data.List (isPrefixOf, sort)
import Data.List.Split (splitOn)
import System.Directory (getDirectoryContents) -- being renamed to listDirectory
import System.Environment (getExecutablePath)

import qualified Control.Distributed.Task.DataAccess.HdfsListing as HDFS
import Control.Distributed.Task.Distribution.TaskDistribution
import Control.Distributed.Task.TaskSpawning.TaskSpawning (fullBinarySerializationOnMaster, serializedThunkSerializationOnMaster, objectCodeSerializationOnMaster)
import Control.Distributed.Task.TaskSpawning.TaskDefinition
import Control.Distributed.Task.Types.HdfsConfigTypes
import Control.Distributed.Task.Types.TaskTypes
import Control.Distributed.Task.Util.Configuration
import Control.Distributed.Task.Util.Logging

-- | The definition of a distributed calculation.
data MasterOptions = MasterOptions {
  -- | the master hostname
  _host :: String,
  -- | the master port
  _port :: Int,
  -- | the task logic
  _taskSpec :: TaskSpec,
  -- | which data to process
  _dataSpecs :: DataSpec,
  -- | how to process the result
  _resultSpec :: ResultSpec
  }

-- | Task logic definition, most modes expect task mode support, see RemoteExecutionSupport.
data TaskSpec
   -- | build the given string as module remotely (restrictions apply)
 = SourceCodeSpec String
   -- | run this binary as task
 | FullBinaryDeployment
   -- | serialize the given function in the context of the given program, run both as task (restrictions apply)
 | SerializedThunk (TaskInput -> TaskResult)
   -- | only transport some of the generated object code and relink remotely (restrictions apply) - the function here is ignored, it only forces the compilation of the contained module
 | ObjectCodeModuleDeployment (TaskInput -> TaskResult)
-- | definition of input data   
data DataSpec
    -- | simple test data, the path is configured, amount of files can be limited
  = SimpleDataSpec Int
    -- | use given HDFS as starting directory, descend a number of directories from there and take all files starting with the filter prefix (if any given)
  | HdfsDataSpec HdfsPath Int (Maybe String)
-- | what to do with the result
data ResultSpec
    -- | process all results with the given method
  = CollectOnMaster ([TaskResult] -> IO ())
    -- | store the results in HDFS, in the given directory(1), with the given suffix (2), based on the input path.
  | StoreInHdfs String String
    -- | do nothing, for testing purposes only
  | Discard

-- | Run a computation.
runMaster :: MasterOptions -> IO ()
runMaster (MasterOptions masterHost masterPort taskSpec dataSpec resultSpec) = do
  taskDef <- buildTaskDef taskSpec
  dataDefs <- expandDataSpec dataSpec
  (resultDef, resultProcessor) <- return $ buildResultDef resultSpec
  executeDistributed (masterHost, masterPort) taskDef dataDefs resultDef resultProcessor
    where
      buildResultDef :: ResultSpec -> (ResultDef, [TaskResult] -> IO ())
      buildResultDef (CollectOnMaster resultProcessor) = (ReturnAsMessage, resultProcessor)
      buildResultDef (StoreInHdfs outputPrefix outputSuffix) = (HdfsResult outputPrefix outputSuffix True, \_ -> putStrLn "result stored in hdfs")
      buildResultDef Discard = (ReturnOnlyNumResults, \num -> putStrLn $ (show num) ++ " results discarded")

buildTaskDef :: TaskSpec -> IO TaskDef
buildTaskDef (SourceCodeSpec modulePath) = do
  moduleContent <- readFile modulePath
  return $ mkSourceCodeModule modulePath moduleContent
buildTaskDef FullBinaryDeployment =
  getExecutablePath >>= fullBinarySerializationOnMaster
buildTaskDef (SerializedThunk function) = do
  selfPath <- getExecutablePath
  serializedThunkSerializationOnMaster selfPath function
buildTaskDef (ObjectCodeModuleDeployment _) = objectCodeSerializationOnMaster

expandDataSpec :: DataSpec -> IO [DataDef]
expandDataSpec (HdfsDataSpec path depth filterPrefix) = do
  putStrLn $ "looking for files at " ++ path
  paths <- hdfsListFilesInSubdirsFiltering depth filterPrefix path
  putStrLn $ "found these input files: " ++ (show paths)
  return $ map HdfsData paths
expandDataSpec (SimpleDataSpec numTasks) = do
  config <- getConfiguration
  files <- getDirectoryContents (_pseudoDBPath config)
  return $ take numTasks $ map PseudoDB $ filter (not . ("." `isPrefixOf`)) $ sort files

mkSourceCodeModule :: String -> String -> TaskDef
mkSourceCodeModule modulePath moduleContent = SourceCodeModule (strippedModuleName modulePath) moduleContent
  where
    strippedModuleName = reverse . takeWhile (/= '/') . drop 1 . dropWhile (/= '.') . reverse

{-|
 Like hdfsListFiles, but descending into subdirectories and filtering the file names. Note that for now this is a rather a quick hack
 for special needs than a full fledged shell expansion
|-}
hdfsListFilesInSubdirsFiltering :: Int -> Maybe String -> String -> IO [String]
hdfsListFilesInSubdirsFiltering descendDepth fileNamePrefixFilter path = do
  initialFilePaths <- HDFS.listFiles path
  recursiveFiles <- recursiveDescent descendDepth path initialFilePaths
  logDebug $ "found: " ++ (show recursiveFiles)
  return $ map trimSlashes $ maybe recursiveFiles (\prefix -> filter ((prefix `isPrefixOf`) . getFileNamePart) recursiveFiles) fileNamePrefixFilter
  where
    getFileNamePart path' = let parts = splitOn "/" path' in if null parts then "" else parts !! (length parts -1)
    recursiveDescent :: Int -> String -> [String] -> IO [String]
    recursiveDescent 0 prefix paths = return (map (\p -> prefix++"/"++p) paths)
    recursiveDescent n prefix paths = do
      absolute <- return $ map (prefix++) paths :: IO [String]
      pathsWithChildren <- mapM (\p -> (HDFS.listFiles p >>= \cs -> return (p, cs))) absolute :: IO [(String, [String])]
      descended <- mapM (\(p, cs) -> if null cs then return [p] else recursiveDescent (n-1) p cs) pathsWithChildren :: IO [[String]]
      return $ concat descended
    trimSlashes :: String -> String
    trimSlashes [] = [] -- hadoop-rpc does not work on /paths//with/double//slashes
    trimSlashes ('/':'/':rest) = trimSlashes $ '/':rest
    trimSlashes (x:rest) = x:(trimSlashes rest)
