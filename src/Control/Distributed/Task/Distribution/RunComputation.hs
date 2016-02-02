module Control.Distributed.Task.Distribution.RunComputation (
  MasterOptions(..),
  TaskSpec(..),
  DataSpec(..),
  ResultSpec(..),
  runMaster) where

import Data.List (isPrefixOf)
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
 | FullBinaryDeployment
 | SerializedThunk (TaskInput -> TaskResult)
 | ObjectCodeModuleDeployment (TaskInput -> TaskResult)
data DataSpec
  = SimpleDataSpec Int
  | HdfsDataSpec HdfsPath Int (Maybe String)
data ResultSpec
  = CollectOnMaster ([TaskResult] -> IO ())
  | StoreInHdfs String String
  | Discard

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
  return $ take numTasks $ map PseudoDB files

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
