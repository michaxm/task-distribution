module Control.Distributed.Task.TaskSpawning.SourceCodeExecution (
  processSourceCodeTasks
  ) where

import Control.Monad.IO.Class (MonadIO)
import Data.List (intersperse)
import Data.Time.Clock (NominalDiffTime)
import Data.Typeable (Typeable)
import qualified Language.Haskell.Interpreter as I
import System.Directory (getTemporaryDirectory, removeFile, removeDirectory)
import System.IO.Temp (createTempDirectory)
import System.FilePath ((</>))

import Control.Distributed.Task.DataAccess.DataSource (loadData)
import Control.Distributed.Task.TaskSpawning.ExecutionUtil
import Control.Distributed.Task.TaskSpawning.TaskDefinition
import Control.Distributed.Task.TaskSpawning.TaskDescription
import Control.Distributed.Task.Types.TaskTypes
import Control.Distributed.Task.Util.Configuration
import Control.Distributed.Task.Util.Logging

processSourceCodeTasks :: String -> String -> [DataDef] -> IO ([TaskResult], NominalDiffTime)
processSourceCodeTasks moduleName moduleContent dataDefs =
  measureDuration $ mapM runSourceCodeTask dataDefs
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
            taskFn <- loadTask (I.as :: Task) moduleName moduleContent
            putStrLn "applying data"
            return $ taskFn taskInput

loadTask :: (Typeable resultType) => resultType -> String -> String -> IO resultType
loadTask resultType moduleName moduleContent= do
  config <- getConfiguration
  iRes <- I.runInterpreter (loadTaskDef resultType moduleName moduleContent config)
  case iRes of
       Left err -> do
         printInterpreterError err
         error $ "could not load " ++ moduleName
       Right res -> return res

loadTaskDef :: Typeable a => a -> String -> String -> Configuration -> I.Interpreter a
loadTaskDef resultType moduleName moduleContent config = do
  sayI $ "Interpreter: Loading static modules and: " ++ moduleName ++ " ..."
  I.set [
    I.installedModulesInScope I.:= True,
    I.searchPath I.:= [_sourceCodeDistributionHome config]
    ]
  withTempModuleFile moduleName moduleContent loadModule
  func <- I.interpret "task" resultType
  sayI $ "done.\n"
  return func
    where
      loadModule moduleFilePath = do
        I.loadModules [moduleFilePath]
        I.setTopLevelModules [moduleName]
        I.setImportsQ (_sourceCodeModules config)

withTempModuleFile :: (MonadIO m) => String -> String -> (FilePath -> m a) -> m a
withTempModuleFile moduleName moduleContent moduleAction = do
  (moduleFile, moduleDir) <- I.liftIO $ writeModuleFile
  res <- moduleAction moduleFile -- FIXME Exception Handling -> cleanup
  I.liftIO $ cleanupModuleFile (moduleFile, moduleDir)
  return res
  where
    writeModuleFile :: IO (FilePath, FilePath)
    writeModuleFile = do
      tempDir <- getTemporaryDirectory
      moduleTempDir <- createTempDirectory tempDir moduleName -- FIXME a) hierarchical module names, b) could probably be easier implemented with withSystemTempDirectory, too
      moduleFile <- return $ moduleTempDir </> moduleName ++ ".hs"
      writeFile moduleFile moduleContent
      return (moduleFile, moduleTempDir)
    cleanupModuleFile :: (FilePath, FilePath) -> IO ()
    cleanupModuleFile (f, d) = do
      removeFile f
      removeDirectory d

printInterpreterError :: I.InterpreterError -> IO ()
printInterpreterError (I.WontCompile ghcErrors) = putStrLn $ "InterpreterError: " ++ (concat $ intersperse "\n" $ map I.errMsg ghcErrors)
printInterpreterError e = putStrLn $ "InterpreterError: " ++ (show e)

sayI :: String -> I.Interpreter ()
sayI = I.liftIO . putStr
