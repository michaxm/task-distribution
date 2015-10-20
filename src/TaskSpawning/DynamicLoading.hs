module TaskSpawning.DynamicLoading (
  loadTask
  ) where

import Data.List (intersperse)
import Data.Typeable (Typeable)
import qualified Language.Haskell.Interpreter as I
import System.Directory (getTemporaryDirectory, removeFile, removeDirectory)
import System.IO.Temp (createTempDirectory)
import System.FilePath ((</>))

loadTask :: (Typeable resultType) => resultType -> String -> String -> IO resultType
loadTask resultType moduleName moduleContent = do
  iRes <- I.runInterpreter (loadTaskDef resultType moduleName moduleContent)
  case iRes of
       Left err -> do
         printInterpreterError err
         error $ "could not load " ++ moduleName
       Right res -> return res

loadTaskDef :: Typeable a => a -> String -> String -> I.Interpreter a
loadTaskDef resultType moduleName moduleContent = do
  I.setImports ["Prelude"]
  sayI $ "Interpreter: Loading static modules and: " ++ moduleName ++ " ..."
  withTempModuleFile moduleName moduleContent loadModule
  func <- I.interpret "task" resultType
  sayI $ "done.\n"
  return func
    where
      loadModule moduleFile = do
        I.loadModules ["src/TaskSpawning/TaskTypes.hs", moduleFile] --FIXME typing + Hint
        I.setTopLevelModules [moduleName]

withTempModuleFile :: String -> String -> (FilePath -> I.Interpreter a) -> I.Interpreter a
withTempModuleFile moduleName moduleContent moduleAction = do
  (moduleFile, moduleDir) <- I.liftIO $ writeModuleFile
  res <- moduleAction moduleFile -- FIXME Exception Handling -> cleanup
  I.liftIO $ cleanupModuleFile (moduleFile, moduleDir)
  return res
  where
    writeModuleFile :: IO (FilePath, FilePath)
    writeModuleFile = do
      tempDir <- getTemporaryDirectory
      moduleTempDir <- createTempDirectory tempDir moduleName -- FIXME hierarchical module names
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
