module DynamicLoading (
  loadTask
  ) where

import Data.List (intersperse)
import Data.Typeable (Typeable)
import qualified Language.Haskell.Interpreter as I

loadTask :: (Typeable resultType) => resultType -> String -> IO resultType
loadTask resultType moduleName = do
  iRes <- I.runInterpreter (loadTaskDef resultType moduleName)
  case iRes of
       Left err -> do
         printInterpreterError err
         error $ "could not load " ++ moduleName
       Right res -> return res

loadTaskDef :: Typeable a => a -> String -> I.Interpreter a
loadTaskDef resultType m = do
  I.setImports ["Prelude"]
  sayI $ "Interpreter: Loading static modules and: " ++ m ++ " ..."
  I.loadModules ["src/TaskTypes.hs", m]
  I.setTopLevelModules [strippedModuleName m]
  func <- I.interpret "task" resultType
  sayI $ "done.\n"
  return func
    where
      -- TODO refactor, this should not be necessary ...
      strippedModuleName = reverse . takeWhile (/= '/') . drop 1 . dropWhile (/= '.') . reverse

printInterpreterError :: I.InterpreterError -> IO ()
printInterpreterError (I.WontCompile ghcErrors) = putStrLn $ "InterpreterError: " ++ (concat $ intersperse "\n" $ map I.errMsg ghcErrors)
printInterpreterError e = putStrLn $ "InterpreterError: " ++ (show e)

sayI :: String -> I.Interpreter ()
sayI = I.liftIO . putStr
