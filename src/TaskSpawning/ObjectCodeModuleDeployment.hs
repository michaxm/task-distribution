module TaskSpawning.ObjectCodeModuleDeployment (
  loadObjectCode, codeExecutionOnWorker,
  -- visible for testing:
  buildLibs
  ) where

import qualified Data.ByteString.Lazy as BL
import Data.List (intersperse)
import Data.List.Split (splitOn)
import System.Directory (getHomeDirectory)
import System.IO.Temp (withSystemTempDirectory)

import TaskSpawning.ExecutionUtil
import TaskSpawning.TaskTypes
import Util.Logging

loadObjectCode :: IO BL.ByteString
loadObjectCode = do
  pathConfig <- getConfig "relative-object-codepath"
  BL.readFile pathConfig

getConfig :: String -> IO String
getConfig key = do
  conf <- readFile "etc/config" >>= return . filter (not . null . fst) . map parseConfig . map (splitOn "=") . lines
  return $ maybe "" id $ lookup key conf
    where
      parseConfig :: [String] -> (String, String)
      parseConfig es = if length es < 2 then ("", "") else (head es, concat $ tail es)

codeExecutionOnWorker :: BL.ByteString -> TaskInput -> IO TaskResult
codeExecutionOnWorker objectCode taskInput = do
  withSystemTempDirectory "object-code-build-dir" $ \builddir -> do
    codeExecutionOnWorker' taskInput builddir objectCode

codeExecutionOnWorker' :: TaskInput -> FilePath -> BL.ByteString -> IO TaskResult
codeExecutionOnWorker' taskInput builddir objectCode = do
  logInfo "creating execution frame"
  _ <- executeExternal "ghc" ["-no-link", "-outputdir", builddir, "object-code-app/RemoteExecutable.hs", "object-code-app/RemoteExecutor.hs"]
  logInfo "saving transported code"
  objectCodeFilePath <- return $ builddir ++ "/RemoteExecutable.o"
  BL.writeFile objectCodeFilePath objectCode
  binaryPath <- return $ builddir++"/binary"
  logInfo $ "linking: " ++ binaryPath
  libs <- determineLibs
  _ <- executeExternal "ghc" (["-o", binaryPath, builddir++"/Main.o", objectCodeFilePath] ++ (fst libs))
  logInfo $ "running " ++ binaryPath
  logDebug $ "task input: " ++ (show taskInput)
  result <- withTempBLCFile "distributed-program-data" (serializeTaskInput taskInput) $ \taskInputFilePath ->
    withEnv "LD_LIBRARY_PATH" (concat $ intersperse ":" $ snd libs) $ do
      executeExternal binaryPath [taskInputFilePath]
  logDebug $ "got run output: " ++ (show result)
  parseResult result

determineLibs :: IO ([String], [String])
determineLibs = do
  homeDir <- getHomeDirectory
  libLocation <- getConfig "lib-location"
  ghcVersion <- getConfig "ghc-version"
  remoteLibs <- readFile "etc/remotelibs"
  return $ buildLibs False homeDir libLocation ghcVersion remoteLibs

-- TODO advantages of .so?
-- transforms system configuration and given libs to (shared libs, lib dirs)
buildLibs :: Bool -> FilePath -> FilePath -> String -> String -> ([String], [String])
buildLibs dynamic homeDir libLocation ghc = unzip . buildLibs'
    where
      buildLibs' = map buildLib . lines
        where
          buildLib lib = (libloc ++ "libHS" ++ lib ++ libtype, libloc)
            where
              libloc = loc ++ "/" ++ lib ++ "/"
              libtype = if dynamic then "-" ++ (filter (/='-') ghc) ++ ".so" else ".a"
              loc = replaceHome libLocation
                where 
                  replaceHome ('~':rest) = homeDir ++ rest
                  replaceHome o = o
