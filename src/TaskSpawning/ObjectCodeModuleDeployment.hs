module TaskSpawning.ObjectCodeModuleDeployment (
  loadObjectCode, codeExecutionOnSlave,
  -- visible for testing:
  buildLibs
  ) where

import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.List (intersperse)
import Data.Time.Clock (NominalDiffTime)
import System.Directory (getHomeDirectory)
import System.IO.Temp (withSystemTempDirectory)

import Control.Distributed.Task.Util.Configuration
import TaskSpawning.ExecutionUtil
import Types.TaskTypes
import Util.Logging

loadObjectCode :: IO BL.ByteString
loadObjectCode = do
  config <- getConfiguration
  BL.readFile (_relativeObjectCodePath config)

codeExecutionOnSlave :: BL.ByteString -> TaskInput -> IO (TaskResult, NominalDiffTime, NominalDiffTime)
codeExecutionOnSlave objectCode taskInput = do
  ((res, execDur), totalDur) <- measureDuration $ withSystemTempDirectory "object-code-build-dir" $ \builddir -> do
    codeExecutionOnSlave' taskInput builddir objectCode
  return (res, totalDur - execDur, execDur)

codeExecutionOnSlave' :: TaskInput -> FilePath -> BL.ByteString -> IO (TaskResult, NominalDiffTime)
codeExecutionOnSlave' taskInput builddir objectCode = do
  logInfo "slave: creating execution frame"
  _ <- executeExternal "ghc" ["-no-link", "-outputdir", builddir, "object-code-app/RemoteExecutable.hs", "object-code-app/RemoteExecutor.hs"]
  logInfo "slave: saving transported code"
  objectCodeFilePath <- return $ builddir ++ "/RemoteExecutable.o"
  BL.writeFile objectCodeFilePath objectCode
  binaryPath <- return $ builddir++"/binary"
  logInfo $ "slave: linking: " ++ binaryPath
  libs <- determineLibs
  _ <- executeExternal "ghc" (["-o", binaryPath, builddir++"/Main.o", objectCodeFilePath] ++ (fst libs))
  logInfo $ "slave: running " ++ binaryPath
  logDebug $ "slave: task input: " ++ (show taskInput)
  (result, execDur) <- measureDuration $ withTempBLCFile "distributed-program-data" (serializeTaskInput taskInput) $ \taskInputFilePath ->
    withEnv "LD_LIBRARY_PATH" (concat $ intersperse ":" $ snd libs) $ do
      executeExternal binaryPath [taskInputFilePath]
  logInfo $ "executing task finished"
  logDebug $ "got run output: " ++ (show result)
  parsedResult <- parseResultStrict $ BLC.pack result
  return (parsedResult, execDur)

determineLibs :: IO ([String], [String])
determineLibs = do
  homeDir <- getHomeDirectory
  config <- getConfiguration
  remoteLibs <- readFile "etc/remotelibs"
  return $ buildLibs False homeDir (_libLocation config) (_ghcVersion config) remoteLibs

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
