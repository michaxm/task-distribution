module Control.Distributed.Task.TaskSpawning.DeployObjectCodeRelinked (
  loadObjectCode, deployAndRunObjectCodeRelinked
  ) where

import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.List (intersperse)
import System.IO.Temp (withSystemTempDirectory)

import Control.Distributed.Task.TaskSpawning.DeployFullBinary
import Control.Distributed.Task.TaskSpawning.ExecutionUtil
import Control.Distributed.Task.Util.Configuration
import Control.Distributed.Task.Util.Logging

loadObjectCode :: IO BL.ByteString
loadObjectCode = do
  config <- getConfiguration
  BL.readFile (_objectCodePathOnMaster config ++ "/RemoteExecutable.o")

deployAndRunObjectCodeRelinked :: BLC.ByteString -> IOHandling -> IO ExternalExecutionResult
deployAndRunObjectCodeRelinked objectCode ioHandling =
  withSystemTempDirectory "object-code-build-dir" $ \builddir ->
    let objectCodeFilePath = builddir ++ "/RemoteExecutable.o"
        binaryPath = builddir++"/binary"
    in do
      logInfo "slave: preparing alternative main object code"
      config <- getConfiguration
      libs <- readFile "etc/remotelibs" >>= return . lines
      _ <- executeExternal "ghc"
           [
             "-no-link",
             "-outputdir", builddir,
             "-package-db", _packageDbPath config,
             "-i"++(_objectCodeResourcesPathSrc config),
             "Control.Distributed.Task.TaskSpawning.DeployFullBinary",
             "Control.Distributed.Task.Types.TaskTypes",
             "-i"++(_objectCodeResourcesPathExtra config),
             "RemoteExecutable",
             (_objectCodeResourcesPathExtra config)++"/RemoteExecutor.hs" --explicit for Main
           ]
      logInfo $ "slave: prepared frame in "++builddir
      logInfo "slave: storing transported object code"
      BL.writeFile objectCodeFilePath objectCode
      logInfo $ "slave: linking: " ++ binaryPath
      _ <- executeExternal "ghc"
           ([
               "-o", binaryPath,
               builddir++"/Main.o",
               objectCodeFilePath,
               "-package-db", _packageDbPath config
            ]
            ++ (map ((builddir++"/")++) taskDistributionDeps)
            ++ (asPackageOpts libs)
            )
      logInfo $ "slave: running " ++ binaryPath
      res <- runExternalBinary [] ioHandling binaryPath
      logInfo $ "executing task finished"
      logInfo $ "results: "++show res
      return res
  where
    taskDistributionDeps = [
      "Control/Distributed/Task/DataAccess/DataSource.o",
      "Control/Distributed/Task/DataAccess/HdfsDataSource.o",
      "Control/Distributed/Task/DataAccess/SimpleDataSource.o",
      "Control/Distributed/Task/TaskSpawning/DeployFullBinary.o",
      "Control/Distributed/Task/TaskSpawning/ExecutionUtil.o",
      "Control/Distributed/Task/TaskSpawning/TaskDescription.o",
      "Control/Distributed/Task/TaskSpawning/TaskDefinition.o",
      "Control/Distributed/Task/TaskSpawning/TaskSpawningTypes.o",
      "Control/Distributed/Task/Types/TaskTypes.o",
      "Control/Distributed/Task/Util/Logging.o",
      "Control/Distributed/Task/Util/Configuration.o",
      "Control/Distributed/Task/Util/ErrorHandling.o",
      "Control/Distributed/Task/Util/FileUtil.o"
      ]
    asPackageOpts = ("-package":) . (intersperse "-package")
