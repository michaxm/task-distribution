module Control.Distributed.Task.Util.Configuration (
  Configuration(..), getConfiguration, DistributionStrategy(..)
  )where

import Data.List.Split (splitOn)

import Control.Distributed.Task.Types.HdfsConfigTypes

data Configuration = Configuration {
  _maxTasksPerNode :: Int,
  _hdfsConfig :: HdfsConfig,
  _pseudoDBPath :: FilePath,
  _distributionStrategy :: DistributionStrategy,
  _taskLogFile :: FilePath,
  _sourceCodeDistributionHome :: FilePath,
  _sourceCodeModules :: [(String, Maybe String)],
  _objectCodePathOnMaster :: FilePath,
  _packageDbPath :: FilePath,
  _objectCodeResourcesPathSrc :: FilePath,
  _objectCodeResourcesPathExtra :: FilePath
  }

data DistributionStrategy
  = FirstTaskWithData
  | AnywhereIsFine

getConfiguration :: IO Configuration
getConfiguration = do
  confFile <- readFile "etc/config"
  sourceCodeModulesFile <- readFile "etc/sourcecodemodules"
  return $ parseConfig confFile sourceCodeModulesFile
  where
    parseConfig conf sourceCodeModulesFile =
      Configuration
      (read $ f "max-tasks-per-node")
      (readEndpoint $ f "hdfs")
      (f "pseudo-db-path")
      (readStrat $ f "distribution-strategy")
      (f "task-log-file")
      (f "source-code-distribution-home")
      (map mkSourceCodeModule $ lines sourceCodeModulesFile)
      (f "object-code-path-on-master")
      (f "package-db-path")
      (f "object-code-resources-path-src")
      (f "object-code-resources-path-extra")
      where
        f = getConfig conf
        readStrat s = case s of
                       "local" -> FirstTaskWithData
                       "anywhere" -> AnywhereIsFine
                       _ -> error $ "unknown strategy: "++s
        readEndpoint str = let es = splitOn ":" str
                           in if length es == 2
                              then (es !! 0, read $ es !! 1)
                              else error $ "endpoint not properly configured in etc/config (example: hdfs=localhost:55555): "++str

        mkSourceCodeModule line = let es = splitOn ":" line in if (length es) == 1 then (es !! 0, Nothing) else (es !! 0, Just $ es !! 1)

getConfig :: String -> String -> String
getConfig file key =
  let conf = (filter (not . null . fst) . map parseConfig . map (splitOn "=") . lines) file
  in maybe (error $ "not configured: "++key) id $ lookup key conf
  where
    parseConfig :: [String] -> (String, String)
    parseConfig es = if length es < 2 then ("", "") else (head es, concat $ tail es)
