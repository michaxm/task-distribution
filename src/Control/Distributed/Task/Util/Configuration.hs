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
  _objectCodePathOnMaster :: FilePath,
  _packageDbPath :: FilePath,
  _objectCodeResourcesPathSrc :: FilePath,
  _objectCodeResourcesPathExtra :: FilePath,
  _libLocation :: FilePath,
  _ghcVersion :: String
  }

data DistributionStrategy
  = FirstTaskWithData
  | AnywhereIsFine

getConfiguration :: IO Configuration
getConfiguration = readFile "etc/config" >>= return . parseConfig
  where
    parseConfig conf = Configuration
                       (read $ f "max-tasks-per-node")
                       (readEndpoint $ f "hdfs")
                       (f "pseudo-db-path")
                       (readStrat $ f "distribution-strategy")
                       (f "task-log-file")
                       (f "object-code-path-on-master")
                       (f "package-db-path")
                       (f "object-code-resources-path-src")
                       (f "object-code-resources-path-extra")
                       (f "lib-location")
                       (f "ghc-version")
      where
        f = getConfig conf
        readStrat s = case s of
                       "local" -> FirstTaskWithData
                       "anywhere" -> AnywhereIsFine
                       _ -> error $ "unknown strategy: "++s
        readEndpoint str = let es = splitOn ":" str
                           in if length es == 2
                              then (es !! 0, read $ es !! 1)
                              else error $ "hdfs not properly configured in etc/config (example: hdfs=localhost:55555): "++str

getConfig :: String -> String -> String
getConfig file key =
  let conf = (filter (not . null . fst) . map parseConfig . map (splitOn "=") . lines) file
  in maybe (error $ "not configured: "++key) id $ lookup key conf
  where
    parseConfig :: [String] -> (String, String)
    parseConfig es = if length es < 2 then ("", "") else (head es, concat $ tail es)
