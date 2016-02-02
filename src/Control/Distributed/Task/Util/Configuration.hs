module Control.Distributed.Task.Util.Configuration (
  Configuration(..), getConfiguration, DistributionStrategy(..)
  )where

import Data.List.Split (splitOn)

import Control.Distributed.Task.Types.HdfsConfigTypes

data Configuration = Configuration {
  _relativeObjectCodePath :: FilePath,
  _libLocation :: FilePath,
  _ghcVersion :: String,
  _hdfsConfig :: HdfsConfig,
  _pseudoDBPath :: FilePath,
  _distributionStrategy :: DistributionStrategy,
  _taskLogFile :: FilePath,
  _maxTasksPerNode :: Int
  }

data DistributionStrategy
  = FirstTaskWithData
  | AnywhereIsFine

getConfiguration :: IO Configuration
getConfiguration = readFile "etc/config" >>= return . parseConfig
  where
    parseConfig conf = Configuration
                       (f "relative-object-codepath")
                       (f "lib-location")
                       (f "ghc-version")
                       (readHdfs $ f "hdfs")
                       ("pseudo-db-path")
                       (readStrat $ f "distribution-strategy")
                       (f "task-log-file")
                       (read $ f "max-tasks-per-node")
      where
        f = getConfig conf
        readStrat s = case s of
                       "local" -> FirstTaskWithData
                       "anywhere" -> AnywhereIsFine
                       _ -> error $ "unknown strategy: "++s
        readHdfs str = let es = splitOn ":" str
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
