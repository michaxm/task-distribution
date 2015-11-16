module ClusterComputing.DataLocality (
  findNodesWithData,
  nodeMatcher -- visible for testing
  ) where

import Control.Distributed.Process (NodeId)
import Data.List (sortBy)
import Data.List.Split (splitOn)
import Data.Ord (comparing)
import System.HDFS.HDFSClient

import TaskSpawning.TaskTypes

{-
 Filters the given nodes to those with any of the file blocks, ordered by the number of file blocks (not regarding individual file block length).
-}
findNodesWithData :: DataDef -> [NodeId] -> IO [NodeId]
findNodesWithData dataDef nodes = do
  hostsWithData <- hdfsFileDistribution (_config dataDef) (_filePath dataDef)
  mergedNodeIds <- return $ map fst $ sortOn snd $ merge matcher merger nodes hostsWithData
-- TODO real logging
--  print hostsWithData
--  print nodes
--  print mergedNodeIds
  return mergedNodeIds
    where
      matcher node (hdfsName, _) = nodeMatcher (show node) hdfsName -- HACK uses show to access nodeId data
      merger :: NodeId -> (String, Int) -> (NodeId, Int)
      merger nid (_, n) = (nid, n)

nodeMatcher ::String -> String -> Bool
nodeMatcher node hdfsName = hostSynonyms (extractHdfsHost hdfsName) == hostSynonyms (extractNodeIdHost node)
  where
    -- HACK: extracts the host name from "nid://localhost:44441:0"
    extractNodeIdHost = dropWhile (=='/') . head . drop 1 . splitOn ":"
    -- HACK: extracts the host name from "127.0.0.1:50010"
    extractHdfsHost = head . splitOn ":"
    -- HACK: localhost == 127.0.0.1, TODO at some time we will need a worder node / hdfs host config anyway, hopefully making all these HACKs obsolete
    hostSynonyms "127.0.0.1" = "localhost"
    hostSynonyms s = s

{-
 Merges left with right: for each left, take the first match in right, ignoring other possible matches.
-}
merge :: (a -> b -> Bool) -> (a -> b -> c) -> [a] -> [b] -> [c]
merge matcher merger = merge'
  where
    merge' _ [] = []
    merge' [] _ = []
    merge' (a:as) bs = maybe restMerge (:restMerge) (merge'' bs)
      where
        restMerge = merge' as bs
--        merge'' :: [b] -> Maybe c
        merge'' [] = Nothing
        merge'' (b:bs') = if matcher a b then Just (merger a b) else merge'' bs'

{- since 4.8 in Data.List -}
sortOn :: Ord b => (a -> b) -> [a] -> [a] 
sortOn f =
  map snd . sortBy (comparing fst) . map (\x -> let y = f x in y `seq` (y, x))
