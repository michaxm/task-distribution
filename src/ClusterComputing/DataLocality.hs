module ClusterComputing.DataLocality (
  findNodesWithData,
  nodeMatcher -- visible for testing
  ) where

import Control.Distributed.Process (NodeId)
import Data.List (sortBy)
import Data.List.Split (splitOn)
import Data.Ord (comparing)
import Prelude hiding (log)
import System.HDFS.HDFSClient
import qualified System.Log.Logger as L

{-
 Filters the given nodes to those with any of the file blocks, ordered by the number of file blocks (not regarding individual file block length).
-}
findNodesWithData :: (String, Int) -> String -> [NodeId] -> IO [NodeId]
findNodesWithData config hdfsFilePath nodes = do
  log ("All nodes: " ++ (show nodes))
  hostsWithData <- hdfsFileDistribution config hdfsFilePath
  (if null hostsWithData then L.errorM else L.infoM) L.rootLoggerName ("Hdfs hosts with data: " ++ (show hostsWithData))
  hosts <- readHostNames
  log (show hosts)
  mergedNodeIds <- return $ map fst $ sortOn snd $ merge (matcher hosts) merger nodes hostsWithData
  log ("Merged nodes: " ++ (show mergedNodeIds))
  return mergedNodeIds
    where
      matcher hosts node (hdfsName, _) = nodeMatcher hosts (show node) hdfsName -- HACK uses show to access nodeId data
      merger :: NodeId -> (String, Int) -> (NodeId, Int)
      merger nid (_, n) = (nid, n)

log :: String -> IO ()
log = L.infoM L.rootLoggerName

readHostNames :: IO [(String, String)]
readHostNames = readFile "/etc/hosts" >>= return . parseHostFile
    where
      parseHostFile :: String -> [(String, String)]
      parseHostFile = concat . map parseHosts . filter comments . lines
        where
          comments [] = False
          comments ('#':_) = False
          comments _ = True
          parseHosts :: String -> [(String, String)]
          parseHosts = parseHosts' . splitOn " " . collapseWhites . map replaceTabs
            where
              replaceTabs :: Char -> Char
              replaceTabs '\t' = ' '
              replaceTabs c = c
              collapseWhites :: String -> String
              collapseWhites (' ':' ':rest) = ' ':(collapseWhites rest)
              collapseWhites (c:rest) = c:(collapseWhites rest)
              collapseWhites r = r
              parseHosts' :: [String] -> [(String, String)]
              parseHosts' es = if length es < 2 then [] else map (\v -> (head es,v)) (tail es)

nodeMatcher ::[(String, String)] -> String -> String -> Bool
nodeMatcher hosts node hdfsName = (extractHdfsHost hdfsName) == (extractNodeIdHost node)
  where
    -- HACK: extracts the host name from "nid://localhost:44441:0"
    extractNodeIdHost = lookupHostname . dropWhile (=='/') . head . drop 1 . splitOn ":"
    -- HACK: extracts the host name from "127.0.0.1:50010"
    extractHdfsHost = lookupHostname . head . splitOn ":"
    lookupHostname :: String -> String
    lookupHostname k = maybe k id (lookup k hosts)

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
        merge'' [] = Nothing
        merge'' (b:bs') = if matcher a b then Just (merger a b) else merge'' bs'

{- since 4.8 in Data.List -}
sortOn :: Ord b => (a -> b) -> [a] -> [a] 
sortOn f =
  map snd . sortBy (comparing fst) . map (\x -> let y = f x in y `seq` (y, x))
