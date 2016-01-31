module Control.Distributed.Task.DataAccess.HdfsListing (
  listFiles, listBlockDistribution
  ) where

import Control.Exception (catch, SomeException)
import qualified Data.ByteString.Char8 as BC
import qualified Data.Hadoop.Configuration as HRPC
import qualified Data.Hadoop.Types as HRPC
import qualified Network.Hadoop.Hdfs as HRPC
import qualified Data.Text as T
import qualified Data.Vector as V

import Control.Distributed.Task.Util.Configuration

import Util.Logging

listFiles :: String -> IO [String]
listFiles path = do
  logInfo $ "listing files at: "++path
  files <- runHdfsWithConfig $ HRPC.getListing' $ BC.pack path
  return $ V.toList $ V.map convert $ files
  where
    convert :: HRPC.FileStatus -> String
    convert = BC.unpack . HRPC.fsPath

listBlockDistribution :: String -> IO [(String, Int)]
listBlockDistribution path = do
  logInfo $ "listing file blocks for: "++path
  locs <- runHdfsWithConfig $ HRPC.getBlockLocations $ BC.pack path
  case V.length locs of
   0 -> error $ "no such file: "++path
   1 -> return $ convert $ HRPC.fsLocations $ locs V.! 0
   _ -> error $ "is not a regular file: "++path
  where
    convert :: HRPC.FsLocations -> [(String, Int)]
    convert HRPC.NotRequested = error "block locations not requested?"
    convert (HRPC.FsLocations ls) =
      let blockLocations = V.toList $ V.map V.toList $ V.map snd ls :: [[HRPC.BlockLocation]]
          hostnames = map (T.unpack . fst) $ concat blockLocations :: [String]
      in groupCount hostnames

groupCount :: (Eq key) => [key] -> [(key, Int)]
groupCount = foldr groupCount' []
  where
    groupCount' :: (Eq key) => key -> [(key, Int)] -> [(key, Int)]
    groupCount' key [] = [(key, 1)]
    groupCount' key (next:collected) = if (fst next == key) then (fst next, snd next +1):collected else next:(groupCount' key collected)

runHdfsWithConfig :: HRPC.Hdfs a -> IO a
runHdfsWithConfig action = do
  hdfsConfig <- mkHdfsConfig `catch` warnAndUseDefault
  (maybe HRPC.runHdfs HRPC.runHdfs' hdfsConfig) action
  where
    warnAndUseDefault :: SomeException -> IO (Maybe HRPC.HadoopConfig)
    warnAndUseDefault e = logWarn ("failed to load config, trying system defaults: "++show e) >> return Nothing
    mkHdfsConfig :: IO (Maybe HRPC.HadoopConfig)
    mkHdfsConfig = do
      user <- HRPC.getHadoopUser
      (host, port) <- getConfiguration >>= return . _hdfsConfig
      return $ Just $ HRPC.HadoopConfig user [HRPC.Endpoint (T.pack host) port] Nothing
