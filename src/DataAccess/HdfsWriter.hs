module DataAccess.HdfsWriter where

--import qualified Codec.Compression.GZip as GZip
--import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.List (intersperse)
import Data.List.Split (splitOn)
import qualified Data.Text.Lazy as TL
import System.HDFS.HDFSClient

import Types.TaskTypes
import Types.HdfsConfigTypes (HdfsConfig)
import Util.ErrorHandling
import Util.Logging

writeEntriesToHdfs :: Bool -> HdfsConfig -> String -> TaskResult -> IO ()
writeEntriesToHdfs _ config path entries = do
  logInfo $ "writing to HDFS: " ++ path
  logInfo $ show entries
  withErrorPrefix ("Error writing to "++path) $ writeToHdfsCreatingDirs
  logInfo $ "writing to HDFS complete"
  where
    formatEntries =  TL.pack
-- . (if not zipIt then BLC.unpack . GZip.compress . BLC.pack else id)
                     . concat . intersperse "\n"
    writeToHdfsCreatingDirs :: IO ()
    writeToHdfsCreatingDirs = hdfsWriteNewFile config path $ formatEntries entries

{-|
 Managing when the hdfs://-prefix is included and when not is suboptimal for now. This helper method stripping that part should
 become obsolete.
|-}
stripHDFSPartOfPath :: String -> String
stripHDFSPartOfPath = concat . intersperse "/" . drop 3 . splitOn "/"
