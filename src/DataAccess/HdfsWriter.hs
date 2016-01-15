module DataAccess.HdfsWriter where

import Data.List (intersperse)
import Data.List.Split (splitOn)
import qualified Data.Text.Lazy as TL
import System.HDFS.HDFSClient

import Types.HdfsConfigTypes (HdfsConfig)
import Util.ErrorHandling
import Util.Logging

writeEntriesToFile :: HdfsConfig -> String -> [String] -> IO ()
writeEntriesToFile config path entries = do
  logInfo $ "writing to hdfs: " ++ path
  withErrorPrefix ("Error writing to "++path) $ writeToHdfsCreatingDirs
  logInfo $ "writing complete."
  where
    formatEntries =  TL.pack . concat . intersperse "\n"
    writeToHdfsCreatingDirs :: IO ()
    writeToHdfsCreatingDirs = hdfsWriteNewFile config path $ formatEntries entries

{-|
 Managing when the hdfs://-prefix is included and when not is suboptimal for now. This helper method stripping that part should
 become obsolete.
|-}
stripHDFSPartOfPath :: String -> String
stripHDFSPartOfPath = concat . intersperse "/" . drop 3 . splitOn "/"
