module DataAccess.HdfsWriter where

import Data.List (intersperse)
import qualified Data.Text.Lazy as TL
import System.HDFS.HDFSClient

import Types.HdfsConfigTypes (HdfsConfig)
import Util.ErrorHandling
import Util.Logging

writeEntriesToFile :: HdfsConfig -> String -> [String] -> IO ()
writeEntriesToFile config path entries = do
  logInfo $ "writing to hdfs: " ++ path
  withErrorPrefix ("Error writing to "++path) $ hdfsWriteNewFile config path $ formatEntries entries
  logInfo $ "writing complete."
  where
    formatEntries =  TL.pack . concat . intersperse "\n"
