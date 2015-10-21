module DataAccess.HdfsDataSource where

import qualified Data.Text.Lazy as TL
import System.HDFS.HDFSClient

import DataAccess.DataSource

dataSource :: DataSource String
dataSource = DataSource { _loadEntries = loadEntries' }
  where
    loadEntries' :: String -> IO [String]
    loadEntries' filePath = hdfsReadFile ("localhost", 55555) filePath >>= \t -> (print t >> return t) >>= return . lines . TL.unpack
