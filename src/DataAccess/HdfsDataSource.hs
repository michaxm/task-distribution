module DataAccess.HdfsDataSource where

import System.HDFS.HDFSClient

import DataAccess.DataSource

dataSource :: DataSource String
dataSource = DataSource { _loadEntries = loadEntries' }
  where
    loadEntries' :: String -> IO [String]
    loadEntries' filePath = hdfsListStatus ("localhost", 55555) filePath
