module DataAccess.HdfsDataSource where

import qualified Data.Text.Lazy as TL
import System.HDFS.HDFSClient

import DataAccess.DataSource
import TaskSpawning.TaskTypes (HdfsConfig)

dataSource :: HdfsConfig -> DataSource String
dataSource config = DataSource { _loadEntries = loadEntries' }
  where
    loadEntries' :: String -> IO [String]
    loadEntries' filePath =
      putStrLn (filePath ++ " " ++ (show config)) >>
      hdfsReadFile config filePath >>=
      \t -> (print t >> return t) >>=
      return . lines . TL.unpack
