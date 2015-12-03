module DataAccess.HdfsDataSource where

import qualified Data.Text.Lazy as TL
import System.HDFS.HDFSClient

import DataAccess.DataSource
import TaskSpawning.TaskTypes (HdfsConfig)
import Util.ErrorHandling
--import Util.Logging

dataSource :: HdfsConfig -> DataSource String
dataSource config = DataSource { _loadEntries = loadEntries' }
  where
    loadEntries' :: String -> IO [String]
    loadEntries' filePath =
      putStrLn targetDescription >>
      withErrorPrefix ("Error accessing "++targetDescription) (hdfsReadCompleteFile config filePath) >>=
--      (\contents -> logDebug (TL.unpack contents) >> return contents) >>=
      return . lines . TL.unpack
      where
        targetDescription = filePath ++ " " ++ (show config)
