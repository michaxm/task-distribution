module DataAccess.HdfsDataSource where

import Control.Exception (catch, SomeException)
import qualified Data.Text.Lazy as TL
import System.HDFS.HDFSClient

import DataAccess.DataSource
import TaskSpawning.TaskTypes (HdfsConfig)

dataSource :: HdfsConfig -> DataSource String
dataSource config = DataSource { _loadEntries = loadEntries' }
  where
    loadEntries' :: String -> IO [String]
    loadEntries' filePath =
      putStrLn targetDescription >>
      hdfsReadFile config filePath `catch` wrapException >>=
      \t -> (print t >> return t) >>=
      return . lines . TL.unpack
      where
        targetDescription = filePath ++ " " ++ (show config)
        wrapException :: SomeException -> IO a
        wrapException e = error $ "Error accessing "++targetDescription++": "++(show e)
