module DataAccess.HdfsDataSource where

import qualified Data.Text.Lazy as TL
import System.HDFS.HDFSClient

import DataAccess.DataSource
import Types.HdfsConfigTypes (HdfsConfig)
import Util.ErrorHandling

dataSource :: HdfsConfig -> DataSource String
dataSource config = DataSource { _loadEntries = loadEntries' }
  where
    loadEntries' :: String -> IO [String]
    loadEntries' filePath =
      putStrLn targetDescription >>
      withErrorPrefix ("Error accessing "++targetDescription) (hdfsReadCompleteFile config filePath) >>=
      return . lines . TL.unpack
      where
        targetDescription = filePath ++ " " ++ (show config)
