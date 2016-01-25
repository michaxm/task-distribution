module DataAccess.HdfsDataSource where

import qualified Data.Text.Lazy as TL
import System.HDFS.HDFSClient

import Types.TaskTypes (TaskInput)
import Types.HdfsConfigTypes (HdfsLocation)
import Util.ErrorHandling
import Util.Logging

loadEntries :: HdfsLocation -> IO TaskInput
loadEntries hdfsLocation = do
  logInfo $ "loading: " ++ targetDescription
  withErrorPrefix ("Error accessing "++ targetDescription) (uncurry hdfsReadCompleteFile hdfsLocation) >>= return . lines . TL.unpack
  where
    targetDescription = show hdfsLocation
