module DataAccess.HdfsDataSource where

import qualified Data.Text.Lazy as TL
import System.HDFS.HDFSClient

import Types.TaskTypes (TaskInput)
import Types.HdfsConfigTypes (HdfsLocation)
import Util.ErrorHandling

loadEntries :: HdfsLocation -> IO TaskInput
loadEntries hdfsLocation =
  putStrLn targetDescription >>
  withErrorPrefix ("Error accessing "++ targetDescription) (uncurry hdfsReadCompleteFile hdfsLocation) >>=
  return . lines . TL.unpack
  where
    targetDescription = show hdfsLocation
