module Control.Distributed.Task.DataAccess.SimpleDataSource where

import qualified Data.ByteString.Lazy.Char8 as BLC

import Control.Distributed.Task.Types.TaskTypes (TaskInput)
import Control.Distributed.Task.Util.Logging
import Control.Distributed.Task.Util.Configuration

loadEntries :: FilePath -> IO TaskInput
loadEntries filePath = do
  pseudoDBPath <- getConfiguration >>= return . _pseudoDBPath
  let path = pseudoDBPath++"/"++filePath
    in do
    logInfo $ "accessing pseudo db at: "++path
    BLC.readFile path >>= return . BLC.lines
