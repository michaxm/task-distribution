module Control.Distributed.Task.DataAccess.SimpleDataSource where

import qualified Data.ByteString.Lazy.Char8 as BLC

import Control.Distributed.Task.Types.TaskTypes (TaskInput)

loadEntries :: FilePath -> IO TaskInput
loadEntries filePath =
      putStrLn ("accessing " ++ filePath) >>
      BLC.readFile filePath >>=
      return . BLC.lines
