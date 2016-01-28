module DataAccess.SimpleDataSource where

import qualified Data.ByteString.Lazy.Char8 as BLC
import Types.TaskTypes (TaskInput)

loadEntries :: FilePath -> IO TaskInput
loadEntries filePath =
      putStrLn ("accessing " ++ filePath) >>
      BLC.readFile filePath >>=
      return . BLC.lines
