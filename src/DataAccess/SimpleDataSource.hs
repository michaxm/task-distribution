module DataAccess.SimpleDataSource where

import Types.TaskTypes (TaskInput)

loadEntries :: FilePath -> IO TaskInput
loadEntries filePath =
      putStrLn ("accessing " ++ filePath) >>
      readFile filePath >>=
      return . lines
