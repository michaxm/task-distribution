module DataAccess.SimpleDataSource (stringSource, dataSource) where

import DataAccess.DataSource

stringSource :: DataSource String
stringSource = DataSource { _loadEntries = loadEntries' id }

dataSource ::  (Read a) => DataSource a
dataSource = DataSource { _loadEntries = loadEntries' read }

loadEntries' :: (String -> a) -> String -> IO [a]
loadEntries' f filePath =
      putStrLn ("accessing " ++ filePath) >>
      readFile filePath >>=
      return . map f . lines
