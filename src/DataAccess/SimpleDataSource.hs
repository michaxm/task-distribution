module DataAccess.SimpleDataSource where

import DataAccess.DataSource

dataSource :: DataSource String
dataSource = DataSource { _loadEntries = loadEntries' }
  where
    loadEntries' :: {-(Read a) => -}String -> IO [String]
    loadEntries' filePath = readFile filePath >>= return . map id . lines
