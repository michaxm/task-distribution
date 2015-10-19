{-# LANGUAGE Rank2Types #-}
module DataAccess.DataSource where

data DataSource a = DataSource {
  _loadEntries :: Read a => String -> IO [a]
}
