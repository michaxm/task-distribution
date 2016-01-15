module Util.FileUtil where

import Data.List (intersperse)
import Data.List.Split (splitOn)

getFileNamePart :: FilePath -> String
getFileNamePart = snd . splitBasePath
  --let parts = splitOn "/" path in if null parts then "" else parts !! (length parts -1)

splitBasePath :: FilePath -> (FilePath, String)
splitBasePath path = let parts = splitOn "/" path in
  if null parts
  then ("", "")
  else (concat $ intersperse "/" $ take (length parts -1) parts,
        parts !! (length parts -1))
