module Control.Distributed.Task.TaskSpawning.TaskSpawningTypes (
  NominalDiffTime, -- reexport
  IOHandling(..), packIOHandling, unpackIOHandling
  ) where

import Data.Time.Clock (NominalDiffTime)

import Control.Distributed.Task.TaskSpawning.TaskDefinition
import Data.List (intersperse)
import Data.List.Split (splitOn)

-- task communication

data IOHandling = IOHandling [DataDef] ResultDef

packIOHandling :: IOHandling -> String
packIOHandling (IOHandling dataDefs resultDef) = (packResultDef resultDef)++"|"++(concat $ intersperse "|" $ map packDataDef dataDefs)
unpackIOHandling :: String -> IOHandling
unpackIOHandling s = let fields = splitOn "|" s
                     in if length fields < 2
                        then error $ "incompliete io handling: "++s
                        else IOHandling (map unpackDataDef $ tail fields) (unpackResultDef $ head fields)

splitOnExpect :: Int -> String -> String -> [String]
splitOnExpect len delim s = let es = splitOn delim s
                            in if (length es) /= len
                               then error $ "unexpected segment amount: "++s
                               else es

packDataDef :: DataDef -> String
packDataDef (HdfsData p) = "HdfsData:"++p
packDataDef (PseudoDB p) = "PseudoDB:"++p
unpackDataDef :: String -> DataDef
unpackDataDef s = let es = splitOnExpect 2 ":" s
                  in case es !! 0 of
                  "HdfsData" -> HdfsData $ es !! 1
                  "PseudoDB" -> PseudoDB $ es !! 1
                  str -> error $ "unknown data source value: " ++ str

packResultDef :: ResultDef -> String
packResultDef ReturnAsMessage = "ReturnAsMessage"
packResultDef (HdfsResult p s z) = "HdfsResult:"++p++":"++s++":"++show z
packResultDef ReturnOnlyNumResults = "ReturnOnlyNumResults"
unpackResultDef :: String -> ResultDef
unpackResultDef s = let es = splitOn ":" s
                    in if null es
                       then error $ "unknown segment amount"++s
                       else case es !! 0 of
                       "ReturnAsMessage" -> ReturnAsMessage
                       "ReturnOnlyNumResults" -> ReturnOnlyNumResults
                       "HdfsResult" -> if length es /= 4
                                       then error $ "unknown segment amount"++s
                                       else HdfsResult (es !! 1) (es !! 2) (read $ es !! 3)
                       str -> error $ "unknown return action value: " ++ str
