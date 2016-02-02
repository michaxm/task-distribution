module Control.Distributed.Task.TaskSpawning.TaskSpawningTypes (
  CompleteTaskResult, TaskResultWrapper(..), SingleTaskRunStatistics,
  IOHandling(..), packIOHandling, unpackIOHandling
  ) where

import Data.Time.Clock (NominalDiffTime)

import Control.Distributed.Task.TaskSpawning.TaskDefinition
import Control.Distributed.Task.Types.TaskTypes
import Data.List (intersperse)
import Data.List.Split (splitOn)

-- task results with runtime statistics

type CompleteTaskResult = (TaskResultWrapper, SingleTaskRunStatistics)
data TaskResultWrapper = DirectResult TaskResult | StoredRemote
type SingleTaskRunStatistics = (NominalDiffTime, NominalDiffTime)

-- task communication

data IOHandling = IOHandling [DataDef] ResultDef

packIOHandling :: IOHandling -> String
packIOHandling (IOHandling dataDefs resultDef) = (packResultDef resultDef)++"\n"++(concat $ intersperse "\n" $ map packDataDef dataDefs)
unpackIOHandling :: String -> IOHandling
unpackIOHandling s = let sLines = lines s
                         firstLine = if length sLines < 1 then error "empty io handling" else head sLines
                     in IOHandling (map unpackDataDef $ tail sLines) (unpackResultDef firstLine)

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
