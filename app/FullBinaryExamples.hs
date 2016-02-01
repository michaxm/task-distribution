module FullBinaryExamples where

import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.List (isInfixOf)

import Control.Distributed.Task.Types.TaskTypes

appendDemo :: String -> TaskInput -> TaskResult
appendDemo  demoArg = map (\str -> str `BLC.append` (BLC.pack $ " "++demoArg))

filterDemo :: String -> TaskInput -> TaskResult
filterDemo demoArg = filter (((BC.pack demoArg) `BC.isInfixOf`) . BC.concat . BLC.toChunks )
