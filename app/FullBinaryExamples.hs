module FullBinaryExamples where

import Data.List (isInfixOf)

import Types.TaskTypes

appendDemo :: String -> TaskInput -> TaskResult
appendDemo  demoArg = map (++ (" "++demoArg))

filterDemo :: String -> TaskInput -> TaskResult
filterDemo demoArg = filter (demoArg `isInfixOf`)
