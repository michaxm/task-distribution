module RemoteExecutable (remoteExecutable) where

import Data.List (isInfixOf)

remoteExecutable :: [String] -> [String]
remoteExecutable = filter ("9404" `isInfixOf`)
