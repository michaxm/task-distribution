{-
 Publishes some functionality from internal modules needed for full binary deployment.
-}
module TaskSpawning.FullBinaryTaskSpawningInterface (executeFullBinaryArg, fullExecutionWithinWorkerProcess) where

import qualified TaskSpawning.TaskSpawning as T

executeFullBinaryArg :: String
executeFullBinaryArg = T.executeFullBinaryArg

fullExecutionWithinWorkerProcess :: String -> FilePath -> IO ()
fullExecutionWithinWorkerProcess = T.fullExecutionWithinWorkerProcess
