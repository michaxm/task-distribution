import System.Environment (getArgs)

import Control.Distributed.Task.Distribution.TaskDistribution (startSlaveNode)

main :: IO ()
main = do
  args <- getArgs
  case args of
   [slaveHost, slavePort] -> startSlaveNode (slaveHost, (read slavePort))
   _ -> error "syntax: <host> <port>"
