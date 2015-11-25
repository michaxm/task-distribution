import qualified Data.ByteString.Char8 as BLC
import System.Environment (getArgs)

import RemoteExecutable

main :: IO ()
main = do
  args <- getArgs
  case args of
   [taskInputFilePath] -> do
     fileContents <- BLC.readFile taskInputFilePath
     executeF (read $ BLC.unpack fileContents)
   _ -> error "Syntax: <task input file path>\n"

executeF :: [String] -> IO ()
executeF i = do
  res <- return $ remoteExecutable i
  print res
