import Data.List (intersperse)
import System.Environment (getArgs)

import qualified Data.ByteString.Lazy.Char8 as BLC
import VisitCalculation

main :: IO ()
main = do
  args <- getArgs
  case args of
   [filePath] -> calculateVisits' filePath
   _ -> error "missing input file"

calculateVisits' :: String -> IO ()
calculateVisits' filePath = do
  contents <- BLC.readFile filePath
  visits <- return $ calculateVisits $ BLC.lines contents
  BLC.writeFile "tmp/out" $ BLC.concat $ intersperse (BLC.pack "\n") visits
