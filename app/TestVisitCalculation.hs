import Data.List (intersperse)
import System.Environment (getArgs)

import VisitCalculation

main :: IO ()
main = do
  args <- getArgs
  case args of
   [filePath] -> calculateVisits' filePath
   _ -> error "missing input file"

calculateVisits' :: String -> IO ()
calculateVisits' filePath = do
  contents <- readFile filePath
  visits <- return $ calculateVisits $ lines contents
  writeFile "tmp/out" $ concat $ intersperse "\n" visits
