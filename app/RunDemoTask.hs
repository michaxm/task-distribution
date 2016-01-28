import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.List
import qualified Codec.Compression.GZip as GZip
import System.Environment (getArgs)

import DemoTask

main :: IO ()
main = do
  args <- getArgs
  case args of
   [filename] -> (if ".gz" `isSuffixOf` filename then processZippedInput else processPlainInput) filename
   _ -> error $ "usage: <filename>"

processZippedInput :: FilePath -> IO ()
processZippedInput filename = BL.readFile filename >>= return . calculateRatio . BLC.lines . GZip.decompress >>= print

processPlainInput :: FilePath -> IO ()
processPlainInput filename = BL.readFile filename >>= return . calculateRatio . BLC.lines >>= print
