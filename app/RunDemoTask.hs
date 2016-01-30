import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Control.Concurrent.Async
import Data.List
import qualified Codec.Compression.GZip as GZip
import System.Environment (getArgs)

import DemoTask

main :: IO ()
main = do
  args <- getArgs
  case args of
   [] -> error $ "usage: <filename|[filename]>"
   [filename] -> runSequential filename
   filenames -> runParallel filenames

runParallel :: [FilePath] -> IO ()
runParallel filenames = do
  results <- mapM (async . runSequential) filenames
  mapM_ wait results

runSequential :: FilePath -> IO ()
runSequential filename = BL.readFile filename
                         >>= return . calculateRatio . BLC.lines . unzipIfNecessary
                         >>= print
  where
    isZipped = ".gz" `isSuffixOf` filename
    unzipIfNecessary = if isZipped then GZip.decompress else id
