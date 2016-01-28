module DataAccess.HdfsDataSource (loadEntries, copyToLocal) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.Hadoop.Configuration
import Data.Hadoop.Types
import qualified Data.Text as T
import Network.Hadoop.Hdfs
import Network.Hadoop.Read
import System.Directory (doesFileExist)
import System.IO (IOMode(..), withBinaryFile)

import Types.HdfsConfigTypes (HdfsConfig)
import Types.TaskTypes (TaskInput)
import Types.HdfsConfigTypes (HdfsLocation)
import Util.ErrorHandling
import Util.Logging

loadEntries :: HdfsLocation -> IO TaskInput
loadEntries hdfsLocation = do
  logInfo $ "loading: " ++ targetDescription
  withErrorPrefix ("Error accessing "++ targetDescription) doLoad >>= return . BLC.lines
  where
    targetDescription = show hdfsLocation
    doLoad = readHdfsFile hdfsLocation

readHdfsFile :: HdfsLocation -> IO BL.ByteString
readHdfsFile (hdfsConfig, path) = do
  logInfo $ "now loading" ++ show (hdfsConfig, path)
  config <- buildConfig hdfsConfig
  chunks <- readAllChunks readerAction config path
  logInfo "loading chunks finished"
  return $ BLC.fromChunks $ reverse chunks
  where
    readerAction :: [B.ByteString] -> BC.ByteString -> IO [B.ByteString]
    readerAction collected nextChunk = return $ nextChunk:collected

buildConfig :: HdfsConfig -> IO HadoopConfig
buildConfig (host, port) = do
  user <- getHadoopUser
  return $ HadoopConfig user [(Endpoint (T.pack host) port)] Nothing

readAllChunks :: ([B.ByteString] -> BC.ByteString -> IO [B.ByteString]) -> HadoopConfig -> String -> IO [B.ByteString]
readAllChunks action config path =  do
  readHandle_ <- runHdfs' config $ openRead $ BC.pack path
  case readHandle_ of
   (Just readHandle) -> hdfsFoldM action [] readHandle
   Nothing -> error "no read handle"

copyToLocal :: HdfsLocation -> FilePath -> IO ()
copyToLocal (hdfsConfig, path) destFile = do
  logInfo $ "now copying from " ++ show hdfsConfig
  config <- buildConfig hdfsConfig
  copyHdfsFileToLocal destFile (Just config) path

copyHdfsFileToLocal :: FilePath -> Maybe HadoopConfig -> String -> IO ()
copyHdfsFileToLocal destFile config path = do
  destFileExists <- doesFileExist destFile
  if destFileExists
    then error $ destFile++" exists"
    else withBinaryFile destFile WriteMode $ \h -> withHdfsReader (BC.hPut h) config path

withHdfsReader :: (BC.ByteString -> IO ()) -> Maybe HadoopConfig -> String -> IO ()
withHdfsReader action config path =  do
  readHandle_ <- maybe runHdfs runHdfs' config $ openRead $ BC.pack path
  case readHandle_ of
   (Just readHandle) -> hdfsMapM_ action readHandle
   Nothing -> error "no read handle"
