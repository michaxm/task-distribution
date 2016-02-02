module Control.Distributed.Task.DataAccess.HdfsDataSource (supplyPathWithConfig, loadEntries, copyToLocal) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Codec.Compression.GZip as GZip
import qualified Data.Hadoop.Configuration as HDFS
import qualified Data.Hadoop.Types as HDFS
import Data.List (isSuffixOf)
import qualified Data.Text as T
import Network.Hadoop.Hdfs
import Network.Hadoop.Read
import System.Directory (doesFileExist)
import System.IO (IOMode(..), withBinaryFile)

import Control.Distributed.Task.Types.TaskTypes (TaskInput)
import Control.Distributed.Task.Types.HdfsConfigTypes
import Control.Distributed.Task.Util.Configuration
import Control.Distributed.Task.Util.ErrorHandling
import Control.Distributed.Task.Util.Logging

supplyPathWithConfig :: HdfsPath -> IO HdfsLocation
supplyPathWithConfig p = do
  config <- getConfiguration
  return (_hdfsConfig config, p)

loadEntries :: HdfsLocation -> IO TaskInput
loadEntries hdfsLocation = do
  logInfo $ "loading: " ++ targetDescription
  withErrorPrefix ("Error accessing "++ targetDescription) doLoad >>= return . BLC.lines . unzipIfNecessary
  where
    targetDescription = show hdfsLocation
    doLoad = readHdfsFile hdfsLocation
    unzipIfNecessary = if ".gz" `isSuffixOf` (snd hdfsLocation) then GZip.decompress else id

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

buildConfig :: HdfsConfig -> IO HDFS.HadoopConfig
buildConfig (host, port) = do
  user <- HDFS.getHadoopUser
  return $ HDFS.HadoopConfig user [(HDFS.Endpoint (T.pack host) port)] Nothing

readAllChunks :: ([B.ByteString] -> BC.ByteString -> IO [B.ByteString]) -> HDFS.HadoopConfig -> String -> IO [B.ByteString]
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

copyHdfsFileToLocal :: FilePath -> Maybe HDFS.HadoopConfig -> String -> IO ()
copyHdfsFileToLocal destFile config path = do
  destFileExists <- doesFileExist destFile
  if destFileExists
    then error $ destFile++" exists"
    else withBinaryFile destFile WriteMode $ \h -> withHdfsReader (BC.hPut h) config path

withHdfsReader :: (BC.ByteString -> IO ()) -> Maybe HDFS.HadoopConfig -> String -> IO ()
withHdfsReader action config path =  do
  readHandle_ <- maybe runHdfs runHdfs' config $ openRead $ BC.pack path
  case readHandle_ of
   (Just readHandle) -> hdfsMapM_ action readHandle
   Nothing -> error "no read handle"
