{-|
 Simple binary file storage for temporary files.
 Stores under <tempdir>/temp-binary-storage/
|-}
module TaskSpawning.BinaryStorage (calculateHash, put, get, clearAll) where

import qualified Data.ByteString.Lazy as BL
import Data.Hashable (hash)
import System.Directory

calculateHash :: BL.ByteString -> Int
calculateHash = hash

put :: Int -> BL.ByteString -> IO ()
put fileHash content = do
  (filePath, fileExists) <- getFilePath fileHash
  if fileExists
    then return ()
    else BL.writeFile filePath content

get :: Int -> IO (Maybe FilePath)
get fileHash = do
  (filePath, fileExists) <- getFilePath fileHash
  if fileExists
    then return $ Just filePath
    else return $ Nothing

clearAll :: IO ()
clearAll = do
  homeDir <- getHomeDir
  removeFile homeDir

getFilePath :: Int -> IO (FilePath, Bool)
getFilePath fileHash = do
  homeDir <- getHomeDir
  filePath <- return $ homeDir ++ "/" ++ (show fileHash)
  fileExists <- doesFileExist filePath
  return (filePath, fileExists)

getHomeDir :: IO FilePath
getHomeDir = do
  sysTempDir <- getTemporaryDirectory
  homeDir <- return $ sysTempDir ++ "/temp-binary-storage/"
  hDExists <- doesDirectoryExist homeDir
  if hDExists
    then return ()
    else createDirectory homeDir
  return homeDir
