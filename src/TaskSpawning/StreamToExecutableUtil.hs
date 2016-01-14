--{-# LANGUAGE CPP #-}
{-|
 Mostly borrowed from System.Process internals.
|-}
module TaskSpawning.StreamToExecutableUtil (executeExternalWritingToStdIn) where

import Control.Concurrent (forkIO, killThread)
import Control.Concurrent.MVar
import Control.DeepSeq (rnf)
import Control.Exception.Base
import System.Exit (ExitCode(..))
import System.FilePath ()
import System.IO (Handle, hClose, hGetContents, hPutStrLn)
import System.Process

import Types.TaskTypes

executeExternalWritingToStdIn :: FilePath -> [String] -> TaskInput -> IO (ExitCode, String, String)
executeExternalWritingToStdIn progName progArgs taskInput =
  withCreateProcess_ (processOptions progName progArgs) handleProcess
  where
    handleProcess (Just stdin) (Just stdout) _ processHandle = do
      output  <- hGetContents stdout -- forks?
      ex <- withForkWait (evaluate $ rnf output) $ \waitOut -> do
        mapM_ (hPutStrLn stdin) taskInput
        ignoreSigPipe $ hClose stdin
        waitOut
        hClose stdout
        waitForProcess processHandle
      return (ex, output, "")
    handleProcess Nothing _ _ _ = error "stdin not set"
    handleProcess _ _ _ _ = error "expecting at least stdin and stdout to be set"
    processOptions :: FilePath -> [String] -> CreateProcess
    processOptions cmd args = CreateProcess (RawCommand cmd args) Nothing Nothing CreatePipe CreatePipe Inherit False False False

withCreateProcess_ :: CreateProcess -> (Maybe Handle -> Maybe Handle -> Maybe Handle -> ProcessHandle -> IO a) -> IO a
withCreateProcess_ c action =
  bracketOnError (createProcess c) cleanupProcess (\(m_in, m_out, m_err, ph) -> action m_in m_out m_err ph)
  where
    cleanupProcess :: (Maybe Handle, Maybe Handle, Maybe Handle, ProcessHandle) -> IO ()
    cleanupProcess (mb_stdin, mb_stdout, mb_stderr, ph) = do
      terminateProcess ph
      maybe (return ()) (ignoreSigPipe . hClose) mb_stdin
      maybe (return ()) hClose mb_stdout
      maybe (return ()) hClose mb_stderr
      _ <- forkIO (waitForProcess ph >> return ())
      return ()

ignoreSigPipe :: IO () -> IO ()
-- #if defined(__GLASGOW_HASKELL__)
--        ignoreSigPipe = handle $ \e -> case e of
--                                          IOError { ioe_type  = ResourceVanished
--                                                  , ioe_errno = Just ioe }
--                                            | Errno ioe == ePIPE -> return ()
--                                          _ -> throwIO e
-- #else
ignoreSigPipe = id
-- #endif

withForkWait :: IO () -> (IO () ->  IO a) -> IO a
withForkWait async body = do
  waitVar <- newEmptyMVar :: IO (MVar (Either SomeException ()))
  mask $ \restore -> do
    tid <- forkIO $ try (restore async) >>= putMVar waitVar
    let wait = takeMVar waitVar >>= either throwIO return
    restore (body wait) `onException` killThread tid
