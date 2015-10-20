module ClusterComputing.TaskDistribution (
  startWorkerNode,
  executeDistributed,
  shutdownWorkerNodes) where

import Control.Distributed.Process (Process, ProcessId, NodeId,
                                    say, getSelfPid, spawn, send, expect, liftIO, onException,
                                    RemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet (initializeBackend, startMaster, startSlave, terminateSlave)
import qualified Control.Distributed.Process.Serializable as PS
import qualified Control.Distributed.Static as S
import Control.Distributed.Process.Closure (staticDecode)
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Serializable (Serializable)
import Control.Monad (forM_)
import qualified Data.Binary as B (encode)
import qualified Data.Rank1Dynamic as R1 (toDynamic)

import ClusterComputing.TaskTransport
import TaskSpawning.TaskSpawning (processTask)
import TaskSpawning.TaskTypes

-- BEGIN bindings for node communication
workerTask :: TaskTransport -> Process () -- TODO: have a node local config?
workerTask (TaskTransport masterProcess taskName taskDef dataSpec) = do
  say $ "processing: " ++ taskName
  sendError
  result <- liftIO (processTask taskDef dataSpec) `onException` sendError
  send masterProcess (Right result :: Either String TaskResult)
  say $ "processing done"
  where
    sendError = send masterProcess (Left "ouch." :: Either String TaskResult)

-- template haskell vs. its result
{-# LANGUAGE TemplateHaskell, DeriveDataTypeable, DeriveGeneric #-}
--remotable ['workerTask] 
-- ======>
workerTask__static :: S.Static (TaskTransport -> Process ())
workerTask__static = S.staticLabel "ClusterComputing.TaskDistribution.workerTask"
workerTask__sdict :: S.Static (PS.SerializableDict TaskTransport)
workerTask__sdict = S.staticLabel "ClusterComputing.TaskDistribution.workerTask__sdict"
--workerTask__tdict :: S.Static (PS.SerializableDict ())
--workerTask__tdict = S.staticLabel "ClusterComputing.TaskDistribution.workerTask__tdict"
__remoteTable :: S.RemoteTable -> S.RemoteTable
__remoteTable =
  ((S.registerStatic "ClusterComputing.TaskDistribution.workerTask" (R1.toDynamic workerTask))
   . ((S.registerStatic "ClusterComputing.TaskDistribution.workerTask__sdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict TaskTransport)))
      . (S.registerStatic "ClusterComputing.TaskDistribution.workerTask__tdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict ())))))

workerTaskClosure :: TaskTransport -> S.Closure (Process ())
workerTaskClosure =
-- $(mkClosure 'workerTask)
-- ======>
   ((S.closure
    (workerTask__static
     `S.staticCompose`
     (staticDecode
      workerTask__sdict)))
   . B.encode)

rtable :: RemoteTable
rtable = __remoteTable $ initRemoteTable
-- END bindings for node communication

startWorkerNode :: String -> IO ()
startWorkerNode workerNumber = do
  backend <- initializeBackend "localhost" ("4444" ++ workerNumber) rtable
  putStrLn "initializing worker"
  startSlave backend

executeDistributed :: (Serializable a) => Int -> String -> ([a] -> IO ()) -> IO ()
executeDistributed numDBs modulePath resultProcessor = do
  backend <- initializeBackend "localhost" "44440" rtable
  startMaster backend $ \workerNodes -> do
    result <- executeOnNodes numDBs modulePath workerNodes
    liftIO $ resultProcessor result

executeOnNodes :: (Serializable a) => Int -> String -> [NodeId] -> Process [a]
executeOnNodes numDBs modulePath workerNodes = do
  if null workerNodes
    then say "no workers => no results (ports open?)" >> return [] 
    else executeOnNodes' numDBs modulePath workerNodes

executeOnNodes' :: (Serializable a) => Int -> String -> [NodeId] -> Process [a]
executeOnNodes' numDBs modulePath workerNodes = do
  masterProcess <- getSelfPid
  forM_ (zip [1..numDBs] (cycle workerNodes)) (spawnWorkerProcess masterProcess)
  collectResults numDBs []
    where
      spawnWorkerProcess :: ProcessId -> (Int, NodeId) -> Process ()
      spawnWorkerProcess masterProcess (numDB, workerNode) = do
        moduleContent <- liftIO $ readFile modulePath
        _workerProcessId <- spawn workerNode (workerTaskClosure (TaskTransport masterProcess "myTask" (SourceCodeModule moduleContent) {-(HdfsData "/")-} (PseudoDB numDB)))
        return ()
      collectResults :: (Serializable a) => Int -> [[a]] -> Process [a]
      collectResults 0 res = return $ concat $ reverse res
      collectResults n res = do
        say $ "expecting " ++ (show n) ++ " more responses"
        next <- expect
        case next of
          (Left msg) -> say (msg ++ " failure not handled ...") >> collectResults (n-1) res
          (Right nextChunk) -> collectResults (n-1) (nextChunk:res)

shutdownWorkerNodes :: IO ()
shutdownWorkerNodes =  do
  backend <- initializeBackend "localhost" "44440" rtable
  startMaster backend $ \workerNodes -> do
    say $ "found " ++ (show $ length workerNodes) ++ " worker nodes, shutting them down"
    forM_ workerNodes terminateSlave
    -- try terminateAllSlaves instead?

