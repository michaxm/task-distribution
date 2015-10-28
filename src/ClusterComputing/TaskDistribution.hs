module ClusterComputing.TaskDistribution (
  startWorkerNode,
  executeDistributed,
  shutdownWorkerNodes) where

import Control.Distributed.Process (Process, ProcessId, NodeId,
                                    say, getSelfPid, spawn, send, expect, liftIO, catch,
                                    RemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet (initializeBackend, startMaster, startSlave, terminateSlave)
import qualified Control.Distributed.Process.Serializable as PS
import qualified Control.Distributed.Static as S
import Control.Distributed.Process.Closure (staticDecode)
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Serializable (Serializable)
import Control.Exception.Base (SomeException)
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
  result <- liftIO (processTask taskDef dataSpec >>= return . Right) `catch` buildError
  send masterProcess (result :: Either String TaskResult)
  where
    buildError :: SomeException -> Process (Either String TaskResult)
    buildError e = return $ Left $ "Task execution failed: " ++ (format $ show e)
      where
        format [] = []
        format ('\\':'n':'\\':'t':rest) = "\n\t" ++ (format rest)
        format (x:rest) = x:[] ++ (format rest)

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

type NodeConfig = (String, String)

startWorkerNode :: NodeConfig -> IO ()
startWorkerNode (host, port) = do
  backend <- initializeBackend host port rtable
  putStrLn "initializing worker"
  startSlave backend

executeDistributed :: (Serializable a) => NodeConfig -> TaskDef -> [DataSpec] -> ([a] -> IO ()) -> IO () -- FIXME [DataSpec] is a list only because of testing purposes for now (select other data on different nodes)
executeDistributed (host, port) taskDef dataSpecs resultProcessor = do
  backend <- initializeBackend host port rtable
  startMaster backend $ \workerNodes -> do
    result <- executeOnNodes taskDef dataSpecs workerNodes
    liftIO $ resultProcessor result

executeOnNodes :: (Serializable a) => TaskDef -> [DataSpec] -> [NodeId] -> Process [a]
executeOnNodes taskDef dataSpecs workerNodes = do
  if null workerNodes
    then say "no workers => no results (ports open?)" >> return [] 
    else executeOnNodes' taskDef dataSpecs workerNodes

executeOnNodes' :: (Serializable a) => TaskDef -> [DataSpec] -> [NodeId] -> Process [a]
executeOnNodes' taskDef dataSpecs workerNodes = do
  masterProcess <- getSelfPid
  forM_ (zip dataSpecs (cycle workerNodes)) (spawnWorkerProcess masterProcess) -- FIXME a worker node should not be allocated twice, let it have an 'occupied' state
  collectResults (length dataSpecs) []
    where
      spawnWorkerProcess :: ProcessId -> (DataSpec, NodeId) -> Process ()
      spawnWorkerProcess masterProcess (dataSpec, workerNode) = do
        _workerProcessId <- spawn workerNode (workerTaskClosure (TaskTransport masterProcess "myTask" taskDef dataSpec))
        return ()
      collectResults :: (Serializable a) => Int -> [[a]] -> Process [a]
      collectResults 0 res = return $ concat $ reverse res
      collectResults n res = do --FIXME handler for unexpected types
        say $ "expecting " ++ (show n) ++ " more responses"
        next <- expect
        case next of
          (Left msg) -> say (msg ++ " failure not handled ...") >> collectResults (n-1) res
          (Right nextChunk) -> say "got a result" >> collectResults (n-1) (nextChunk:res)

shutdownWorkerNodes :: NodeConfig -> IO ()
shutdownWorkerNodes (host, port) = do
  backend <- initializeBackend host port rtable
  startMaster backend $ \workerNodes -> do
    say $ "found " ++ (show $ length workerNodes) ++ " worker nodes, shutting them down"
    forM_ workerNodes terminateSlave
    -- try terminateAllSlaves instead?

