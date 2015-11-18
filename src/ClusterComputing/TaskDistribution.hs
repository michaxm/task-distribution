module ClusterComputing.TaskDistribution (
  startWorkerNode,
  executeDistributed,
  showWorkerNodes,
  showWorkerNodesWithData,
  shutdownWorkerNodes) where

import Control.Distributed.Process (Process, ProcessId, NodeId,
                                    say, getSelfPid, spawn, send, expect, catch,
                                    RemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet (initializeBackend, startMaster, startSlave, terminateSlave)
import qualified Control.Distributed.Process.Serializable as PS
import qualified Control.Distributed.Static as S
import Control.Distributed.Process.Closure (staticDecode)
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Serializable (Serializable)
import Control.Exception.Base (SomeException)
import Control.Monad (forM_)
import Control.Monad.IO.Class
import qualified Data.Binary as B (encode)
import Data.List (delete)
import qualified Data.Rank1Dynamic as R1 (toDynamic)

import ClusterComputing.DataLocality (findNodesWithData)
import ClusterComputing.TaskTransport
import TaskSpawning.TaskSpawning (processTask)
import TaskSpawning.TaskTypes

{-
 The bits in this file are arranged so that the less verbose Template Haskell version would work. That version is not used due to incompability
 issues between packman and Template Haskell though, but is retained in the commens for documentational purposes.
-}

-- BEGIN bindings for node communication
{-
 This is the final building block of the worker task execution, calling TaskSpawning.processTask.
-}
workerTask :: TaskTransport -> Process () -- TODO: have a node local config?
workerTask (TaskTransport masterProcess taskMetaData taskDef dataSpec) = do
  say $ "processing: " ++ taskName
  result <- liftIO (processTask taskDef dataSpec >>= return . Right) `catch` buildError
  say $ "processing done for: " ++ taskName
  send masterProcess (taskMetaData, result :: Either String TaskResult)
  where
    taskName = _taskName taskMetaData
    buildError :: SomeException -> Process (Either String TaskResult)
    buildError e = return $ Left $ "Task execution (for: "++taskName++") failed: " ++ (format $ show e)
      where
        format [] = []
        format ('\\':'n':'\\':'t':rest) = "\n\t" ++ (format rest)
        format (x:rest) = x:[] ++ (format rest)

-- template haskell vs. its result
-- needs: {-# LANGUAGE TemplateHaskell, DeriveDataTypeable, DeriveGeneric #-}
--remotable ['workerTask] 
-- ======>
workerTask__static :: S.Static (TaskTransport -> Process ())
workerTask__static = S.staticLabel "ClusterComputing.TaskDistribution.workerTask"
workerTask__sdict :: S.Static (PS.SerializableDict TaskTransport)
workerTask__sdict = S.staticLabel "ClusterComputing.TaskDistribution.workerTask__sdict"
-- not used for now, removed due to warnings as errors
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

type NodeConfig = (String, Int)

startWorkerNode :: NodeConfig -> IO ()
startWorkerNode (host, port) = do
  backend <- initializeBackend host (show port) rtable
  putStrLn "initializing worker"
  startSlave backend

executeDistributed :: (Serializable a) => NodeConfig -> TaskDef -> [DataDef] -> ([a] -> IO ())-> IO ()
executeDistributed (host, port) taskDef dataDefs resultProcessor = do
  backend <- initializeBackend host (show port) rtable
  startMaster backend $ \workerNodes -> do
    result <- executeOnNodes taskDef dataDefs workerNodes
    liftIO $ resultProcessor result

executeOnNodes :: (Serializable a) => TaskDef -> [DataDef] -> [NodeId] -> Process [a]
executeOnNodes taskDef dataDefs workerNodes = do
  if null workerNodes
    then say "no workers => no results (ports open?)" >> return [] 
    else executeOnNodes' taskDef dataDefs workerNodes

data DistributionStrategy = NextFreeNodeWithDataLocality

executeOnNodes' :: (Serializable a) => TaskDef -> [DataDef] -> [NodeId] -> Process [a]
executeOnNodes' taskDef dataDefs workerNodes = do
  masterProcess <- getSelfPid
  distributeWork masterProcess NextFreeNodeWithDataLocality taskDef dataDefs workerNodes 0 workerNodes (length dataDefs) []

-- try to allocate work until no work can be delegated to the remaining free workers, then collect a single result, repeat
distributeWork :: (Serializable a) => ProcessId -> DistributionStrategy -> TaskDef -> [DataDef] -> [NodeId] -> Int -> [NodeId] -> Int -> [[a]] -> Process [a]
-- done waiting:
distributeWork _ _ _ _ _ _ _ 0 collected = do
  say "all tasks accounted for"
  return $ concat $ reverse collected
-- done distributing:
distributeWork _ _ _ [] _ _ _ numWaiting collected = do
  say $ "expecting " ++ (show numWaiting) ++ " more responses"
  (_, nextResult) <- collectSingle
  distributeWork undefined undefined undefined [] undefined undefined undefined (numWaiting-1) (maybe collected (:collected) nextResult)
-- distribute as much as possible, collect single otherwise:
distributeWork masterProcess NextFreeNodeWithDataLocality taskDef (dataDef:rest) workerNodes numBusyNodes freeNodes numWaiting collected = do
  nodesWithData <- liftIO $ findNodesWithData (_config dataDef) (_filePath dataDef) freeNodes
  if null nodesWithData
    then do
    if numBusyNodes <= 0 then error "no worker accepts the task" else say $ "collecting since all workers are busy"
    (taskMetaData, nextResult) <- collectSingle
    distributeWork masterProcess NextFreeNodeWithDataLocality taskDef (dataDef:rest) workerNodes (numBusyNodes-1) ((_workerNodeId taskMetaData):freeNodes) (numWaiting-1) (maybe collected (:collected) nextResult)
    else do
    spawnWorkerProcess (head nodesWithData)
    say $ "spawning on: " ++ (show $ head nodesWithData)
    distributeWork masterProcess NextFreeNodeWithDataLocality taskDef rest workerNodes (numBusyNodes+1) (delete (head nodesWithData) freeNodes) numWaiting collected
      where
        spawnWorkerProcess workerNode = do
          _workerProcessId <- spawn workerNode (workerTaskClosure (TaskTransport masterProcess (TaskMetaData (taskDescription taskDef dataDef) workerNode) taskDef dataDef))
          return ()

collectSingle :: (Serializable a) => Process (TaskMetaData, Maybe a)
collectSingle = do
  (taskMetaData, nextResult) <- expect
  case nextResult of
   (Left  msg) -> say (msg ++ " failure not handled for "++(_taskName taskMetaData)++"...") >> return (taskMetaData, Nothing)
   (Right taskResult) -> say ("got a result for: "++(_taskName taskMetaData)) >> return (taskMetaData, Just taskResult)

showWorkerNodes :: NodeConfig -> IO ()
showWorkerNodes config = withWorkerNodes config (
  \workerNodes -> putStrLn ("Worker nodes: " ++ show workerNodes))

showWorkerNodesWithData :: NodeConfig -> NodeConfig -> String -> IO ()
showWorkerNodesWithData workerConfig hdfsConfig hdfsFilePath = withWorkerNodes workerConfig (
  \workerNodes -> do
    nodesWithData <- findNodesWithData hdfsConfig hdfsFilePath workerNodes
    putStrLn $ "Found these nodes with data: " ++ show nodesWithData)

withWorkerNodes :: NodeConfig -> ([NodeId] -> IO ()) -> IO ()
withWorkerNodes (host, port) action = liftIO $ do
  backend <- initializeBackend host (show port) initRemoteTable
  startMaster backend (\workerNodes -> liftIO (action workerNodes))

shutdownWorkerNodes :: NodeConfig -> IO ()
shutdownWorkerNodes (host, port) = do
  backend <- initializeBackend host (show port) rtable
  startMaster backend $ \workerNodes -> do
    say $ "found " ++ (show $ length workerNodes) ++ " worker nodes, shutting them down"
    forM_ workerNodes terminateSlave
    -- try terminateAllSlaves instead?

taskDescription :: TaskDef -> DataDef -> String
taskDescription t d = "Task: " ++ (describe t) ++ " " ++ (describe d)

class Describable a where
  describe :: a -> String
instance Describable TaskDef where
  describe (SourceCodeModule n _) = n
  describe (UnevaluatedThunk _ _) = "user function"
instance Describable DataDef where
  describe (HdfsData _ p) = p
  describe (PseudoDB n) = show n
