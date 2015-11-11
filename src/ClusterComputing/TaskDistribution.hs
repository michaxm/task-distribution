module ClusterComputing.TaskDistribution (
  startWorkerNode,
  executeDistributed,
  showWorkerNodes,
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

{-
 The bits in this file are arranged so that the less verbose Template Haskell version would work. That version is not used due to incompability
 issues between packman and Template Haskell though, but is retained in the commens for documentational purposes.
-}

-- BEGIN bindings for node communication
{-
 This is the final building block of the worker task execution, calling TaskSpawning.processTask.
-}
workerTask :: TaskTransport -> Process () -- TODO: have a node local config?
workerTask (TaskTransport masterProcess taskName taskDef dataSpec) = do
  say $ "processing: " ++ taskName
  result <- liftIO (processTask taskDef dataSpec >>= return . Right) `catch` buildError
  send masterProcess ((result :: Either String TaskResult) >>= \r -> Right (taskName, r))
  where
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

executeDistributed :: (Serializable a) => NodeConfig -> TaskDef -> [DataDef] -> ([a] -> IO ()) -> IO () -- FIXME [DataDef] is a list only because of testing purposes for now (select other data on different nodes)
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

executeOnNodes' :: (Serializable a) => TaskDef -> [DataDef] -> [NodeId] -> Process [a]
executeOnNodes' taskDef dataDefs workerNodes = do
  masterProcess <- getSelfPid
  taskWorkerPairing <- liftIO $ pairSuitableWorkers dataDefs workerNodes -- TODO have a choice of strategies, TODO do the pairing on the fly, respecting current worker state (see below)
  forM_ taskWorkerPairing (spawnWorkerProcess masterProcess) -- FIXME a worker node should not be allocated twice, let it have an 'occupied' state
  collectResults (length dataDefs) []
    where
      spawnWorkerProcess :: ProcessId -> (DataDef, NodeId) -> Process ()
      spawnWorkerProcess masterProcess (dataDef, workerNode) = do
        _workerProcessId <- spawn workerNode (workerTaskClosure (TaskTransport masterProcess (taskDescription taskDef dataDef) taskDef dataDef))
        return ()
      collectResults :: (Serializable a) => Int -> [[a]] -> Process [a]
      collectResults 0 res = return $ concat $ reverse res
      collectResults n res = do --FIXME handler for unexpected types
        say $ "expecting " ++ (show n) ++ " more responses"
        next <- expect
        case next of
          (Left msg) -> say (msg ++ " failure not handled ...") >> collectResults (n-1) res
          (Right (taskName, nextChunk)) -> say ("got a result for: "++taskName) >> collectResults (n-1) (nextChunk:res)

pairSuitableWorkers :: [DataDef] -> [NodeId] -> IO [(DataDef, NodeId)]
pairSuitableWorkers dataDefs workerNodes = do
  return $ (zip dataDefs (cycle workerNodes)) -- TODO implement real distribution

showWorkerNodes :: NodeConfig -> IO ()
showWorkerNodes (host, port) = do
  backend <- initializeBackend host (show port) initRemoteTable
  startMaster backend (\workerNodes -> liftIO . putStrLn $ "Slaves: " ++ show workerNodes)

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
