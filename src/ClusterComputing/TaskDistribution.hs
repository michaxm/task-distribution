module ClusterComputing.TaskDistribution (
  startWorkerNode,
  executeDistributed,
  shutdownWorkerNodes) where

import Control.Distributed.Process (Process, ProcessId, NodeId,
                                    say, getSelfPid, spawn, send, expect, liftIO, onException,
                                    RemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet (initializeBackend, startMaster, startSlave, terminateSlave)
import Control.Distributed.Static (Closure)  -- FIXME?
import qualified Control.Distributed.Process.Serializable as PS
import qualified Control.Distributed.Static as S
import Control.Distributed.Process.Closure (mkClosure, remotable, staticDecode)
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Serializable (Serializable)
import Control.Monad (forM_)
import qualified Data.Binary as B (encode)
import qualified Data.Rank1Dynamic as R1 (toDynamic)
import qualified Language.Haskell.Interpreter as I -- ugly: should not be referred here

-- FIXME move data access to config
import qualified DataAccess.DataSource as DS
--import qualified DataAccess.SimpleDataSource as DS
import qualified DataAccess.HdfsDataSource as DS

--FIXME separate module
import DynamicLoading
--FIXME DataEntry should be as dynamic as possible, not part of Distribution, but of spawning
import TaskTypes

import ClusterComputing.TaskTransport

-- BEGIN bindings for node communication

-- at some point it might get relevant to have a worker local config
-- - I do not know, how to implement that, the distributed-process setup might prevent that
workerTask :: TaskTransport -> Process ()
workerTask (TaskTransport masterProcess modulePath numDB) = do
  say $ "processing: " ++ modulePath
  -- TODO at this point there is still really undesired explicit coupling
  --  - how can a binding between a data source and a task as data consumer be somewhat typesafe inferred?
  --  - at least [a]/a could be supported, one one needs to be implemented
  liftIO $ putStrLn $ "local worker log; processing: " ++ modulePath
  func <- liftIO (loadTask (I.as :: [DataEntry] -> [DataEntry]) modulePath) `onException` sendEmpty
  say "loading data"
  localData <- liftIO (DS._loadEntries DS.dataSource mkFilePath :: IO [DataEntry]) `onException` sendEmpty
  send masterProcess (func $ localData)
  say $ "processing done"
  where
    sendEmpty = send masterProcess ([] :: [DataEntry])
    mkFilePath = "/" --"resources/pseudo-db/" ++ (show numDB)

-- template haskell vs. its result
{-# LANGUAGE TemplateHaskell, DeriveDataTypeable, DeriveGeneric #-}
--remotable ['workerTask] 
-- ======>
workerTask__static :: S.Static (TaskTransport -> Process ())
workerTask__static = S.staticLabel "ClusterComputing.TaskDistribution.workerTask"
workerTask__sdict :: S.Static (PS.SerializableDict TaskTransport)
workerTask__sdict = S.staticLabel "ClusterComputing.TaskDistribution.workerTask__sdict"
workerTask__tdict :: S.Static (PS.SerializableDict ())
workerTask__tdict = S.staticLabel "ClusterComputing.TaskDistribution.workerTask__tdict"
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
        workerProcessId <- spawn workerNode (workerTaskClosure (TaskTransport masterProcess modulePath numDB))
        return ()
      collectResults :: (Serializable a) => Int -> [[a]] -> Process [a]
      collectResults 0 res = return $ concat $ reverse res
      collectResults n res = do
        say $ "expecting " ++ (show n) ++ " more responses"
        next <- expect
        collectResults (n-1) (next:res)

shutdownWorkerNodes :: IO ()
shutdownWorkerNodes =  do
  backend <- initializeBackend "localhost" "44440" rtable
  startMaster backend $ \workerNodes -> do
    say $ "found " ++ (show $ length workerNodes) ++ " worker nodes, shutting them down"
    forM_ workerNodes terminateSlave
    -- try terminateAllSlaves instead?

