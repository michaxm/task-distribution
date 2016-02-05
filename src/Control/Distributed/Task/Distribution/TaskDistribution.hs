module Control.Distributed.Task.Distribution.TaskDistribution (
  startSlaveNode,
  executeDistributed,
  showSlaveNodes,
  showSlaveNodesWithData,
  shutdownSlaveNodes) where

import Control.Distributed.Process (Process, ProcessId, NodeId,
                                    say, getSelfPid, spawn, send, expect, receiveWait, match, matchAny, catch, 
                                    RemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet (initializeBackend, startMaster, startSlave, terminateSlave)
import qualified Control.Distributed.Process.Serializable as PS
import qualified Control.Distributed.Static as S
import Control.Distributed.Process.Closure (staticDecode)
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Exception.Base (SomeException)
import Control.Monad (forM_)
import Control.Monad.IO.Class
import qualified Data.Binary as B (encode)
import Data.Bool (bool)
import Data.ByteString.Lazy (ByteString)
import Data.List (intersperse)
import qualified Data.Map.Strict as Map
import qualified Data.Rank1Dynamic as R1 (toDynamic)
import Data.Time.Clock (UTCTime, diffUTCTime, getCurrentTime)

import Control.Distributed.Task.Distribution.DataLocality (findNodesWithData)
import Control.Distributed.Task.Distribution.LogConfiguration
import Control.Distributed.Task.Distribution.TaskTransport
import qualified Control.Distributed.Task.TaskSpawning.BinaryStorage as RemoteStore
import Control.Distributed.Task.TaskSpawning.TaskDefinition
import Control.Distributed.Task.TaskSpawning.TaskDescription
import Control.Distributed.Task.TaskSpawning.TaskSpawning (processTasks, TasksExecutionResult)
import Control.Distributed.Task.TaskSpawning.TaskSpawningTypes
import Control.Distributed.Task.Types.TaskTypes
import Control.Distributed.Task.Util.Configuration
import Control.Distributed.Task.Util.Logging
import Control.Distributed.Task.Util.SerializationUtil

{-
 The bits in this file are arranged so that the less verbose Template Haskell version would work. That version is not used due to incompability
 issues between packman and Template Haskell though, but is retained in the commens for documentational purposes.
-}

-- BEGIN bindings for node communication
{-
 This is the final building block of the slave task execution, calling TaskSpawning.processTasks.
-}
slaveTask :: TaskTransport -> Process () -- TODO: have a node local config?
slaveTask = handleSlaveTask

querySlavePreparation :: QuerySlavePreparationRequest -> Process ()
querySlavePreparation (masterProcess, hash) = handleQuerySlavePreparation hash >>= send masterProcess

prepareSlave :: PrepareSlaveRequest -> Process ()
prepareSlave (masterProcess, hash, content) = handlePrepareSlave hash content >>= send masterProcess

-- template haskell vs. its result
-- needs: {-# LANGUAGE TemplateHaskell, DeriveDataTypeable, DeriveGeneric #-}
--remotable ['slaveTask] 
-- ======>
slaveTask__static :: S.Static (TaskTransport -> Process ())
slaveTask__static = S.staticLabel "ClusterComputing.TaskDistribution.slaveTask"
slaveTask__sdict :: S.Static (PS.SerializableDict TaskTransport)
slaveTask__sdict = S.staticLabel "ClusterComputing.TaskDistribution.slaveTask__sdict"
-- not used for now, removed due to warnings as errors
--slaveTask__tdict :: S.Static (PS.SerializableDict ())
--slaveTask__tdict = S.staticLabel "ClusterComputing.TaskDistribution.slaveTask__tdict"
querySlavePreparation__static :: S.Static (QuerySlavePreparationRequest -> Process())
querySlavePreparation__static = S.staticLabel "ClusterComputing.TaskDistribution.querySlavePreparation"
querySlavePreparation__sdict :: S.Static (PS.SerializableDict QuerySlavePreparationRequest)
querySlavePreparation__sdict = S.staticLabel "ClusterComputing.TaskDistribution.querySlavePreparation__sdict"
prepareSlave__static :: S.Static (PrepareSlaveRequest -> Process())
prepareSlave__static = S.staticLabel "ClusterComputing.TaskDistribution.prepareSlave"
prepareSlave__sdict :: S.Static (PS.SerializableDict PrepareSlaveRequest)
prepareSlave__sdict = S.staticLabel "ClusterComputing.TaskDistribution.prepareSlave__sdict"
__remoteTable :: S.RemoteTable -> S.RemoteTable
__remoteTable =
  (S.registerStatic "ClusterComputing.TaskDistribution.slaveTask" (R1.toDynamic slaveTask))
  . (S.registerStatic "ClusterComputing.TaskDistribution.slaveTask__sdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict TaskTransport)))
  . (S.registerStatic "ClusterComputing.TaskDistribution.slaveTask__tdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict ())))
  . (S.registerStatic "ClusterComputing.TaskDistribution.querySlavePreparation" (R1.toDynamic querySlavePreparation))
  . (S.registerStatic "ClusterComputing.TaskDistribution.querySlavePreparation__sdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict QuerySlavePreparationRequest)))
  . (S.registerStatic "ClusterComputing.TaskDistribution.querySlavePreparation__tdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict ())))
  . (S.registerStatic "ClusterComputing.TaskDistribution.prepareSlave" (R1.toDynamic prepareSlave))
  . (S.registerStatic "ClusterComputing.TaskDistribution.prepareSlave__sdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict PrepareSlaveRequest)))
  . (S.registerStatic "ClusterComputing.TaskDistribution.prepareSlave__tdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict ())))

slaveTaskClosure :: TaskTransport -> S.Closure (Process ())
slaveTaskClosure =
  -- $(mkClosure 'slaveTask)
-- ======>
   ((S.closure
    (slaveTask__static
     `S.staticCompose`
     (staticDecode
      slaveTask__sdict)))
   . B.encode)

querySlavePreparationClosure :: QuerySlavePreparationRequest -> S.Closure (Process ())
querySlavePreparationClosure =
   ((S.closure
    (querySlavePreparation__static
     `S.staticCompose`
     (staticDecode
      querySlavePreparation__sdict)))
   . B.encode)
   
prepareSlaveClosure :: PrepareSlaveRequest -> S.Closure (Process ())
prepareSlaveClosure =
   ((S.closure
    (prepareSlave__static
     `S.staticCompose`
     (staticDecode
      prepareSlave__sdict)))
   . B.encode)

rtable :: RemoteTable
rtable = __remoteTable $ initRemoteTable
-- END bindings for node communication

type NodeConfig = (String, Int)

startSlaveNode :: NodeConfig -> IO ()
startSlaveNode (host, port) = do
  initDefaultLogging (show port)
  backend <- initializeBackend host (show port) rtable
  putStrLn "initializing slave"
  startSlave backend

executeDistributed :: NodeConfig -> TaskDef -> [DataDef] -> ResultDef -> ([TaskResult] -> IO ())-> IO ()
executeDistributed (host, port) taskDef dataDefs resultDef resultProcessor = do
  backend <- initializeBackend host (show port) rtable
  startMaster backend $ \slaveNodes -> do
    result <- executeOnNodes taskDef dataDefs resultDef slaveNodes
    liftIO $ resultProcessor result

executeOnNodes :: TaskDef -> [DataDef] -> ResultDef -> [NodeId] -> Process [TaskResult]
executeOnNodes taskDef dataDefs resultDef slaveNodes = do
  if null slaveNodes
    then say "no slaves => no results (ports open?)" >> return [] 
    else executeOnNodes' taskDef dataDefs resultDef slaveNodes

executeOnNodes' :: TaskDef -> [DataDef] -> ResultDef -> [NodeId] -> Process [TaskResult]
executeOnNodes' taskDef dataDefs resultDef slaveNodes = do
  masterProcess <- getSelfPid
  config <- liftIO getConfiguration
  before <- liftIO getCurrentTime
  collectedResults <- distributeWorkForNodes masterProcess (_maxTasksPerNode config) (_distributionStrategy config) taskDef dataDefs resultDef slaveNodes
  after <- liftIO getCurrentTime
  say $ "total time: " ++ show (diffUTCTime after before)
  say $ "remote times:" ++ (printStatistics collectedResults)
  return $ concat $ map _collectedResults collectedResults

printStatistics :: [CollectedResult] -> String
printStatistics = concat . map printStat
  where
    printStat r = "\ntotal: " ++total++ ", remote total: " ++remoteTotal++ ", task execution total real time: " ++taskTotal++" for remote run: "++taskname
      where
        taskname = _taskName $ _collectedMetaData r
        total = show $ _totalDistributedRuntime r
        remoteTotal = show $ _totalRemoteRuntime r
        taskTotal = show $ _totalTasksRuntime r

type NodeOccupancy = Map.Map NodeId Bool

occupyNode :: NodeId -> NodeOccupancy -> NodeOccupancy
occupyNode = Map.adjust (bool True $ error "node already occupied")

unoccupyNode :: NodeId -> NodeOccupancy -> NodeOccupancy
unoccupyNode = Map.adjust (flip bool False $ error "node already vacant")

initializeOccupancy :: [NodeId] -> NodeOccupancy
initializeOccupancy = foldr (flip Map.insert False) Map.empty

{-|
 Tries to find work for every worker node, looking at all tasks, forgetting the node if no task is found.

 Note (obsolete, but may be reactivated): tries to be open to other result types but assumes a list of results, as these can be concatenated over multiple tasks. Requires result entries to be serializable, not the
  complete result - confusing this can cause devastatingly misleading compiler complaints about Serializable.
|-}
distributeWorkForNodes :: ProcessId -> Int -> DistributionStrategy -> TaskDef -> [DataDef] -> ResultDef -> [NodeId] -> Process [CollectedResult]
distributeWorkForNodes masterProcess maxTasksPerNode strategy taskDef dataDefs resultDef allNodes = doItFor ([], 0, initializeOccupancy allNodes, dataDefs)
  where
    doItFor :: ([CollectedResult], Int, NodeOccupancy, [DataDef]) -> Process [CollectedResult]
    doItFor (collected, 0, _, []) = return collected -- everything processed, results mainly in reversed order (a bit garbled)
    doItFor (collected, resultsWaitingOn, nodeOccupancy, []) = collectNextResult collected nodeOccupancy [] resultsWaitingOn -- everything distributed, but still results to be collected
    doItFor (collected, resultsWaitingOn, nodeOccupancy, undistributedTasks)
      | noFreeNodes nodeOccupancy = liftIO (logInfo $ "no unoccupied workers, wait results to come back: "++show nodeOccupancy)
                                    >> collectNextResult collected nodeOccupancy undistributedTasks resultsWaitingOn -- 
      | otherwise = let freeNodes = nextFreeNodes nodeOccupancy
                        freeNode = if null freeNodes then error "no free nodes" else head freeNodes :: NodeId
                    in do
       liftIO $ logInfo $ "finding suitable tasks for the next unoccupied node: "++show freeNode++" - occupations: "++show nodeOccupancy
       (suitableTasks, remainingTasks) <- findSuitableTasks strategy freeNode
       if null suitableTasks
         then error $ "nothing found to distribute, should not have tried in the first place then: "++(show (map describe undistributedTasks, map describe suitableTasks, map describe remainingTasks))
         else do -- regular distribution
           say $ "spawning on: " ++ (show $ freeNode)
           spawnSlaveProcess masterProcess taskDef suitableTasks resultDef freeNode
           doItFor (collected, resultsWaitingOn+1, occupyNode freeNode nodeOccupancy, remainingTasks)
      where
        noFreeNodes = null . nextFreeNodes
        nextFreeNodes :: NodeOccupancy -> [NodeId]
        nextFreeNodes =  Map.keys . Map.filter not
        findSuitableTasks :: DistributionStrategy -> NodeId -> Process ([DataDef], [DataDef])
        findSuitableTasks AnywhereIsFine _ = return (take maxTasksPerNode undistributedTasks, drop maxTasksPerNode undistributedTasks)
        findSuitableTasks FirstTaskWithData freeNode = if length undistributedTasks <= maxTasksPerNode then findSuitableTasks AnywhereIsFine freeNode else takeLocalTasks maxTasksPerNode [] [] undistributedTasks
          where
            takeLocalTasks :: Int -> [DataDef] -> [DataDef] -> [DataDef] -> Process ([DataDef], [DataDef])
            takeLocalTasks 0 found notSuitable rest = return (found, notSuitable++rest) -- enough found
            takeLocalTasks n found notSuitable [] = return (found++(take n notSuitable), drop n notSuitable) -- everything searched, fill up
            takeLocalTasks n found notSuitable (t:rest) = do
              allNodesSuitableForTask <- findNodesWithData' t
              if any (==freeNode) allNodesSuitableForTask
                then takeLocalTasks (n-1) (t:found) notSuitable rest
                else takeLocalTasks n found (t:notSuitable) rest
              where
                findNodesWithData' :: DataDef -> Process [NodeId]
                findNodesWithData' (HdfsData loc) = liftIO $ findNodesWithData loc allNodes -- TODO this listing is not really efficient for this approach, caching necessary?
                findNodesWithData' (PseudoDB _) = return allNodes -- no data locality emulation support planned for simple pseudo db yet
    collectNextResult :: [CollectedResult] -> NodeOccupancy -> [DataDef] -> Int -> Process [CollectedResult]
    collectNextResult collected nodeOccupancy undistributedTasks resultsWaitingOn = do
      say $ "waiting for a result"
      collectedResult <- collectSingle
      let taskMetaData = _collectedMetaData collectedResult
          updatedResults = (collectedResult:collected) -- note: effectively scrambles result order a bit, but that should already be so due to parallel processing
        in do
        say $ "got result from: " ++ (show $ _slaveNodeId taskMetaData)
        doItFor (updatedResults, resultsWaitingOn-1, unoccupyNode (_slaveNodeId taskMetaData) nodeOccupancy, undistributedTasks)

{-|
 Represents the results of a single remote execution, including multiple results from parallel execution.
-}
data CollectedResult = CollectedResult {
  _collectedMetaData :: TaskMetaData,
  _totalDistributedRuntime :: NominalDiffTime,
  _totalRemoteRuntime :: NominalDiffTime,
  _totalTasksRuntime :: NominalDiffTime,
  _collectedResults :: [TaskResult]
}

collectSingle :: Process CollectedResult
collectSingle = receiveWait [
  match $ handleTransportedResult,
  matchAny $ \msg -> liftIO $ putStr " received unhandled  message : " >> print msg >> error "aborting due to unknown message type"
  ]
  where
    handleTransportedResult :: TransportedResult -> Process CollectedResult
    handleTransportedResult (TransportedResult taskMetaData remoteRuntime tasksRuntime serializedResults) = do
      say $ "got task processing response for: "++_taskName taskMetaData
      now <- liftIO getCurrentTime
      let
        taskName = _taskName taskMetaData
        distributedRuntime' = diffUTCTime now (deserializeTime $ _taskDistributionStartTime taskMetaData)
        remoteRuntime' = deserializeTimeDiff remoteRuntime
        tasksRuntime' = deserializeTimeDiff tasksRuntime
        in case serializedResults of
            (Left  msg) -> say (msg ++ " failure not handled for "++taskName++"...") >> return (CollectedResult taskMetaData distributedRuntime' remoteRuntime' tasksRuntime' []) -- note: no restarts take place here
            (Right results) -> return $ CollectedResult taskMetaData distributedRuntime' remoteRuntime' tasksRuntime' results

spawnSlaveProcess :: ProcessId -> TaskDef -> [DataDef] -> ResultDef -> NodeId -> Process ()
spawnSlaveProcess masterProcess taskDef dataDefs resultDef slaveNode = do
  now <- liftIO getCurrentTime
  preparedTaskDef <- prepareSlaveForTask taskDef
  taskTransport <- return $ TaskTransport masterProcess (TaskMetaData (taskDescription taskDef dataDefs slaveNode) slaveNode (serializeTime now)) preparedTaskDef dataDefs resultDef
  _unusedSlaveProcessId <- spawn slaveNode (slaveTaskClosure taskTransport)
  return ()
  where
    prepareSlaveForTask :: TaskDef -> Process TaskDef
    prepareSlaveForTask (DeployFullBinary program) = do
      hash <- return $ RemoteStore.calculateHash program
      say $ "preparing for " ++ (show hash)
      _ <- spawn slaveNode (querySlavePreparationClosure (masterProcess, hash))
      prepared <- expect -- TODO match slaveNodes?
      case prepared of
       Prepared -> say $ "already prepared: " ++ (show hash)
       Unprepared -> do
         say $ "distributing " ++ (show hash)
         before <- liftIO getCurrentTime
         _ <- spawn slaveNode (prepareSlaveClosure (masterProcess, hash, program))
         ok <- expect -- TODO match slaveNodes?
         after <- liftIO getCurrentTime
         case ok of
          PreparationFinished -> say $ "distribution took: " ++ (show $ diffUTCTime after before)
      return $ PreparedDeployFullBinary hash
    prepareSlaveForTask d = return d -- nothing to prepare for other tasks (for now)

-- remote logic

handleSlaveTask :: TaskTransport -> Process ()
handleSlaveTask (TaskTransport masterProcess taskMetaData taskDef dataDefs resultDef) = do
  transportedResult <- (
    do
      acceptTime <- liftIO getCurrentTime
      say $ "processing: " ++ taskName
      results <- liftIO (processTasks taskDef dataDefs resultDef)
      say $ "processing done for: " ++ taskName
      processingDoneTime <- liftIO getCurrentTime
      return $ prepareResultsForResponding taskMetaData acceptTime processingDoneTime results
    ) `catch` buildError
  say $ "replying"
  send masterProcess (transportedResult :: TransportedResult)
  say $ "reply sent"
  where
    taskName = _taskName taskMetaData
    buildError :: SomeException -> Process TransportedResult
    buildError e = return $ TransportedResult taskMetaData emptyDuration emptyDuration $ Left $ "Task execution (for: "++taskName++") failed: " ++ (show e)
      where
        emptyDuration = fromIntegral (0 :: Integer)

prepareResultsForResponding :: TaskMetaData -> UTCTime -> UTCTime -> TasksExecutionResult -> TransportedResult
prepareResultsForResponding metaData acceptTime processingDoneTime (results, tasksRuntime) =
  TransportedResult metaData remoteRuntime (serializeTimeDiff tasksRuntime) $ Right results
  where
    remoteRuntime = serializeTimeDiff $ diffUTCTime processingDoneTime acceptTime

-- preparation negotiaion

handleQuerySlavePreparation :: Int -> Process QuerySlavePreparationResponse
handleQuerySlavePreparation hash = liftIO (RemoteStore.get hash) >>= return . (maybe Unprepared (\_ -> Prepared))

handlePrepareSlave :: Int -> ByteString -> Process PrepareSlaveResponse
handlePrepareSlave hash content = liftIO (RemoteStore.put hash content) >> return PreparationFinished

-- simple tasks
showSlaveNodes :: NodeConfig -> IO ()
showSlaveNodes config = withSlaveNodes config (
  \slaveNodes -> putStrLn ("Slave nodes: " ++ show slaveNodes))

showSlaveNodesWithData :: NodeConfig -> String -> IO ()
showSlaveNodesWithData slaveConfig hdfsFilePath = withSlaveNodes slaveConfig (
  \slaveNodes -> do
    nodesWithData <- findNodesWithData hdfsFilePath slaveNodes
    putStrLn $ "Found these nodes with data: " ++ show nodesWithData)

withSlaveNodes :: NodeConfig -> ([NodeId] -> IO ()) -> IO ()
withSlaveNodes (host, port) action = liftIO $ do
  backend <- initializeBackend host (show port) initRemoteTable
  startMaster backend (\slaveNodes -> liftIO (action slaveNodes))

shutdownSlaveNodes :: NodeConfig -> IO ()
shutdownSlaveNodes (host, port) = do
  backend <- initializeBackend host (show port) rtable
  startMaster backend $ \slaveNodes -> do
    say $ "found " ++ (show $ length slaveNodes) ++ " slave nodes, shutting them down"
    forM_ slaveNodes terminateSlave
    -- try terminateAllSlaves instead?

taskDescription :: TaskDef -> [DataDef] -> NodeId -> String
taskDescription t d n = "task: " ++ (describe t) ++ " " ++ (concat $ intersperse ", " $ map describe d) ++ " on " ++ (show n)
