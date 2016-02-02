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
import Data.Time.Clock (UTCTime, diffUTCTime, NominalDiffTime, getCurrentTime)

import Control.Distributed.Task.Distribution.DataLocality (findNodesWithData)
import Control.Distributed.Task.Distribution.LogConfiguration
import Control.Distributed.Task.Distribution.TaskTransport
import qualified Control.Distributed.Task.TaskSpawning.BinaryStorage as RemoteStore
import Control.Distributed.Task.TaskSpawning.TaskDefinition
import Control.Distributed.Task.TaskSpawning.TaskDescription
import Control.Distributed.Task.TaskSpawning.TaskSpawning (processTask)
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
 This is the final building block of the slave task execution, calling TaskSpawning.processTask.
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
  let (pureResults, runStatistics) = splitStatistics collectedResults
    in do
    say $ printStatistics runStatistics
    return $ pureResults

type AggregatedResult = ([TaskResult], RunStatistics)
type RunStatistics = (NominalDiffTime, NominalDiffTime, [SingleTaskRunStatistics])

splitStatistics :: [CollectedResult] -> AggregatedResult
splitStatistics = foldr aggregateResults ([], emptyStats)
  where
    emptyStats = (emptyDuration, emptyDuration, [])
    emptyDuration = fromIntegral (0 :: Integer)
    aggregateResults :: CollectedResult -> AggregatedResult -> AggregatedResult
    aggregateResults collectedResult aggregatedResults = combine aggregatedResults $ toAggregatedResult collectedResult
      where
        combine :: AggregatedResult -> AggregatedResult -> AggregatedResult
        combine (rs1, stats1) (rs2, stats2) = (rs1++rs2, combineStats stats1 stats2)
          where
            combineStats (d1, r1, ts1) (d2, r2, ts2) = (d1+d2, r1+r2, ts1++ts2)
        toAggregatedResult :: CollectedResult -> AggregatedResult
        toAggregatedResult (CollectedResult _ d r res) = (map fst res, (d, r, map toStats res))
          where
            toStats :: CollectedCompleteTaskResult -> SingleTaskRunStatistics
            toStats = snd
        

printStatistics :: RunStatistics -> String
printStatistics = show -- TODO too lazy for now ...
--  n ++ ": total: " ++ (show totalTaskTime) ++ ", remote total: " ++ (show remoteTotal) ++ ", data load: " ++ (show dataLoadDur) ++ ", task exec: " ++ (show execTaskDur)

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
         then error $ "nothing found to distribute, should not have tried in the first place then"
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
                else takeLocalTasks (n-1) found (t:notSuitable) rest
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

data CollectedResult = CollectedResult {
  _collectedMetaData :: TaskMetaData,
  _totalDistributedRuntime :: NominalDiffTime,
  _totalRemoteRuntime :: NominalDiffTime,
  _collectedResults :: [CollectedCompleteTaskResult]
}
type CollectedCompleteTaskResult = (TaskResult, SingleTaskRunStatistics)

collectSingle :: Process CollectedResult
collectSingle = receiveWait [
  match $ handleTransportedResult,
  matchAny $ \msg -> liftIO $ putStr " received unhandled  message : " >> print msg >> error "aborting due to unknown message type"
  ]
  where
--    handleTransportedResult :: TransportedResult -> Process CollectedResult
    handleTransportedResult (TransportedResult taskMetaData remoteRuntime serializedResults) = do
      say $ "got task processing response for: "++_taskName taskMetaData
      now <- liftIO getCurrentTime
      let
        taskName = _taskName taskMetaData
        distributedTime = diffUTCTime now (deserializeTime $ _taskDistributionStartTime taskMetaData)
        remoteRuntime' = deserializeTimeDiff remoteRuntime
        in case serializedResults of
            (Left  msg) -> say (msg ++ " failure not handled for "++taskName++"...") >> return (CollectedResult taskMetaData distributedTime remoteRuntime' []) -- note: no restarts take place here
            (Right results) -> return (CollectedResult taskMetaData distributedTime remoteRuntime' (map deserializeTaskResult results))
        where
          deserializeTaskResult :: SerializedCompleteTaskResult -> CollectedCompleteTaskResult
          deserializeTaskResult (taskRes, (d, e)) = (taskRes, (deserializeTimeDiff d, deserializeTimeDiff e))

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
      results <- liftIO (processTask taskDef dataDefs resultDef)
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
    buildError e = return $ TransportedResult taskMetaData (fromIntegral (0 :: Integer)) $ Left $ "Task execution (for: "++taskName++") failed: " ++ (format $ show e)
      where
        format [] = []
        format ('\\':'n':'\\':'t':rest) = "\n\t" ++ (format rest)
        format (x:rest) = x:[] ++ (format rest)

prepareResultsForResponding :: TaskMetaData -> UTCTime -> UTCTime -> [CompleteTaskResult] -> TransportedResult
prepareResultsForResponding metaData acceptTime processingDoneTime results =
  TransportedResult metaData remoteRuntime $ Right $ map prepareSingleResult results
  where
    remoteRuntime = serializeTimeDiff $ diffUTCTime processingDoneTime acceptTime
    prepareSingleResult :: CompleteTaskResult -> SerializedCompleteTaskResult
    prepareSingleResult (taskResultWrapper, runStat) = case taskResultWrapper of
      StoredRemote -> ([], serializedRunStat)
      (DirectResult plainResult) -> (plainResult, serializedRunStat)
      where
        serializedRunStat :: SerializedSingleTaskRunStatistics
        serializedRunStat = (serializeTimeDiff $ fst runStat, serializeTimeDiff $ snd runStat)

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
