{-# LANGUAGE ScopedTypeVariables #-}
module ClusterComputing.TaskDistribution (
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
import Control.Distributed.Process.Serializable (Serializable)
import Control.Exception.Base (SomeException)
import Control.Monad (forM_)
import Control.Monad.IO.Class
import qualified Data.Binary as B (encode)
import Data.ByteString.Lazy (ByteString)
import qualified Data.Rank1Dynamic as R1 (toDynamic)
import Data.Time.Clock (UTCTime, diffUTCTime, NominalDiffTime, getCurrentTime)

import ClusterComputing.DataLocality (findNodesWithData)
import ClusterComputing.LogConfiguration
import ClusterComputing.TaskTransport
import DataAccess.HdfsWriter (writeEntriesToFile, stripHDFSPartOfPath)
import qualified TaskSpawning.BinaryStorage as RemoteStore
import TaskSpawning.TaskDefinition
import TaskSpawning.TaskDescription
import TaskSpawning.ExecutionUtil (measureDuration, executeExternal, parseResult)
import TaskSpawning.TaskSpawning (processTask, RunStat)
import Types.TaskTypes
import Util.FileUtil
import Util.Logging

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

executeDistributed :: (Serializable a) => NodeConfig -> TaskDef -> [DataDef] -> ResultDef -> ([a] -> IO ())-> IO ()
executeDistributed (host, port) taskDef dataDefs resultDef resultProcessor = do
  backend <- initializeBackend host (show port) rtable
  startMaster backend $ \slaveNodes -> do
    result <- executeOnNodes taskDef dataDefs resultDef slaveNodes
    liftIO $ resultProcessor result

executeOnNodes :: (Serializable a) => TaskDef -> [DataDef] -> ResultDef -> [NodeId] -> Process [a]
executeOnNodes taskDef dataDefs resultDef slaveNodes = do
  if null slaveNodes
    then say "no slaves => no results (ports open?)" >> return [] 
    else executeOnNodes' taskDef dataDefs resultDef slaveNodes

data DistributionStrategy = FirstTaskWithData

executeOnNodes' :: (Serializable a) => TaskDef -> [DataDef] -> ResultDef -> [NodeId] -> Process [a]
executeOnNodes' taskDef dataDefs resultDef slaveNodes = do
  masterProcess <- getSelfPid
  before <- liftIO getCurrentTime
  taskResults <- distributeWorkForNodes masterProcess FirstTaskWithData taskDef dataDefs resultDef slaveNodes
  after <- liftIO getCurrentTime
  say $ "total time: " ++ show (diffUTCTime after before)
  mapM_ say $ map showRunStat $ snd taskResults
  say $ showRunStat $ foldr aggregateStats emptyStat $ snd taskResults
  return $ fst taskResults
    where
      emptyStat = ("all tasks", deserializeTimeDiff emptyDuration, RemoteRunStat emptyDuration emptyDuration emptyDuration emptyDuration)
      emptyDuration = fromIntegral (0 :: Integer)
      aggregateStats (_, t, RemoteRunStat a b c d) (n, t', RemoteRunStat a' b' c' d') = (n, t+t', RemoteRunStat (a+a') (b+b') (c+c') (d+d'))

type TaskRunStat = (String, NominalDiffTime, RemoteRunStat)

showRunStat :: TaskRunStat -> String
showRunStat (n, totalTaskTime, remoteStat) =
  n ++ ": total: " ++ (show totalTaskTime) ++ ", remote total: " ++ (show remoteTotal) ++ ", data load: " ++ (show dataLoadDur) ++ ", task load: " ++ (show taskLoadDur) ++ ", task exec: " ++ (show execTaskDur)
  where
    remoteTotal = deserializeTimeDiff $ _remoteTotalDuration remoteStat
    dataLoadDur = deserializeTimeDiff $ _remoteDataLoadDuration remoteStat
    taskLoadDur = deserializeTimeDiff $ _remoteTaskLoadDuration remoteStat
    execTaskDur = deserializeTimeDiff $ _remoteExecTaskDuration remoteStat

{-|
 Tries to find work for every worker node, looking at all tasks, forgetting the node if no task is found.

 Note: tries to be open to other result types but assumes a list of results, as these can be concatenated over multiple tasks. Requires result entries to be serializable, not the
  complete result - confusing this can cause devastatingly misleading compiler complaints about Serializable.
|-}
distributeWorkForNodes :: forall entry . (Serializable entry) => ProcessId -> DistributionStrategy -> TaskDef -> [DataDef] -> ResultDef -> [NodeId] -> Process ([entry], [TaskRunStat])
distributeWorkForNodes masterProcess strategy taskDef dataDefs resultDef allNodes = doItFor ([], 0, allNodes, dataDefs)
  where
    doItFor :: ([([entry], TaskRunStat)], Int, [NodeId], [DataDef]) -> Process ([entry], [TaskRunStat])
    doItFor (collected, 0, _, []) = return $ let (res, stat) = unzip collected in (concat $ reverse res, stat) -- everything processed
    doItFor (collected, resultsWaitingOn, freeNodes, []) = collectNextResult collected freeNodes [] resultsWaitingOn -- everything distributed, but still results to be collected
    doItFor (collected, resultsWaitingOn, [], undistributedTasks) = collectNextResult collected [] undistributedTasks resultsWaitingOn -- no unoccupied workers, wait results to come back
    doItFor (collected, resultsWaitingOn, freeNode:freeNodes, undistributedTasks) = do -- find a suitable task for an unoccupied nodes
       (suitableTask, remainingTasks) <- findSuitableTask strategy
       maybe
         (doItFor (collected, resultsWaitingOn, freeNodes, remainingTasks)) -- no further work for this node available, discard it for distribution
         (\t -> do -- regular distribution
             say $ "spawning on: " ++ (show $ freeNode)
             spawnSlaveProcess masterProcess taskDef t resultDef freeNode
             doItFor (collected, resultsWaitingOn+1, freeNodes, remainingTasks))
         suitableTask
      where
        findSuitableTask :: DistributionStrategy -> Process (Maybe DataDef, [DataDef])
        findSuitableTask FirstTaskWithData = findSuitableTask' [] undistributedTasks
          where
            findSuitableTask' notSuitable [] = return (Nothing, reverse notSuitable)
            findSuitableTask' notSuitable (t:rest) = do
              allNodesSuitableForTask <- findNodesWithData' t
              if any (==freeNode) allNodesSuitableForTask
                then return (Just t, reverse notSuitable ++ rest)
                else findSuitableTask' (t:notSuitable) rest
              where
                findNodesWithData' (HdfsData loc) = liftIO $ findNodesWithData loc allNodes -- TODO this listing is not really efficient for this approach ...
                findNodesWithData' (PseudoDB _) = return allNodes -- no data locality strategy for simple pseudo db
    collectNextResult collected freeNodes undistributedTasks resultsWaitingOn = do
      say $ "waiting for a result"
      (taskMetaData, maybeNextResult) <- collectSingle
      say $ "got result from: " ++ (show $ _slaveNodeId taskMetaData)
      let updatedResults = maybe collected (:collected) maybeNextResult in -- no restarts for failed tasks for now
       doItFor (updatedResults, resultsWaitingOn-1, _slaveNodeId taskMetaData:freeNodes, undistributedTasks)

collectSingle :: forall entry . (Serializable entry) => Process (TaskMetaData, Maybe ([entry], TaskRunStat))
collectSingle = receiveWait [
  match $ handleTransportedResult,
  matchAny $ \msg -> liftIO $ putStr " received unhandled  message : " >> print msg >> error "aborting due to unknown message type"
  ]
  where
    handleTransportedResult (taskMetaData, nextResult) = do
      now <- liftIO getCurrentTime
      let
        taskName = _taskName taskMetaData
        in case nextResult of
            (Left  msg) -> say (msg ++ " failure not handled for "++taskName++"...") >> return (taskMetaData, Nothing)
            (Right (taskResult, remoteRunStat)) -> say ("got a result for: "++taskName) >> return (taskMetaData, Just (taskResult, taskStats))
              where
                taskStats :: TaskRunStat
                taskStats = (taskName, diffUTCTime now (deserializeTime $ _taskDistributionStartTime taskMetaData), remoteRunStat)

spawnSlaveProcess :: ProcessId -> TaskDef -> DataDef -> ResultDef -> NodeId -> Process ()
spawnSlaveProcess masterProcess taskDef dataDef resultDef slaveNode = do
  now <- liftIO getCurrentTime
  preparedTaskDef <- prepareSlaveForTask taskDef
  taskTransport <- return $ TaskTransport masterProcess (TaskMetaData (taskDescription taskDef dataDef slaveNode) slaveNode (serializeTime now)) preparedTaskDef dataDef resultDef
  _unusedSlaveProcessId <- spawn slaveNode (slaveTaskClosure taskTransport)
  return ()
  where
    prepareSlaveForTask :: TaskDef -> Process TaskDef
    prepareSlaveForTask (DeployFullBinary program inputMode) = do
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
      return $ PreparedDeployFullBinary hash inputMode
    prepareSlaveForTask d = return d -- nothing to prepare for other tasks (for now)

-- remote logic

-- task result is explicit here since post processing is dependant on that type
type TransportedResult = (TaskMetaData, (Either String (TaskResult, RemoteRunStat)))

handleSlaveTask :: TaskTransport -> Process ()
handleSlaveTask (TaskTransport masterProcess taskMetaData taskDef dataDef resultDef) = do
  handledResult <- (
    do
      acceptTime <- liftIO getCurrentTime
      say $ "processing: " ++ taskName
      result <- liftIO (processTask taskDef dataDef resultDef)
      say $ "processing done for: " ++ taskName
      processingDoneTime <- liftIO getCurrentTime
      liftIO (handleSlaveResult dataDef resultDef result acceptTime processingDoneTime) >>= return . Right
    ) `catch` buildError
  say $ "replying"
  send masterProcess ((taskMetaData, handledResult) :: TransportedResult)
  where
    taskName = _taskName taskMetaData
    buildError :: SomeException -> Process (Either String a)
    buildError e = return $ Left $ "Task execution (for: "++taskName++") failed: " ++ (format $ show e)
      where
        format [] = []
        format ('\\':'n':'\\':'t':rest) = "\n\t" ++ (format rest)
        format (x:rest) = x:[] ++ (format rest)

handleSlaveResult :: DataDef -> ResultDef -> (Either FilePath TaskResult, RunStat) -> UTCTime -> UTCTime -> IO (TaskResult, RemoteRunStat)
handleSlaveResult dataDef resultDef (taskResult, runStat) acceptTime processingDoneTime = do
  res <- handleResult
  return (res, serializedRunStat runStat)
  where
    serializedRunStat (d, t, e) =
      RemoteRunStat (serializeTimeDiff $ diffUTCTime processingDoneTime acceptTime) (serializeTimeDiff d) (serializeTimeDiff t) (serializeTimeDiff e)
    handleResult :: IO TaskResult
    handleResult = case taskResult of
      (Right plainResult) -> handlePlainResult dataDef resultDef plainResult
      (Left resultFilePath) -> handleFileResult dataDef resultDef resultFilePath
      where
        handlePlainResult _ ReturnAsMessage plainResult = return plainResult
        handlePlainResult (HdfsData (config, path)) (HdfsResult outputPrefix outputSuffix) plainResult = writeToHdfs $ writeEntriesToFile config (outputPrefix ++ "/" ++ (stripHDFSPartOfPath path)++outputSuffix) plainResult
        handlePlainResult _ (HdfsResult _ _) _ = error "storage to hdfs with other data source than hdfs currently not supported"
        handlePlainResult _ ReturnOnlyNumResults plainResult = return (plainResult >>= \rs -> [show $ length rs])
        handleFileResult (HdfsData _) ReturnAsMessage resultFilePath = logWarn ("Reading result from file: "++resultFilePath++", with hdfs input this is probably unnecesary imperformant for larger results")
                                                                       >> readResultFromFile resultFilePath
        handleFileResult _ ReturnAsMessage resultFilePath = readResultFromFile resultFilePath
        handleFileResult _ ReturnOnlyNumResults _ = error "not implemented for only returning numbers"
        handleFileResult (HdfsData (_, path)) (HdfsResult outputPrefix outputSuffix) resultFilePath = writeToHdfs $ copyToHdfs resultFilePath (outputPrefix++restpath) (filename'++outputSuffix)
          where (restpath, filename') = splitBasePath (stripHDFSPartOfPath path)
        handleFileResult _ (HdfsResult _ _) _ = error "storage to hdfs with other data source than hdfs currently not supported"
        writeToHdfs writeAction = do
          (_, storeDur) <- measureDuration $ writeAction
          putStrLn $ "stored result data in: " ++ (show storeDur)
          return []
        copyToHdfs localFile destPath destFilename = do
          _ <- executeExternal "hdfs" ["dfs", "-mkdir", "-p", destPath]
          executeExternal "hdfs" ["dfs", "-copyFromLocal", localFile, destPath ++ "/" ++ destFilename]
        readResultFromFile :: FilePath -> IO TaskResult
        readResultFromFile f = readFile f >>= parseResult

-- preparation negotiaion

handleQuerySlavePreparation :: Int -> Process QuerySlavePreparationResponse
handleQuerySlavePreparation hash = liftIO (RemoteStore.get hash) >>= return . (maybe Unprepared (\_ -> Prepared))

handlePrepareSlave :: Int -> ByteString -> Process PrepareSlaveResponse
handlePrepareSlave hash content = liftIO (RemoteStore.put hash content) >> return PreparationFinished

-- simple tasks
showSlaveNodes :: NodeConfig -> IO ()
showSlaveNodes config = withSlaveNodes config (
  \slaveNodes -> putStrLn ("Slave nodes: " ++ show slaveNodes))

showSlaveNodesWithData :: NodeConfig -> NodeConfig -> String -> IO ()
showSlaveNodesWithData slaveConfig hdfsConfig hdfsFilePath = withSlaveNodes slaveConfig (
  \slaveNodes -> do
    nodesWithData <- findNodesWithData (hdfsConfig, hdfsFilePath) slaveNodes
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

taskDescription :: TaskDef -> DataDef -> NodeId -> String
taskDescription t d n = "task: " ++ (describe t) ++ " " ++ (describe d) ++ " on " ++ (show n)
