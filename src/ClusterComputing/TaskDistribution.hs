module ClusterComputing.TaskDistribution (
  startSlaveNode,
  executeDistributed,
  showSlaveNodes,
  showSlaveNodesWithData,
  shutdownSlaveNodes) where

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
import Data.ByteString.Lazy (ByteString)
import Data.List (delete)
import Data.List.Split (splitOn)
import qualified Data.Rank1Dynamic as R1 (toDynamic)
import Data.Time.Clock (UTCTime, diffUTCTime, NominalDiffTime, getCurrentTime)

import ClusterComputing.DataLocality (findNodesWithData)
import ClusterComputing.HdfsWriter (writeEntriesToFile)
import ClusterComputing.LogConfiguration
import ClusterComputing.TaskTransport
import qualified TaskSpawning.BinaryStorage as RemoteStore
import TaskSpawning.ExecutionUtil (measureDuration, executeExternal)
import TaskSpawning.TaskSpawning (processTask, RunStat)
import TaskSpawning.TaskTypes

{-
 The bits in this file are arranged so that the less verbose Template Haskell version would work. That version is not used due to incompability
 issues between packman and Template Haskell though, but is retained in the commens for documentational purposes.
-}

-- BEGIN bindings for node communication
{-
 This is the final building block of the slave task execution, calling TaskSpawning.processTask.
-}
type TransportedResult = (TaskMetaData, Either String (TaskResult, RemoteRunStat)) -- signature here defines transported type, handle with care
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

data DistributionStrategy = NextFreeNodeWithDataLocality

executeOnNodes' :: (Serializable a) => TaskDef -> [DataDef] -> ResultDef -> [NodeId] -> Process [a]
executeOnNodes' taskDef dataDefs resultDef slaveNodes = do
  masterProcess <- getSelfPid
  before <- liftIO getCurrentTime
  taskResults <- distributeWork masterProcess NextFreeNodeWithDataLocality taskDef dataDefs resultDef slaveNodes 0 slaveNodes (length dataDefs) []
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

-- try to allocate work until no work can be delegated to the remaining free slaves, then collect a single result, repeat
distributeWork :: (Serializable a) => ProcessId -> DistributionStrategy -> TaskDef -> [DataDef] -> ResultDef -> [NodeId] -> Int -> [NodeId] -> Int -> [([a], TaskRunStat)] -> Process ([a], [TaskRunStat])
-- done waiting:
distributeWork _ _ _ _ _ _ _ _ 0 collected = do
  say "all tasks accounted for"
  return $ let (res, stat) = unzip collected in (concat $ reverse res, stat)
-- done distributing:
distributeWork _ _ _ [] _ _ _ _ numWaiting collected = do
  say $ "expecting " ++ (show numWaiting) ++ " more responses"
  (_, nextResult) <- collectSingle
  distributeWork undefined undefined undefined [] undefined undefined undefined undefined (numWaiting-1) (maybe collected (:collected) nextResult)
-- distribute as much as possible, otherwise collect single result and retry :
distributeWork masterProcess NextFreeNodeWithDataLocality taskDef (dataDef:rest) resultDef slaveNodes numBusyNodes freeNodes numWaiting collected = do
  nodesWithData <- liftIO $ nodesWithData' dataDef
  if null nodesWithData
    then do
    if numBusyNodes <= 0 then error "no slave accepts the task" else say $ "collecting since all slaves are busy"
    (taskMetaData, nextResult) <- collectSingle
    distributeWork masterProcess NextFreeNodeWithDataLocality taskDef (dataDef:rest) resultDef slaveNodes (numBusyNodes-1) ((_slaveNodeId taskMetaData):freeNodes) (numWaiting-1) (maybe collected (:collected) nextResult)
    else do
    selectedSlave <- return (head nodesWithData)
    spawnSlaveProcess' selectedSlave
    say $ "spawning on: " ++ (show $ selectedSlave)
    distributeWork masterProcess NextFreeNodeWithDataLocality taskDef rest resultDef slaveNodes (numBusyNodes+1) (delete (head nodesWithData) freeNodes) numWaiting collected
      where
        spawnSlaveProcess' = spawnSlaveProcess masterProcess taskDef dataDef resultDef
        nodesWithData' (HdfsData loc) = findNodesWithData loc freeNodes
        nodesWithData' (PseudoDB _) = return freeNodes -- no data locality strategy for simple pseudo db

collectSingle :: (Serializable a) => Process (TaskMetaData, Maybe (a, TaskRunStat))
collectSingle = do
  (taskMetaData, nextResult) <- expect -- :: Hint: Process TransportedResult
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
handleSlaveTask (TaskTransport masterProcess taskMetaData taskDef dataDef resultDef) = do
  handledResult <- (
    do
      acceptTime <- liftIO getCurrentTime
      say $ "processing: " ++ taskName
      result <- liftIO (processTask taskDef dataDef)
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
        handlePlainResult (HdfsData (config, path)) (HdfsResult outputPrefix) plainResult = writeToHdfs $ writeEntriesToFile config (outputPrefix ++ "/" ++ (getFileNamePart path)) plainResult
        handlePlainResult _ (HdfsResult _) _ = error "storage to hdfs with other data source than hdfs currently not supported"
        handlePlainResult _ ReturnOnlyNumResults plainResult = return (plainResult >>= \rs -> [show $ length rs])
        handleFileResult _ ReturnAsMessage _ = error "not implemented, returning saved contents of a file does not make much sense"
        handleFileResult _ ReturnOnlyNumResults _ = error "not implemented for only returning numbers"
        handleFileResult (HdfsData (_, path)) (HdfsResult outputPrefix) resultFilePath = writeToHdfs $ executeExternal "hdfs" ["dfs", "-copyFromLocal", resultFilePath, outputPrefix ++ "/" ++ (getFileNamePart path)]
        handleFileResult _ (HdfsResult _) _ = error "storage to hdfs with other data source than hdfs currently not supported"
        getFileNamePart path = let parts = splitOn "/" path in if null parts then "" else parts !! (length parts -1)
        writeToHdfs writeAction = do
          (_, storeDur) <- measureDuration $ writeAction
          putStrLn $ "stored result data in: " ++ (show storeDur)
          return []

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

class Describable a where
  describe :: a -> String
instance Describable TaskDef where
  describe (SourceCodeModule n _) = n
  describe (DeployFullBinary _) = "user function defined in main"
  describe (PreparedDeployFullBinary _) = "user function defined in main (prepared)"
  describe (UnevaluatedThunk _ _) = "unevaluated user function"
  describe (ObjectCodeModule _) = "object code module"
instance Describable DataDef where
  describe (HdfsData (_, p)) = p
  describe (PseudoDB n) = show n
