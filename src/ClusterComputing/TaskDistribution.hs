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
 This is the final building block of the worker task execution, calling TaskSpawning.processTask.
-}
type TransportedResult = (TaskMetaData, Either String (TaskResult, RemoteRunStat)) -- signature here defines transported type, handle with care
workerTask :: TaskTransport -> Process () -- TODO: have a node local config?
workerTask = handleWorkerTask

queryWorkerPreparation :: QueryWorkerPreparationRequest -> Process ()
queryWorkerPreparation (masterProcess, hash) = handleQueryWorkerPreparation hash >>= send masterProcess

prepareWorker :: PrepareWorkerRequest -> Process ()
prepareWorker (masterProcess, hash, content) = handlePrepareWorker hash content >>= send masterProcess

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
queryWorkerPreparation__static :: S.Static (QueryWorkerPreparationRequest -> Process())
queryWorkerPreparation__static = S.staticLabel "ClusterComputing.TaskDistribution.queryWorkerPreparation"
queryWorkerPreparation__sdict :: S.Static (PS.SerializableDict QueryWorkerPreparationRequest)
queryWorkerPreparation__sdict = S.staticLabel "ClusterComputing.TaskDistribution.queryWorkerPreparation__sdict"
prepareWorker__static :: S.Static (PrepareWorkerRequest -> Process())
prepareWorker__static = S.staticLabel "ClusterComputing.TaskDistribution.prepareWorker"
prepareWorker__sdict :: S.Static (PS.SerializableDict PrepareWorkerRequest)
prepareWorker__sdict = S.staticLabel "ClusterComputing.TaskDistribution.prepareWorker__sdict"
__remoteTable :: S.RemoteTable -> S.RemoteTable
__remoteTable =
  (S.registerStatic "ClusterComputing.TaskDistribution.workerTask" (R1.toDynamic workerTask))
  . (S.registerStatic "ClusterComputing.TaskDistribution.workerTask__sdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict TaskTransport)))
  . (S.registerStatic "ClusterComputing.TaskDistribution.workerTask__tdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict ())))
  . (S.registerStatic "ClusterComputing.TaskDistribution.queryWorkerPreparation" (R1.toDynamic queryWorkerPreparation))
  . (S.registerStatic "ClusterComputing.TaskDistribution.queryWorkerPreparation__sdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict QueryWorkerPreparationRequest)))
  . (S.registerStatic "ClusterComputing.TaskDistribution.queryWorkerPreparation__tdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict ())))
  . (S.registerStatic "ClusterComputing.TaskDistribution.prepareWorker" (R1.toDynamic prepareWorker))
  . (S.registerStatic "ClusterComputing.TaskDistribution.prepareWorker__sdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict PrepareWorkerRequest)))
  . (S.registerStatic "ClusterComputing.TaskDistribution.prepareWorker__tdict" (R1.toDynamic (PS.SerializableDict :: PS.SerializableDict ())))

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

queryWorkerPreparationClosure :: QueryWorkerPreparationRequest -> S.Closure (Process ())
queryWorkerPreparationClosure =
   ((S.closure
    (queryWorkerPreparation__static
     `S.staticCompose`
     (staticDecode
      queryWorkerPreparation__sdict)))
   . B.encode)
   
prepareWorkerClosure :: PrepareWorkerRequest -> S.Closure (Process ())
prepareWorkerClosure =
   ((S.closure
    (prepareWorker__static
     `S.staticCompose`
     (staticDecode
      prepareWorker__sdict)))
   . B.encode)

rtable :: RemoteTable
rtable = __remoteTable $ initRemoteTable
-- END bindings for node communication

type NodeConfig = (String, Int)

startWorkerNode :: NodeConfig -> IO ()
startWorkerNode (host, port) = do
  initDefaultLogging (show port)
  backend <- initializeBackend host (show port) rtable
  putStrLn "initializing worker"
  startSlave backend

executeDistributed :: (Serializable a) => NodeConfig -> TaskDef -> [DataDef] -> ResultDef -> ([a] -> IO ())-> IO ()
executeDistributed (host, port) taskDef dataDefs resultDef resultProcessor = do
  backend <- initializeBackend host (show port) rtable
  startMaster backend $ \workerNodes -> do
    result <- executeOnNodes taskDef dataDefs resultDef workerNodes
    liftIO $ resultProcessor result

executeOnNodes :: (Serializable a) => TaskDef -> [DataDef] -> ResultDef -> [NodeId] -> Process [a]
executeOnNodes taskDef dataDefs resultDef workerNodes = do
  if null workerNodes
    then say "no workers => no results (ports open?)" >> return [] 
    else executeOnNodes' taskDef dataDefs resultDef workerNodes

data DistributionStrategy = NextFreeNodeWithDataLocality

executeOnNodes' :: (Serializable a) => TaskDef -> [DataDef] -> ResultDef -> [NodeId] -> Process [a]
executeOnNodes' taskDef dataDefs resultDef workerNodes = do
  masterProcess <- getSelfPid
  before <- liftIO getCurrentTime
  taskResults <- distributeWork masterProcess NextFreeNodeWithDataLocality taskDef dataDefs resultDef workerNodes 0 workerNodes (length dataDefs) []
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

-- try to allocate work until no work can be delegated to the remaining free workers, then collect a single result, repeat
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
distributeWork masterProcess NextFreeNodeWithDataLocality taskDef (dataDef:rest) resultDef workerNodes numBusyNodes freeNodes numWaiting collected = do
  nodesWithData <- liftIO $ findNodesWithData (_hdfsInputLocation dataDef) freeNodes
  if null nodesWithData
    then do
    if numBusyNodes <= 0 then error "no worker accepts the task" else say $ "collecting since all workers are busy"
    (taskMetaData, nextResult) <- collectSingle
    distributeWork masterProcess NextFreeNodeWithDataLocality taskDef (dataDef:rest) resultDef workerNodes (numBusyNodes-1) ((_workerNodeId taskMetaData):freeNodes) (numWaiting-1) (maybe collected (:collected) nextResult)
    else do
    selectedWorker <- return (head nodesWithData)
    spawnWorkerProcess' selectedWorker
    say $ "spawning on: " ++ (show $ selectedWorker)
    distributeWork masterProcess NextFreeNodeWithDataLocality taskDef rest resultDef workerNodes (numBusyNodes+1) (delete (head nodesWithData) freeNodes) numWaiting collected
      where
        spawnWorkerProcess' = spawnWorkerProcess masterProcess taskDef dataDef resultDef

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

spawnWorkerProcess :: ProcessId -> TaskDef -> DataDef -> ResultDef -> NodeId -> Process ()
spawnWorkerProcess masterProcess taskDef dataDef resultDef workerNode = do
  now <- liftIO getCurrentTime
  preparedTaskDef <- prepareWorkerForTask taskDef
  taskTransport <- return $ TaskTransport masterProcess (TaskMetaData (taskDescription taskDef dataDef workerNode) workerNode (serializeTime now)) preparedTaskDef dataDef resultDef
  _unusedWorkerProcessId <- spawn workerNode (workerTaskClosure taskTransport)
  return ()
  where
    prepareWorkerForTask :: TaskDef -> Process TaskDef
    prepareWorkerForTask (DeployFullBinary program) = do
      hash <- return $ RemoteStore.calculateHash program
      say $ "preparing for " ++ (show hash)
      _ <- spawn workerNode (queryWorkerPreparationClosure (masterProcess, hash))
      prepared <- expect -- TODO match workerNodes?
      case prepared of
       Prepared -> say $ "already prepared: " ++ (show hash)
       Unprepared -> do
         say $ "distributing " ++ (show hash)
         before <- liftIO getCurrentTime
         _ <- spawn workerNode (prepareWorkerClosure (masterProcess, hash, program))
         ok <- expect -- TODO match workerNodes?
         after <- liftIO getCurrentTime
         case ok of
          PreparationFinished -> say $ "distribution took: " ++ (show $ diffUTCTime after before)
      return $ PreparedDeployFullBinary hash
    prepareWorkerForTask d = return d -- nothing to prepare for other tasks (for now)

-- remote logic

handleWorkerTask :: TaskTransport -> Process ()
handleWorkerTask (TaskTransport masterProcess taskMetaData taskDef dataDef resultDef) = do
  handledResult <- (
    do
      acceptTime <- liftIO getCurrentTime
      say $ "processing: " ++ taskName
      result <- liftIO (processTask taskDef dataDef)
      say $ "processing done for: " ++ taskName
      processingDoneTime <- liftIO getCurrentTime
      liftIO (handleWorkerResult dataDef resultDef result acceptTime processingDoneTime) >>= return . Right
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

handleWorkerResult :: DataDef -> ResultDef -> (Either FilePath TaskResult, RunStat) -> UTCTime -> UTCTime -> IO (TaskResult, RemoteRunStat)
handleWorkerResult dataDef resultDef (taskResult, runStat) acceptTime processingDoneTime = do
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

handleQueryWorkerPreparation :: Int -> Process QueryWorkerPreparationResponse
handleQueryWorkerPreparation hash = liftIO (RemoteStore.get hash) >>= return . (maybe Unprepared (\_ -> Prepared))

handlePrepareWorker :: Int -> ByteString -> Process PrepareWorkerResponse
handlePrepareWorker hash content = liftIO (RemoteStore.put hash content) >> return PreparationFinished

-- simple tasks

showWorkerNodes :: NodeConfig -> IO ()
showWorkerNodes config = withWorkerNodes config (
  \workerNodes -> putStrLn ("Worker nodes: " ++ show workerNodes))

showWorkerNodesWithData :: NodeConfig -> NodeConfig -> String -> IO ()
showWorkerNodesWithData workerConfig hdfsConfig hdfsFilePath = withWorkerNodes workerConfig (
  \workerNodes -> do
    nodesWithData <- findNodesWithData (hdfsConfig, hdfsFilePath) workerNodes
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
