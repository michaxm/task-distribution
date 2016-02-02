module Control.Distributed.Task.DataAccess.DataSource (loadData) where

import qualified Control.Distributed.Task.DataAccess.HdfsDataSource as HDS
import qualified Control.Distributed.Task.DataAccess.SimpleDataSource as SDS
import Control.Distributed.Task.TaskSpawning.TaskDefinition
import Control.Distributed.Task.Types.TaskTypes

loadData :: DataDef -> IO (TaskInput)
loadData (PseudoDB p) = SDS.loadEntries p
loadData (HdfsData p) = HDS.supplyPathWithConfig p >>= HDS.loadEntries
