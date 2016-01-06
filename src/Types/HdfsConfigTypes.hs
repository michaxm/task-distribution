module Types.HdfsConfigTypes where

type HdfsConfig = (String, Int)
type HdfsPath = String
type HdfsLocation = (HdfsConfig, HdfsPath)
