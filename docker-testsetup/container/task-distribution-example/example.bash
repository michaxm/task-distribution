#!/bin/bash -e

if ! hdfs dfs -ls / |grep testdata > /dev/null 2>&1; then
  hdfs dfs -mkdir -p /testdata
  hdfs dfs -copyFromLocal /usr/local/task-distribution/resources/pseudo-db/* /testdata/
fi

stack exec example master `hostname` 44440 fullbinary hdfs:/testdata/ collectonmaster
