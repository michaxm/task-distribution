#!/bin/bash -e

docker build -t michaxm/base-extended base-extended
docker build -t michaxm/java java
docker build -t michaxm/hadoop hadoop
docker build -t michaxm/hadoop-env hadoop-env
docker build -t michaxm/task-distribution-base task-distribution-base
./build-task-distribution.bash
