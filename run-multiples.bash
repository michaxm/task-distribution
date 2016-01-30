#!/bin/bash -e

if [ "$#" != 3 ]; then
 echo "usage: $0 prefix num <p|s>"
 exit 1
fi

PREFIX=$1
NUM=$2
MODE=$3

if [ "$MODE" != p ] && [ "$MODE" != s ]; then
 echo "mode == [s]eqential || mode == [p]arallel";
 exit 1
fi

if [ -e out ]; then
 echo out exists
 exit 1
fi


for i in `seq -w 01 ${NUM}`; do
 if [ "$MODE" == "p" ]; then
  stack exec run-demo-task ${PREFIX}/test${i}.csv.gz >> out &
 else
  stack exec run-demo-task ${PREFIX}/test${i}.csv.gz
 fi
done

if [ "$MODE" == "p" ]; then
 while [ "`cat out | wc -l`" != "$NUM" ]; do
  sleep 1;
 done
 cat out
 rm out
fi
