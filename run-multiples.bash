#!/bin/bash -e

if [ "$#" != 3 ]; then
 echo "usage: $0 prefix num <s|m|p>"
 exit 1
fi

PREFIX=$1
NUM=$2
MODE=$3

if [ "$MODE" != s ] && [ "$MODE" != m ] && [ "$MODE" != p ]; then
 echo "mode == [s]eqential || mode == [m]ultiple || mode == [p]arallel";
 exit 1
fi

if [ -e multiple.out ]; then
 echo multiple.out exists
 exit 1
fi

if [ "$MODE" == "p" ]; then
 stack exec run-demo-task `seq 01 ${NUM} |xargs printf " ${PREFIX}/test%.2d.csv.gz"`
else
 for i in `seq -w 01 ${NUM}`; do
  if [ "$MODE" == "m" ]; then
   stack exec run-demo-task ${PREFIX}/test${i}.csv.gz >> multiple.out &
  else
   stack exec run-demo-task ${PREFIX}/test${i}.csv.gz
  fi
 done
fi

if [ "$MODE" == "m" ]; then
 while [ "`cat multiple.out |wc -l`" != "$NUM" ]; do
  sleep 1
 done
 cat multiple.out
 rm multiple.out
fi
