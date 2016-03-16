#!/bin/bash -e

#quick and dirty register ip at namenode
ssh -o StrictHostKeyChecking=no namenode exit
ssh namenode "echo `ifconfig | awk '/inet addr/{print substr($2,6)}' |grep -v 127.0.0.1`    `hostname` >> /etc/hosts"

hadoop datanode &
cd /usr/local/task-distribution && stack exec slave $1 $2
