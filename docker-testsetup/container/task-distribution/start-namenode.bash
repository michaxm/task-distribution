#!/bin/bash -e

service ssh start &
hadoop namenode
# awaiting user to attach ...
