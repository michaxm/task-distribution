#!/bin/bash -e
rm -rf task-distribution*
cp -r ../.stack-work/install/i386-linux/lts-2.22/7.8.4/doc/task-distribution-0.1.0.3/ task-distribution-0.1.0.3-docs
tar -cf task-distribution-0.1.0.3.tar task-distribution-0.1.0.3-docs/ --format=ustar
