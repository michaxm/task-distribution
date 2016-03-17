task-distribution
=================

A framework for distributing tasks running on HDFS data using Cloud Haskell.
The goal is speedup through distribution on clusters using regular hardware.
This framework provides different, simple workarounds to transport new code to other cluster nodes.

Examples
--------

I suggest using stack at least for early exploration.

### Basic exploration

* check out project (github), build
* start a local worker: `stack exec slave localhost 44441`
* run the example program: `stack exec example master localhost 44440 fullbinary simpledata collectonmaster`
 
This runs a small _DemoTask_ counting the ratio of entries fulfilling a filter on testdata provided in resources/pseudo-db. Code distribution with fullbinary is done via copying the binary to the slave nodes. 

### Configuration

Some configuration is provided in etc/config - both master and slave programs expect this relativ path to contain some information. Both will crash if an expected configuration is not found. Configuration includes:

* how much local parallelization should be done (using Haskell threads, should be equal or slightly less than the CPU core amount)
* where HDFS/the pseudo-db can be accessed (namenode, port)
* distribution strategy: anywhere/local - anywhere distributes to any unoccupied node. local is an experimental setting that prefers distribution of tasks that the current unoccupied node the data already physically has (at least to some degree). Please note that this will currently require a patched version of hadoop-rpc is needed so you may be required to build task-distribution manually
* log file locations (change task logfiles in a real cluster environment where the project home is in a shared remote fs!)
* resources paths for advanced distribution approaches, mostly relinking

Other config files in etc are:

* hostconfig
* remotelibs
* sourcecodemodules

Do not bother with these as long as nothing complains. They can contain some additional build/host matching info out of scope of the introduction. Create empty if there are complains, then be aware of following problem, dependeng on what you want, that empty file might need to be filled. As always, in doubt the souce code is the best documentation.

### Testing a distributed setup

A docker-compose setup is provided for task-distribution which provides the basic example running on three nodes emulated by containers. In _docker-testsetup_, _container_ contains specifications and scripts to build them all locally. The containers are available at hub.docker.com as well. In _compose_ the example can be started.

API - task-distribution as a library
------------------------------------

The _TaskDistribution_ module is the key distribution API. It is invoked directly to start slave nodes or to perform some other basic operations on top of Cloud Haskell (see the example app for more information).

To launch a distributed calculation, _RunComputation_ is used. It provides different selections of distribution methods, input data specification and result processing.

Most of the provided methods require the user program to support a main call with special arguments. This is needed to execute it in task mode. To setup this support, _RemoteExecutionSupport_ should be used.

[Here](tree/master/doc/module_overview.png) a mostly complete diagram of modules (including internal) can be found.




