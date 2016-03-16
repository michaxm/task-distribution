FROM axm/hadoop-env

#setup & rarely changing dependencies
RUN cd /usr/local && git clone https://github.com/michaxm/task-distribution.git
RUN cd /usr/local/task-distribution && git submodule init && git submodule update --recursive
RUN cd /usr/local/task-distribution && stack setup
RUN cd /usr/local/task-distribution && stack build --only-dependencies
