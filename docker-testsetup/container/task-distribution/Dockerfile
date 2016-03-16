FROM axm/task-distribution-base

#provide scripts for starting name/datanodes (see docker-compose)
ADD start-datanode.bash /usr/local/bin/start-datanode.bash
ADD start-namenode.bash /usr/local/bin/start-namenode.bash

#refresh code
RUN cd /usr/local/task-distribution && git pull --rebase
RUN cd /usr/local/task-distribution && stack build

#configuration
ADD config /usr/local/task-distribution/etc/config
