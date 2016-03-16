FROM axm/task-distribution

#hdfs filtering example
ADD example.bash /usr/local/task-distribution/example.bash
#prepare different task
ADD DemoTask_1 /usr/local/task-distribution/
ADD DemoTask_2 /usr/local/task-distribution/

RUN echo 'cd /usr/local/task-distribution/ && echo run example.bash for a demo' >> /root/.bashrc


