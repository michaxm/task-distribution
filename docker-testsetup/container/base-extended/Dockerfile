FROM ubuntu:14.04

# install common stuff
RUN apt-get -qy update && apt-get upgrade -y
RUN apt-get update && apt-get upgrade -y && apt-get install aptitude netcat wget curl -y

# install stack
RUN wget -q -O- https://s3.amazonaws.com/download.fpcomplete.com/ubuntu/fpco.key | sudo apt-key add -
RUN echo 'deb http://download.fpcomplete.com/ubuntu/trusty stable main'|sudo tee /etc/apt/sources.list.d/fpco.list
RUN sudo apt-get update && sudo apt-get install stack -y

#ssh connections
RUN apt-get install ssh -y
ADD id_rsa /root/.ssh/id_rsa
ADD id_rsa.pub /root/.ssh/id_rsa.pub
ADD id_rsa.pub /root/.ssh/authorized_keys
RUN chmod 600 /root/.ssh/id_rsa /root/.ssh/authorized_keys
#ADD known_hosts /root/.ssh/known_hosts
