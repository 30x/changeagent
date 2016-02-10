# Run changeagent in a container
#
# Example:
#
# First, create a discovery file that includes IP addresses and ports of all servers that will be created
# (future releases will do this better)
#
# For example, for a three-node cluster, it should contain
#
# 1 192.168.100.200:8080
# 2 192.168.100.300:8080
# 3 192.168.100.400:8080
#
# Create a directory on your docker host for this file and put it in a file called "disco":
#   mkdir ~/agent/etc
#   cp disco ~/agent/etc
#
# Optionally, create a directory on your docker host so that the data can be persistently stored:
#
#   mkdir ~/agent/data1
#
# Now run:
#
# docker run -it -p 8080:8080 -v /home/docker/agent/etc:/etc/changeagent \
#   /home/docker/agent/data1:/var/changeagent/data changeagent -id 1
#
# (Pass a different "-id" parameter for each different cluster node.)
#
# Image details:
#
# Changeagent will listen on port 8080. Map it however you want.
#
# It expects a file called /etc/changeagent/disco. This should be the same across
# all agents in the cluster. The directory mounting above is designed to make this work.
#
# Each agent will store its database in /var/changeagent/data. Since data is important,
# this should be a persistent volume.

FROM  gbrail/go-rocksdb:4.2

COPY . /go/src/revision.aeip.apigee.net/greg/changeagent

WORKDIR /go/src/revision.aeip.apigee.net/greg/changeagent/agent

RUN godep restore \
 && godep go build -v \
 && mkdir -p /var/changeagent/data \
 && mkdir -p /etc/changeagent

EXPOSE 8080

VOLUME /var/changeagent/data /etc/changeagent

ENV LD_LIBRARY_PATH=/usr/local/lib

ENTRYPOINT ["./agent", "-logtostderr", "-p", "8080", "-d", "/var/changeagent/data", "-s", "/etc/changeagent/disco"]

