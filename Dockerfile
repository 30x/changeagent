# Run changeagent in a container
#
# Example:
#
# First, create a discovery file that includes IP addresses and ports of all servers that will be created
# (future releases will do this better)
#
# For example, for a three-node cluster, it should contain
#
# 192.168.100.200:8080
# 192.168.100.300:8080
# 192.168.100.400:8080
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
# docker run -it -p 8080:8080 \
#   -v /home/docker/agent/etc:/etc/changeagent \
#   -v /home/docker/agent/data1:/var/changeagent/data changeagent
#
# Image details:
#
# Changeagent will listen on port 8080. Map it however you want.
#
# For a multi-node configuration, it expects a file called
# /etc/changeagent/disco. This should be the same across
# all agents in the cluster. The directory mounting above is designed to make this work.
# If it's not found, it will run as a single node.
#
# Each agent will store its database in /var/changeagent/data. Since data is important,
# this should be a persistent volume.

# This image has Git, Glide, and rocksdb pre-built for us.
FROM  gbrail/go-rocksdb:1.6-4.2

COPY . /go/src/github.com/30x/changeagent
COPY ./metadata/apigee-file.yaml /

RUN \
    (cd /go/src/github.com/30x/changeagent; glide install) \
 && (cd /go/src/github.com/30x/changeagent; make clean all) \
 && cp /go/src/github.com/30x/changeagent/changeagent / \
 && mkdir -p /var/changeagent/data \
 && mkdir -p /etc/changeagent

EXPOSE 8080

VOLUME /var/changeagent/data /etc/changeagent

ENV LD_LIBRARY_PATH=/usr/local/lib

ENTRYPOINT ["/changeagent", "-logtostderr", "-p", "8080", "-d", "/var/changeagent/data"]
