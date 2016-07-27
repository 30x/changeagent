# ChangeAgent

ChangeAgent is a small distributed data store that is especially well-suited for
tracking changes to data and distributing them to other systems. It has the
following characteristics:

## Data Model

Each record in the database is also referred to as a "change". Changes are
kept in a sequential list. Each change has the following fields:

* Index: A global sequential ordering among all changes in the system
* Type: An integer that may be used to distinguish different types of records
  if necessary. Types less than zero are reserved for internal changes.
* Tags: An optional collection of strings. These may be used to filter changes
  into different categories during consumption.
* Data: Arbitrary JSON data.
* Timestamp: The time that the change was made. (However, the canonical ordering
  for changes is the index. Changes created on different nodes may not necessarily
  have their timestamps in order. Timestamps are for informational purposes.)

## API

All records, upon insert, are added to the global change list. This puts them
in a sequential order across all records in the system. No two records will
ever have the same index, and once inserted, the index of a record will
never change.

Clients may query the global change list by index, and ask for any set of
records across the range. This is the primary mechanism for replicating
sets of records between database instances and to clients.

Clients may use an optional long-polling mechanism, in which the client asks
for records that are higher than a certain sequence, and waits for a period
of time until a record is inserted. This allows clients to quickly and
efficiently replicate their view of the database even if the server is
periodically unavailable or the network is unreliable.

In addition, clients may filter queries to the change list using tags. This way,
records for different tenants or use cases can be filtered.

## Architecture

changeagent servers are stand-alone, self-contained servers that persist data on
the local disk using [RocksDB](http://rocksdb.org/). A single changeagent server
can run stand-alone as a quick and simple way to track change lists, but
for most use cases, servers should be clustered.

Clustering is implemented using [Raft](https://raft.github.io/). This makes changeagent
a fairly straightforward implementation of Raft, since Raft is designed to maintain a consistently
ordered list of entries among a cluster of servers, which is pretty much all that
changeagent does.

## Characteristics

Given these implementation, changeagent is designed to offer the following
characteristics:

* All changes will eventually be visible on all servers in the cluster in the same order.
No two servers will ever display the list of changes in a different order.
* However, changes will not necessarily propagate to all servers at once, so
different servers in the cluster may have shorter change lists than others.
* No change can be accepted into the system unless a quorum (N / 2 + 1) of nodes
is available and can interoperate.
* As such, the system is available for writes as long as a quorum is present.
* Furthermore, each individual server can always serve up its own copy of the change
list, so the system is available for reads as long as a single node is reachable.

## Use Cases

changeagent is designed for storing configuration changes for a distributed system.
It is excellent for workloads that have the following characteristics:

* Changes must be propagated in order to a large number of consumers
* Consumers are occasionally connected, and cannot rely on anything but the ability
to make outgoing HTTP requests.
* Changes are produced at a "human" scale -- 10s of changes per second, at most.
* Latency for retrieving lists of changes is important.
* Latency of introducing new changes is less important.

# Example

Post a new change. The API expects an element called "data" which will
hold the actual change data:

    curl http://localhost:9000/changes    \
      -H "Content-Type: application/json" \
      -d '{"data":{"Hello":"world"}}'

Retrieve the first 100 changes since the beginning of time:

    curl http://localhost:9000/changes
    {"changes":[{"_id": 2,"_ts": 1464804748947570788,"data": {"Hello": "world"}}],"atStart":true,"atEnd":true}

... now we'll post a few more changes ...

Retrieve only the changes since change 12:

    curl http://localhost:9000/changes?since=12
    {"changes":[{"_id":13,"_ts":1464805001822543303,"data":{"Hello":"world","seq":10}},
    {"_id":14,"_ts":1464805003646294078,"data":{"Hello":"world","seq":11}},
    {"_id":15,"_ts":1464805005509406435,"data":{"Hello":"world","seq":12}}],
    "atStart":false,"atEnd":true}

Retrieve the changes since change 15, and wait for up to 120 seconds for a
new change to be posted. (And while this API call is running, post a new change
by POSTing to the "/changes" URI:

    $ curl "http://localhost:9000/changes?since=15&block=120"
    {"changes":[{"_id":16,"_ts":1464805113553316620,"data":{"Hello":"World","seq":13}}],
    "atStart":false,"atEnd":true}

Now ask again for the changes since change 16, but this time don't post anything
new:

    $ curl "http://localhost:9000/changes?since=16&block=5"
    {"changes":[],"atStart":false,"atEnd":true}

Post a change that includes two tags. Tags are always specified in an array
so that each entry can have multiple tags.

    curl http://localhost:9000/changes      \
      -H "Content-Type: application/json"   \
      -d '{"tags":["testTag","testTag2"],"data":{"Hello":"world", "seq": 12}}'

Retrieve up to 10 changes including only ones that have the tag "testTag":

    curl "http://localhost:9000/changes?limit=10&tag=testTag"
    {"changes":[{"_id":17,"_ts":1464805241291100178,
      "tags":["testTag","testTag2"],"data":{"Hello":"world","seq":12}}],
      "atStart":true,"atEnd":true}

It is also possible to include multiple "tag" query parameters on an API
call. In that case, all the tags are ORed together. In other words,
changes will be included in the response if they include any of the tags
specified by a "tag" parameter.

(On the other hand, if no tags are specified, then all changes will be
included in the response.)

# Running on Docker

The simplest way to get changeagent running is to use the Docker image. For
instance, the following command will get it running in standalone mode on
your Docker host with port 8080.

    docker run -it --rm -p 8080:8080 gbrail/changeagent

See elsewhere in this README and the Dockerfile for instructions on running it in a cluster,
or for instructions on how to handle persistent data.

# Building from Source

The product is built in Go, but it relies on the "glide" package management
tool for dependency management, and on the rocksdb library for data storage.
Both must be installed to build, in addition to Go.

## Installing Prerequisites on OS X / Darwin

First, install [Homebrew](http://brew.sh/). This will serve you well in the future
if you do not already have it.

Then install the following prerequisites:

    brew install go
    brew install glide
    brew install rocksdb

## Installing Prerequisites on Linux

The prerequisites will come from different places depending on the OS that you
are using.

## Building

Glide requires that the go "vendor extension" is enabled:

    export GO15VENDOREXPERIMENT=1

Install the dependencies using glide:

    glide install

Build:

    make

In addition, there are a few other top-level targets:

* make test: Runs tests in all directories. All of them should pass..
* make clean: Cleans up the "agent" binary and intermediate files.

# Running

## Running a Single Node

To run a standalone node, you will need to pass a port and a directory to
store the data.

The binary "changeagent" will have been built by "make" in the "agent" subdirectory.

For instance, here is a simple way to run on port 9000:

    mkdir data
    ./changeagent -p 9000 -d ./data -logtostderr

## Running a Cluster

When a changeagent node is started, it runs in standalone mode. In order to create
a cluster, cluster configuration is propagated via the Raft protocol just like
any other change. Once propagated, each cluster member has information about
the other members in its local data store.

In order to create a cluster, we start a set of standalone modes, and then gradually
build the cluster.

For instance, start three "changeagent" binaries"

    mkdir data1
    mkdir data2
    mkdir data3
    ./changeagent -p 9000 -d ./data1 -logtostderr &
    ./changeagent -p 9001 -d ./data2 -logtostderr &
    ./changeagent -p 9002 -d ./data3 -logtostderr &

Now we will use the first one to build the cluster, by making three API calls:

    curl http://localhost:9000/cluster/members -d 'address=server1:9000'
    curl http://localhost:9000/cluster/members -d 'address=server2:9001'
    curl http://localhost:9000/cluster/members -d 'address=server3:9002'

At the end of the third API call, then all three members will be part of the
cluster. The cluster information is persisted in each member's database, so
upon restart all of the nodes will continue to find each other.

Note that in this example, we used "server1" as the host name, and not "localhost."
"server1" is a stand-in for the "real" host name or IP address where each node
is running. This is very important, because all cluster nodes need to be able to
reach all the other cluster nodes using this address. "localhost" will not work
unless all the nodes will always be running on the same host.

Also notice that the first thing that we do is to tell one of the cluster nodes
to add itself as a member, with its own address. This is also critical because
the first node in the cluster will advertise its address to the other nodes, and
it does not necessarily know which address that they should use.

Calls to add or remove nodes to the cluster must only be made to a single node.
The change will be propagated to the whole cluster just like any other change,
and will take effect on all nodes.

## Managing a Cluster

It is always possible to see which nodes are in a cluster:

    curl http://localhost:9000/cluster

Furthermore, other API calls behave the way you might expect. To see
cluster members:

    curl http://localhost:9000/cluster/members

To see information about just one member, you need to know its member ID:

    curl http://localhost:9000/cluster/members/MEMBERID

And correspondingly, to delete a cluster node:

    curl http://localhost:9000/cluster/members/MEMBERID -X DELETE

Deleted nodes retain their data, but they go back to being standalone nodes.

# Additional Features

## Authorization

Changeagent supports HTTP Basic authentication using the optional "-pw" command
line flag. If set, this flag must point to a valid password file created
using the "htpasswd" utility.

The password file is read when changeagent starts, and then is polled every
five seconds and reloaded if the file changes. Polling happens automatically
and in a thread-safe way. There is no other way to update the password file.

The password file supports *only* password entries encrypted using bcrypt.

To create a new password file called "passwd" containing the password for the user "foo," for
instance, type:

    htpasswd -cB passwd foo

and then, to add an entry for the user "bar" later, type:

    htpasswd -B passwd bar

("htpasswd" is a part of the Apache HTTP server. On Mac, it should show up
if you use Homebrew to install "httpd24".)

## Cluster Port

Normally, the "-p" argument to changeagent specifies what port the server
will listen on. However, it is possible to configure the server to listen for
inter-cluster traffic on a different port.

The "-cp" option does this -- if specified, then inter-cluster communication uses
the separate port.

## TLS Support

All configuration of changeagent may be secured using TLS. The TLS key,
certificate, and port may be specified separately, and a different set of keys
may be used for each.

For regular API communications, there are two basic configurations.

To ensure that incoming API calls are secured using TLS, but without validating
the client certificate, specify a TLS client certificate and private key, and
use the "-cp" option to specify which port listens using TLS. Also,
in this mode you must use a separate cluster port. Cluster communications will
not use TLS in this configuration.

It is possible to use both the "-p" and "-cp" options so that the server
listens for TLS and non-TLS connections at the same time.

For instance, assuming that the TLS key and certificate are secured using
"cacert.pem" and "cakey.pem":

    changeagent -sp 8443 -cp 9000 -cert cacert.pem -key cakey.pem

To additionally require that clients use a client-side certificate, and to
verify that the certificate was issued by a particular CA, add the "-cas"
option:

    changeagent -sp 8443 -cp 9000 -cert cacert.pem -key cakey.pem -cas cacerts.pem

To secure inter-cluster communication, you need to specify the key and cert for
inter-cluster TLS, and must also specify the CA. Each changeagent in the cluster
need not have the same key, but all agents in the cluster must use certificates
that are signed by one of the CAs known to all servers, or else the cluster
will not be able to communicate:

    changagent -p 8000 -cp 9443 -ckey clusterkey.pem -ccert clustercert.pem -cca clusterca.pem

And finally, it's possible to combine all this together:

    changeagent -sp 8443 -cp 9443 -cert cacert.pem -key cakey.pem -cas cacerts.pem \
      -ckey clusterkey.pem -ccert clustercert.pem -cca clusterca.pem

## Configuration

There are a few tunable configuration parameters. A sample configuration file
is shown below:

    minPurgeRecords: 0
    minPurgeDuration: "0"
    electionTimeout: 10s
    heartbeatTimeout: 2s

The configuration file name is passed to changeagent using the "-f" argument.
If there is no file there when the server is first started, a default
configuration is written to the file.

The "/config" URI path also allows access to the configuration file. GET returns
the current configuration, and PUT may be used to replace the configuration.
When configuration is replaced, the change is propagated across the cluster.
That way a configuration change may be made consistent on all nodes.

The following configuration parameters are supported:

* heartbeatTimeout: The amount of time between heartbeat messages from the
cluster leader to the rest of the members. A shorter timeout means that
a failed leader will be detected more quickly, but a too-short timeout
will result in a "flapping" cluster.
* electionTimeout: The amount of time that a node will wait after receiving
a heartbeat from the leader before deciding that the leader is down and
becoming a candidate. This value must be at least twice the heartbeat
timeout.
* minPurgeDuration: If this is set *and* minPurgeRecords is set, then
records that are older than this duration may be automatically removed
from the database, as long as at least "minPurgeRecords" remain.
* minPurgeRecords: Must be set greater than zero
along with minPurgeDuration to specify the minimum amount of records that must
remain in the database after a purge.

All of the "duration" parameters take a duration as parsed by the function
"time.ParseDuration" in Go. To quote that doc:

    A duration string is a possibly signed sequence of decimal numbers, each
    with optional fraction and a unit suffix, such as "300ms", "-1.5h" or
    "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

## Logging

Logging is provided by the [glog](https://github.com/golang/glog) package.
By default, logs are written to a log file, but most of the time we use
the "-logtostderr" option to ensure that they are written to standard
error output instead.

To see exhaustive debug information about everything that changeagent does,
increase the log level to 3 or higher. For instance "-v 5" will produce a lot
of debugging output.

## Health Check API

The /health URI path will return a 200 response and the text "ok". This
API is the correct API for a load balancer to use when implementing a health check.

To mark down the server, PUT the value "up=false" to the /health endpoint, like this:

    curl http://localhost:9000/health -d 'up=false' -X PUT

Similarly, "up=true" marks it back up. Once the server has been marked down,
GET requests to /health will return 503 (Service Unavailable) instead of 200.
This does not start the server from responding to HTTP requests, but it will
tell the load balancer to stop sending new traffic.

Finally, even if authentication is enabled, GET requests to /health do
not require authentication.

So, in order to perform a rolling restart of a cluster without interrupting
API calls:

0) Ensure that the load balancer has a health check configured that will
"GET /health" and mark the server down unless the response code is 200.
1) For each node in the cluster...
2) PUT "up=false" to /health so that the load balancer will see the health
check fail.
3) Wait for at least as long as the load balancer's health-check timeout so that
we are sure that the server has been marked down.
4) Kill the server process. The server will wait up to 60 seconds for running
API calls to complete before exiting.
5) Do whatever you need to do and restart the server.

## Diagnostics API

There are a series of diagnostics APIs available under the "/diagnostics"
URI path. Calling "/diagnostics" will return a set of hyperlinks to the
currently-enabled diagnostics.

## Web Hook Support

changeagent supports optional web hooks. These web hooks are designed to support
use cases in which we wish to validate a set of changes before putting them
in the change log. They can also be used to update another system, and ensure
that a record is placed in the change log only if that update succeeds.

In other words, changeagent supports an optional list of URIs that the leader will
invoke before submitting any change to the distributed log. The API call
made to the webhook will include the original request, in JSON format.
If the web hook returns anything other than a 200 response code, then the
change will be rejected.

Web hooks are configured using the configuration file mentioned previously.
Here is a sample set of configuration file entries that enable webhooks:

    minPurgeRecords: 123
    minPurgeDuration: 1h
    electionTimeout: 10m
    heartbeatTimeout: 1s
    webHooks:
    - uri: http://foo.com
      headers:
        foo: bar
    - uri: http://bar.com

In other words, in YAML language, wen hooks are expressed by a list of
objects. Each one has a required "uri" parameter, and an optional
map called "headers" that specifies HTTP request headers that should
be sent to the web hook.

# Rationale

Change agent was created to handle a particular use case with the following characteristics.
In particular, there would be many nodes, on many types of networks, that need to be
notified of changes to a set of central data.

We felt that the simplest and most reliable way to get these nodes up to date would
be to allow them to maintain a local copy of the data and to "long poll" for changes.
This way changes made on the central system are not slowed down by slow clients,
and slow clients or clients on slow networks will automatically adjust while still
getting accurate results. Plus, by basing this on an HTTP API, clients need only
to be able to make outgoing HTTP requests in order to be upgraded.

In order for this to be reliable, the servers maintaining the change list need to be
highly available for reads, which means load balancing. Once we are making HTTP
requests to get lists of changes in order, clients may be frequently re-load-balanced
to another server. None of this works if different servers maintain the change list
in different orders. So, all the servers need to maintain the change list in the same
order.

Once we put these things together, we see that the Raft protocol is ideally suited
for this use case. In particular, it guarantees that updates are always propagated in the
same order, and as long as a quorum of nodes is available we can make updates.

However, Raft is not suited for all use cases. In particular, all writes go to the
leader, and the leader must communicate with a quorum of nodes on every write.
So, it is not ideal when high write throughput is needed. Furthermore, if there
is not a quorum, no writes are possible.

We feel that these compromises are acceptable for our use case, which is for data
that does not change often. In these cases, we are also willing to accept less
write availability for more consistency.

Given an extremely over-simplified reading of the CAP theorem, then, it could
be said that this project achieves "CA," like other systems such as etcd and
Zookeeper, and not "AP" like a system such as Dynamo or Cassandra.

## Why Didn't You Just?

There are many other ways to solve this problem. Here's why we think that
changeagent is best for our use case.

### etcd

etcd has a very similar design to changeagent in that it is built in Go and
uses the Raft protocol and a local database. However it has two limitations.
First, it has a different data model which organizes the world into a hierarchical
directory structure. Although it supports watchers via HTTP long-polling as well,
those watchers work only on parts of the directory structure. So, distributing
changes of many different types, as we wish to do, would require a number of different
simultaneous APIs to watch the change list.

Plus, etcd uses a fairly simple in-memory database, plus a write-ahead log. We didn't
feel that it would handle the amount of data that we felt we'd need to store,
which could number into the millions of records.

### Zookeeper

We already use Zookeeper in production at Apigee and we are well aware of its
strengths and weaknesses. Like etcd (which attempted to improve on Zookeeper),
ZK relies on a hierarchical directory structure and would require us to manage
many watchers. Plus, Zookeeper uses a binary protocol and not HTTP, which would
make "raw" ZK a non-starter for our use case.

### Postgres

Another approach to this problem would be to simply put all the data in Postgres
and query a "change" table. This is a simple solution that has a lot of merit.
However, we see changeagent as a low-level service, and it should require much
less configuration and management than Postgres. Furthermore, Postgres is not
itself highly-available, so we'd rely on a service like Amazon RDS, or we'd have
to instruct customers how to make it highly-available for writes on its own.
Also, we'd need to layer an HTTP API on top anyway.

### Cassandra

We also use Cassandra extensively and it also has strengths and weaknesses.
Maintaining an ordered change list, across nodes, in a consistent way is not
what Cassandra was designed to do, and it would not be good for that task.

### CouchDB

CouchDB is an excellent choice for this problem, and it inspired the changeagent
API, because any collection of data maintains a "changes" feed that may be
accessed via long polling. However, making CouchDB reliable out of the box would
require setting up master-slave replication, just like Postgres, which is a
pattern that we prefer to avoid because it requires either manual intervention
or additional complex automation in the event of a failure of the master.
