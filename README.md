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
a fairly straightforward use case, since Raft is designed to maintain a consistently
ordered list of entries among a cluster of servers.

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
* Consumers are occasionally connected, and cannot rely on anything but the abilty
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
    [{"_id": 2,"_ts": 1464804748947570788,"data": {"Hello": "world"}}]

... now we'll post a few more changes ...

Retrieve only the changes since change 12:

    curl http://localhost:9000/changes?since=12
    [{"_id":13,"_ts":1464805001822543303,"data":{"Hello":"world","seq":10}},
    {"_id":14,"_ts":1464805003646294078,"data":{"Hello":"world","seq":11}},
    {"_id":15,"_ts":1464805005509406435,"data":{"Hello":"world","seq":12}}]

Retrieve the changes since change 15, and wait for up to 120 seconds for a
new change to be posted. (And while this API call is running, post a new change
by POSTing to the "/changes" URI:

    $ curl "http://localhost:9000/changes?since=15&block=120"
    [{"_id":16,"_ts":1464805113553316620,"data":{"Hello":"World","seq":13}}]

Now ask again for the changes since change 16, but this time don't post anything
new:

    $ curl "http://localhost:9000/changes?since=16&block=5"
    []

Post a change that includes two tags:

    curl http://localhost:9000/changes      \
      -H "Content-Type: application/json"   \
      -d '{"tags":["testTag","testTag2"],"data":{"Hello":"world", "seq": 12}}'

Retrieve up to 10 changes including only ones that have the tag "testTag":

    curl "http://localhost:9000/changes?limit=10&tag=testTag"
    [{"_id":17,"_ts":1464805241291100178,
      "tags":["testTag","testTag2"],"data":{"Hello":"world","seq":12}}

# Building

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

The binary "agent" will have been built by "make" in the "agent" subdirectory.

For instance, here is a simple way to run on port 9000:

    mkdir data
    ./agent/agent -p 9000 -d ./data -logtostderr

## Running a Cluster

To run a cluster, you will need to create a file that lists the host names and
ports for each cluster node. Then it's necessary to start each agent.

For instance, create a file called "nodes" that contains the following:

    localhost:9000
    localhost:9001
    localhost:9002

Then, start three "agent" binaries:

    mkdir data1
    mkdir data2
    mkdir data3
    ./agent/agent -p 9000 -d ./data1 -logtostderr -s nodes &
    ./agent/agent -p 9001 -d ./data2 -logtostderr -s nodes &
    ./agent/agent -p 9002 -d ./data3 -logtostderr -s nodes &

After about 10 seconds, the three agents will agree on a leader, and you will
be able to make API calls to any cluster node.

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
