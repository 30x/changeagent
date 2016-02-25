# TODO

## High Priority

Discovery.

Support deletes!

Create tenant-specific "changes" collections. Put them all together, but in a separate column family.
Copy change data from main CF to "collection change" CF on each update.

Prune changes from tenant-specific changes DB on every update, and eventually every delete.

Create a lock manager. Lock on collection modifications. Prevent non-serializable behaviors as much
as possible while maintaining weak guarantees in general:
  Acquire locks at master on first API call and drop after commit.
  Read-lock collection and tenant on modification of the collection
  Write-lock collection and tenant on delete of collection (when we support that)
  Read-lock collection entry on any read of a single entry
  Write-lock collection entry on any write
  No writes beyond that!

Support API queries that filter by tenant ID and key.

Read-only slaves.

Local slaves for Node and Java with a local cache.

TLS everywhere.
  Specify CA for trusted connections from server to server.
  Optional CA for API calls.

Add a version number to the whole database!

Test and understand ramifications of current cluster configuration changes.

Come up with a graceful shutdown procedure for the leader.

## Lower Priority

Prune old data from the database.

Queue locally if the leader is not available? Not sure about that one. Maybe as an option.

Add API option to post without waiting for commit.

Should tenant IDs be hierarchical?
  (for instance, "coke/deployments" versus "pepsi/products"?

Add more tests
  Restart the leader after killing it
  Kill a majority of nodes and verify leader fails
  Restart and verify leader recovers

Write a dump / load / recovery tool

Experiment with heartbeat and election timeouts and make them configurable.

Filter API responses to eliminate duplicate changes by key.

Garbage-collect old data by key.

## Discovery Proposal

Each node has a unique ID. (This may be stored in a separate file so that the RocksDB data is theoretically
the same for every node.) The node generates the unique ID on startup.

Each node contains an internal collection that lists all the nodes in the cluster. The collection entry
includes the node's address, ID, and possibly some state information (such as whether it is caught up 
and part of the cluster, or still synchronizing). This collection is replicated just like everything else.

Bootstrapping procedure:

First node: It just starts up and listens on an IP address. It is a single-node cluster.

Subsequent nodes: They need the IP address of another cluster member ("bootstrapping address"). Node calls
an API and proposes a change. Upon election, node is now running.

Leaving a node: There is an API call for that. It also proposes a change. Doesn't happen with no leader.

Raft protocols: Raft has a much more complex protocol. Would be more robust to nodes leaving and entering 
very quickly even with no leader available. 

### "Raw" deployment

When registering, each node must advertise an IP address that is reachable from all the other nodes. 
The node must try to figure that out on its own. If it can't, then we need an option that will allow an 
administrator to set it. (This would be necessarily in a naked Docker deployment, in which both "real" IP
and real port are not visible to the container.

### K8S deployment

In this mode, we assume that the entire cluster will run inside the same K8s cluster. (If that is not the case
then we need use the "raw" method.

When a node boots in this mode, it will use the K8s API to discover its bootstrap node, and it will then
make an API call to that node to register itself. (That means that the registration API must be idempotent
as long as nodes persist their own unique ID.)

Nodes must still use the collection of "peer" nodes that came from the leader, because otherwise we run the risk
of electing multiple readers.

However, again with K8s each node does not know its "real" IP and port. Instead, each node must use the K8s 
management APIs to discover the real IP and port to use to communicate with each node.

In this case, we will need to add some metadata to the K8s API so that each node puts its unique ID in a place
where the other nodes can find it.
