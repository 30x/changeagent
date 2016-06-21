# TODO

## Bugs

Probably need to be able to query by timestamp because we won't always know
when to start.

Election deadlock is possible, at least with two out of three nodes running.
  Only happened a few times.

Insert performance is inconsistent. It seems like many inserts take as long as the HB interval.

tests for having no servers in the discovery file, then adding them later.

## High Priority

Return something at "end of stream." That way clients know when they need
to ask for a snapshot. Also return a link to the previous chunk if they
are not at the end of the stream.
  Add index information to the Raft diagnostic stuff to make this stuff
    easier as well.

Finish membership change support:
  Test removal of a follower
  Implement removal of a leader (it should stop being leader even if still running)
    What stops it from starting a new election?
    Followers need to vote "no" until the leader timeout has expired, right?
  Test address changes
  Test node catchup with a big database
  Test graceful stop of a node

DNS SRV discovery
  By default, look for _changeagent._tcp.YOUR-NAME-HERE. That will make this
  a standard SRV record. Poll every five, or maybe one, minutes, and assume
  that go will do caching. Lookup support is already in the "net" package.

Add a unique cluster ID
  Prevents configuration screw-ups
  Cluster ID generated first time leader sends a message, as an "init" message.
  Gets persisted on each node
  Sent on each request. Node rejects anything with a bad cluster ID.

Read-only slaves.

TLS everywhere.
  Specify CA for trusted connections from server to server.
  Optional CA for API calls.

Add a version number to the whole database!

Test and understand ramifications of current cluster configuration changes.

Come up with a graceful shutdown procedure for the leader.

Think seriously about rolling back any change that does not happen within the
commit timeout.

## Lower Priority

Add API option to post without waiting for commit.

Write a dump / load / recovery tool

Experiment with heartbeat and election timeouts and make them configurable.

# Archive Below

things below are unformed thoughts and at some point will be deleted.

## Transaction Proposal

What happens when a proposal is not committed within a reasonable period of time? Today we end up with
"at most once" semantics:

1) Leader has been elected, but since then followers went away
1) Proposal written to leader log
1) Proposal is not committed in a timely fashion
1) Time out -- return "failure" to clients
1) Later followers come up
1) Proposal is committed

(This is a worst-case scenario. It's also possible that a new leader was elected and followers rolled back.)

What if instead we used a presumed-abort protocol?

1) Leader has been elected, but since then followers went away
1) Proposal written to leader log
1) Proposal is not committed in a timely fashion
1) Leader writes abort record to log
1) Time out -- return "failure" to clients
1) Later followers come up
1) Followers do not commit transaction
1) Followers see abort record and clean up any locks

But what if this happens?

1) Leader has been elected, but since then followers went away
1) Proposal written to leader log
1) Proposal commits
1) Leader writes commit record to log
1) Time out -- return "failure" to clients
1) Later followers come up
1) Followers see original transaction
1) Followers do not see commit and have to wait for it
1) Followers have to wait forever because new leader is elected or something

How about this for a proposal?

1) All proposals include a transaction ID (also allows, well, transactions)
1) Once first proposal commits, a second "commit" proposal is made
1) Or, if the proposal is not committed in time, an "abort" proposal is made
1) Client return is after commit or abort proposal recorded (and committed in case of commit)
1) Followers record transaction records but do not apply changes
1) Followers apply changes to database only when commit record is received
1) Followers assume all transactions are aborted when a new term starts

The last bit is important to handle the case in which the followers see transaction
records, but never see a commit because a new leader was elected and the commit was never
made part of the committed history.

It is still possible that the commit will be appended to the raft log, and it will still
be delivered to all followers, but that the client will see an error because the commit took
too long.

In this proposal, followers need not worry about locking, since the leader takes care of all
that, and we assume that all changes will be propagated in order. Followers, however, must
save all transaction data and not apply any database changes until the commit happens.
(This means that in the case of a simple database failure, the follower should stop processing,
or even panic.)

However, in order to ensure serializable ordering, the leader should impose a locking hierarchy
on the transactions so that we do not have any ordering issues.

One way for the leader to do this would again be to store information about uncommitted transactions
in memory (but with a write lock on the data) and apply them to the database on commit. If the write
lock does not block a read lock, then readers can still read old, committed data until the commit
happens.

Possible locking of key "X":

1) Leader takes out intention write lock on X
1) Any new writers at this point will be blocked
1) Leader records new value in raft log (there already) and in in-memory transaction table
1) Any readers at this point still read the old version of X
1) On commit, leader upgrades lock on X to write lock (to block both reads and writes)
1) Leader reads transaction table and reads each new value from the raft log and updates database
1) Leader drops all locks associated with transaction

What about deadlock detection? Not sure what to do there.

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

###

Other ideas from Shankar and Martin:

Checkpoints. With change #.

Another model:
	Changes and tags, with tag filtering.

Another model for tenants:
	Just entries and tags. Let me filter by tags. That's it.
	Each entry can have multiple tags.

Possibility:
	Delete all collections
	Delete all tenants?
	Add tags.
	Optimize tag implementation.

## How to manage checkpoints

Leave this entirely to a separate tier of "checkpointer" servers.

Each checkpointer simply reads the change log like any other consumer. It is free to run at its own
pace, block, or whatever, without affecting the main message path. So far, so good.

Each checkpointer watches changes, and at a certain point it goes back and re-reads the log
to construct a checkpoint, and then stores the checkpoint in a checkpoint store.

Checkpoints are idempotent, so it doesn't matter if we have more than one checkpointer running
on a particular set of changes at once. However, that will be inefficient.

Instead, each checkpointer should write a "start of checkpoint" record when it starts the checkpoint
process, and an "end of checkpoint" when done. Other checkpointers will use that to avoid starting
a duplicate checkpoint. However, if the checkpointer takes too long, then they may step in anyway.
End of checkpoint records will also tell clients that a checkpoint is available and that they
should use it.

Furthermore, each checkpointer should use the tagging mechanism to entire multitenancy.

What happens if the checkpointers fall too far behind and changes are truncated from the
main database? Should checkpointers register somehow? Or do we just manage that
operationally?

How do we locate checkpoints? Do we end up with a separate checkpoint of checkpoints?
A checkpoint database built in to each server? Might make things much more efficient if we do.

## How to manage data integrity

We have no 2PC, and 2PC doesn't really work anyway. Two approaches for data integrity.

First: maintain a system of record (RDBMS, C*). All requests to change data go to an API over
the SOR DB. API then follows this process:

* Start transaction
* Change data -- at this point regular integrity mechanisms will kick in to place
* Write to change agent
* Commit

In an RDBMS, this will provide serializable consistency.

This may not always be possible, and furthermore we don't want to necessarily use an RDBMS.

Instead:

* Change goes directly to changeagent
* Changeagent maintains a collection of "valdator" URLs
* CA leader invokes each validator, in parallel, with new proposed change
* Any validator may reject the change
* Use tags to make multitenant
