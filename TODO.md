# TODO

## High Priority

Forward requests from non-leaders to the leader. Return 503 if we are not a follower.

Add tenant ID and key to each entry.

Add timestamp to each entry.

Support API queries that filter by tenant ID and key.

Support discovery of new nodes.
  Simplest proposal: Just wait for a notification from ZK and start to replicate.
  Store discovery locally in case there is an issue and we can't get it at startup time.
  Followers:
    Change in discovery will only take effect when we become a candidate
  Leaders:
    Changes have to be pushed to leader so it can start new peers

Support removal of existing nodes.
  Followers:
    Nothing to do. If it was the leader then we will elect a new one.
      Make sure that node list is consistent every time we start election.
  Leader:
    If we're the leader and we stop, well does it matter?
    Only if there are un-committed changes!

TLS everywhere.
  Specify CA for trusted connections from server to server.
  Optional CA for API calls.

Add version numbers to all keys and values in the database.

Add a version number to the whole database!

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
