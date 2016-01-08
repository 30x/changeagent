# TODO

## High Priority

Forward requests from non-leaders to the leader. Return 503 if we are not a follower.

Add tenant ID and key to each entry.

Add timestamp to each entry.

Support API queries that filter by tenant ID and key.

Support discovery of new nodes.
  Simplest proposal: Just wait for a notification from ZK and start to replicate.

Support removal of existing nodes.
  Simplest proposal: Just kill it ;-)

TLS everywhere.
  Specify CA for trusted connections from server to server.
  Optional CA for API calls.

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
