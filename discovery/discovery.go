package discovery

/*
 * This module contains a set of discovery services that make it easier to
 * learn which servers are configured as part of a cluster, and make it easier
 * to know when that configuration changes.
 */

const (
  StateNew = iota
  StateCatchup = iota
  StateMember = iota
  StateDeleting = iota

  NewNode = iota
  DeletedNode = iota
  UpdatedNode = iota
)

type Node struct {
  Id uint64
  Address string
  State int
}

type Change struct {
  Action int
  Node *Node
}

type Discovery interface {
  /*
   * Get a list of nodes from this discovery service. Safe to be called from
   * many threads.
   */
  GetNodes() []Node

  /*
   * Shortcut to get just the "address" field from a single node identified
   * by its node ID. Also safe to be called from many threads.
   */
  GetAddress(id uint64) string

  /*
   * Return a channel that will be notified via a Change object every time
   * the list of nodes is changed.
   */
  Watch() <-chan Change

  /*
   * Stop any resources created by the service.
   */
  Close()
}
