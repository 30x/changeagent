package raft

import "github.com/30x/changeagent/common"

/*
A StateMachine is an interface that is notified whenever a new change is committed
in the raft log. (A commit only happens when a quorum of nodes have accepted a new
proposal, and the leader decides to increment the commit sequence.)
Users of this module may implement this interface so that they can take action
when a change is committed. For instance, they can update a database.
*/
type StateMachine interface {
	Commit(entry *common.Entry) error
}
