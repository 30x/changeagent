package raft

type StateMachine interface {
  Commit(index uint64) error
}
