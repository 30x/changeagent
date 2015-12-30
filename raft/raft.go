package raft

type StateMachine interface {
  ApplyEntry(index uint64, data []byte) error
  GetLastIndex() (uint64, error)
}
