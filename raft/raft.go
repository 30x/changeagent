package raft

type StateMachine interface {
  ApplyEntry(data []byte) error
  GetLastIndex() (uint64, error)
}
