package raft

import (
	"github.com/30x/changeagent/storage"
)

type StateMachine interface {
	Commit(entry *storage.Entry) error
}
