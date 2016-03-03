package raft

import (
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

type StateMachine interface {
  Commit(entry *storage.Entry) error
}
