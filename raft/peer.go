/*
 * This file represents the communication from the raft leader to all its
 * peers. We maintain one goroutine per peer, which we use to send
 * all heartbeats and appends, and to keep track of peer state.
 */

package raft

import (
  "time"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

type raftPeer struct {
  id uint64
  r *RaftImpl
  stopChan chan bool
}

func startPeer(id uint64, r *RaftImpl) *raftPeer {
  p := &raftPeer{
    id: id,
    r: r,
    stopChan: make(chan bool),
  }
  go p.peerLoop()
  return p
}

func (p *raftPeer) stop() {
  p.stopChan <- true
}

func (p *raftPeer) peerLoop() {
  p.r.sendAppend(p.id, nil)

  timeout := time.NewTimer(HeartbeatTimeout)

  for {
    select {
    case <- timeout.C:
      p.r.sendAppend(p.id, nil)
      timeout.Reset(HeartbeatTimeout)
    case <- p.stopChan:
      log.Debugf("Peer %d stopping", p.id)
      return
    }
  }
}
