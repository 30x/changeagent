/*
 * This file represents the communication from the raft leader to all its
 * peers. We maintain one goroutine per peer, which we use to send
 * all heartbeats and appends, and to keep track of peer state.
 */

package raft

import (
  "time"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

type rpcResponse struct {
  success bool
  heartbeat bool
  newIndex uint64
  err error
}

type raftPeer struct {
  id uint64
  r *RaftImpl
  proposals chan uint64
  changeChan chan<- peerMatchResult
  stopChan chan bool
}

func startPeer(id uint64, r *RaftImpl, changes chan<- peerMatchResult) *raftPeer {
  p := &raftPeer{
    id: id,
    r: r,
    proposals: make(chan uint64, 1000),
    changeChan: changes,
    stopChan: make(chan bool, 1),
  }
  go p.peerLoop()
  return p
}

func (p *raftPeer) stop() {
  p.stopChan <- true
}

func (p *raftPeer) propose(ix uint64) {
  p.proposals <- ix
}

func (p *raftPeer) peerLoop() {
  // Next index that we know that we need to send to this peer
  nextIndex, _ := p.r.GetLastIndex()
  // The index, from storage, that we expect to be sending
  desiredIndex := nextIndex

  // Send HTTP requests asynchronously and notify this channel when they complete
  responseChan := make(chan rpcResponse, 1)
  rpcRunning := false

  // Repaat heartbeats during idle periods to prevent
  // election timeouts (§5.2)
  timeout := time.NewTimer(HeartbeatTimeout)

  for {
    if !rpcRunning && (desiredIndex > nextIndex) {
      err := p.sendUpdates(desiredIndex, nextIndex, responseChan)
      if err != nil {
        log.Debugf("Error sending updates to peer: %s", err)
      }
      rpcRunning = true
      timeout.Reset(HeartbeatTimeout)
    }

    select {
    case <- timeout.C:
      if !rpcRunning && (nextIndex == desiredIndex) {
        // Special handling for heartbeats so that a new leader is not elected
        p.sendHeartbeat(desiredIndex, responseChan)
        rpcRunning = true
      }
      timeout.Reset(HeartbeatTimeout)

    case newIndex := <- p.proposals:
      // These are pushed to this routine from the main raft impl
      desiredIndex = newIndex

    case response := <- responseChan:
      rpcRunning = false
      nextIndex = p.handleRpcResult(response)

    case <- p.stopChan:
      log.Debugf("Peer %d stopping", p.id)
      return
    }
  }
}

func (p *raftPeer) handleRpcResult(resp rpcResponse) uint64 {
  if resp.success && !resp.heartbeat {
    // If successful: update nextIndex and matchIndex for
    // follower (§5.3)
    newIndex := resp.newIndex
    log.Debugf("Client %d now up to date with index %d", p.id, resp.newIndex)
    change := peerMatchResult{
      id: p.id,
      newMatch: newIndex,
    }
    p.changeChan <- change
    return newIndex

  } else if !resp.heartbeat {
    // If AppendEntries fails because of log inconsistency:
    // decrement nextIndex and retry (§5.3)
    newNext := resp.newIndex
    if newNext > 0 {
      newNext--
    }
    return newNext

  } else {
    return resp.newIndex
  }
}

func (p *raftPeer) sendHeartbeat(index uint64, rc chan rpcResponse) {
  lastIndex, lastTerm := p.r.GetLastIndex()
  ar := &communication.AppendRequest{
    Term: p.r.GetCurrentTerm(),
    LeaderId: p.r.id,
    PrevLogIndex: lastIndex,
    PrevLogTerm: lastTerm,
    LeaderCommit: p.r.GetCommitIndex(),
  }

  go func() {
    p.r.sendAppend(p.id, ar)
    rpcResp := rpcResponse{
      success: true,
      newIndex: index,
      heartbeat: true,
    }
    rc <- rpcResp
  }()
}

func (p *raftPeer) sendUpdates(desired uint64, next uint64, rc chan rpcResponse) error {
  // If last log index ≥ nextIndex for a follower: send AppendEntries RPC
  // with log entries starting at nextIndex
  // TODO Why not only get stuff since "next" and then slice it to avoid two queries?
  entries, err := p.r.stor.GetEntries(next, desired)
  if err != nil {
    log.Debugf("Error getting entries for peer %d: %v", p.id, err)
    return err
  }

  log.Debugf("Sending %d entries between %d and %d to %d",
    len(entries), next, desired, p.id)

  lastIndex := next - 1
  lastTerm, _, err := p.r.stor.GetEntry(lastIndex)
  if err != nil {
    log.Debugf("Error getting entries for peer %d: %v", p.id, err)
    return err
  }

  ar := &communication.AppendRequest{
    Term: p.r.GetCurrentTerm(),
    LeaderId: p.r.id,
    PrevLogIndex: lastIndex,
    PrevLogTerm: lastTerm,
    LeaderCommit: p.r.GetCommitIndex(),
    Entries: entries,
  }

  go func() {
    success, err := p.r.sendAppend(p.id, ar)
    newIndex := next
    if success { newIndex = desired }
    rpcResp := rpcResponse{
      success: success,
      newIndex: newIndex,
      err: err,
    }
    rc <- rpcResp
  }()

  return nil
}
