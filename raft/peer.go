/*
 * This file represents the communication from the raft leader to all its
 * peers. We maintain one goroutine per peer, which we use to send
 * all heartbeats and appends, and to keep track of peer state.
 */

package raft

import (
  "time"
  "github.com/golang/glog"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/storage"
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
  // Next index that we know that we need to send to this peer. Start from the end
  // of the database. This may be decremented if the peer is behind.
  nextIndex, _ := p.r.GetLastIndex()
  // The index, that we expect the peer to be caught up to eventually. Starts
  // at nextIndex, but will be incremented when more data needs to be replicated.
  desiredIndex := nextIndex

  // Send HTTP requests asynchronously and notify this channel when they complete
  responseChan := make(chan rpcResponse, 1)
  // Only send one message at a time
  rpcRunning := false

  // Repeat heartbeats during idle periods to prevent
  // election timeouts (§5.2)
  hbTimeout := time.NewTimer(HeartbeatTimeout)

  // Provide a way to delay on failure
  failureDelay := false

  for {
    glog.V(2).Infof("Peer %d: next = %d desired = %d", p.id, nextIndex, desiredIndex)

    if !rpcRunning && !failureDelay && (desiredIndex > nextIndex) {
      err := p.sendUpdates(desiredIndex, nextIndex, responseChan)
      if err != nil {
        glog.V(2).Infof("Error sending updates to peer %d: %s", p.id, err)
      }
      rpcRunning = true
      hbTimeout.Reset(HeartbeatTimeout)
    }

    select {
    case <- hbTimeout.C:
      failureDelay = false
      if !rpcRunning && (nextIndex == desiredIndex) {
        // Timeout is up. Send a heartbeat so that a new leader is not elected.
        p.sendHeartbeat(desiredIndex, responseChan)
        rpcRunning = true
      }
      hbTimeout.Reset(HeartbeatTimeout)

    case newIndex := <- p.proposals:
      // These are pushed to this routine from the main raft impl
      desiredIndex = newIndex

    case response := <- responseChan:
      rpcRunning = false
      if response.err == nil {
        nextIndex = p.handleRpcResult(response)
      } else {
        glog.V(2).Infof("Error from peer: %s", response.err)
        failureDelay = true
      }

    case <- p.stopChan:
      glog.V(2).Infof("Peer %d stopping", p.id)
      return
    }
  }
}

func (p *raftPeer) handleRpcResult(resp rpcResponse) uint64 {
  glog.V(2).Infof("Got RPC result. Success = %v index = %d", resp.success, resp.newIndex)
  if resp.success && !resp.heartbeat {
    // If successful: update nextIndex and matchIndex for
    // follower (§5.3)
    newIndex := resp.newIndex
    glog.V(2).Infof("Client %d now up to date with index %d", p.id, resp.newIndex)

    // Send a notification back to the main loop so it can update its own indices.
    change := peerMatchResult{
      id: p.id,
      newMatch: newIndex,
    }
    p.changeChan <- change
    return newIndex

  } else if !resp.success {
    // If AppendEntries fails because of log inconsistency:
    // decrement nextIndex and retry (§5.3)
    newNext := resp.newIndex
    if newNext > 0 {
      newNext--
      glog.V(2).Infof("Decrementing next index to %d", newNext)
    }
    return newNext

  } else {
    return resp.newIndex
  }
}

/*
 * Send a heartbeat to the peer, which is a set of updates with no entries.
 */
func (p *raftPeer) sendHeartbeat(index uint64, rc chan rpcResponse) {
  lastIndex, lastTerm := p.r.GetLastIndex()
  ar := &communication.AppendRequest{
    Term: p.r.GetCurrentTerm(),
    LeaderId: p.r.id,
    PrevLogIndex: lastIndex,
    PrevLogTerm: lastTerm,
    LeaderCommit: p.r.GetCommitIndex(),
  }

  glog.V(2).Infof("Sending heartbeat for index %d to peer %d", index, p.id)

  go func() {
    success, err := p.r.sendAppend(p.id, ar)
    rpcResp := rpcResponse{
      success: success,
      newIndex: index,
      heartbeat: true,
      err: err,
    }
    rc <- rpcResp
  }()
}

/*
 * Send a set of updates to the peer, starting at "next" and going until "desired."
 * The response will be placed on the "rpcResponse" channel.
 */
func (p *raftPeer) sendUpdates(desired uint64, next uint64, rc chan rpcResponse) error {
  // If last log index ≥ nextIndex for a follower: send AppendEntries RPC
  // with log entries starting at nextIndex
  var start uint64
  if next == 0 {
    start = 0
  } else {
    start = next - 1
  }

  glog.V(2).Infof("sendUpdates: start = %d desired = %d", start, desired)

  // Get all the entries from (start - 1).
  allEntries, err := p.r.stor.GetEntries(start, uint(desired - start),
    func(e *storage.Entry) bool { return true })
  if err != nil {
    glog.V(2).Infof("Error getting entries for peer %d: %v", p.id, err)
    return err
  }
  glog.V(2).Infof("Got %d updates", len(allEntries))

  var lastIndex, lastTerm uint64
  var sendEntries []storage.Entry

  // Unless we are starting from 0, the first entry in the list just tells us what the
  // lastIndex and lastTerm should be. The rest are the entries that we actually
  // want to send.
  if next == 0 {
    lastIndex = 0
    lastTerm = 0
    sendEntries = allEntries
  } else {
    lastIndex = allEntries[0].Index
    lastTerm = allEntries[0].Term
    sendEntries = allEntries[1:]
  }

  glog.V(2).Infof("Sending %d entries between %d and %d to %d",
    len(sendEntries), next, desired, p.id)

  ar := &communication.AppendRequest{
    Term: p.r.GetCurrentTerm(),
    LeaderId: p.r.id,
    PrevLogIndex: lastIndex,
    PrevLogTerm: lastTerm,
    LeaderCommit: p.r.GetCommitIndex(),
    Entries: sendEntries,
  }

  // Do the send in a new goroutine because it will block. The main loop will continue
  // and won't do anything else until the response comes back.
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
