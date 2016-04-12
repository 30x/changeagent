/*
 * This file contains the main loops for leaders and followers. This loop
 * runs in a single goroutine per Raft.
 */

package raft

import (
  "errors"
  "time"
  "github.com/golang/glog"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
)

type voteResult struct {
  index uint64
  result bool
  err error
}

type peerMatchResult struct {
  id uint64
  newMatch uint64
}

type raftState struct {
  votedFor uint64
  voteIndex uint64              // Keep track of the voting channel in case something takes a long time
  voteResults chan voteResult
  peers map[uint64]*raftPeer
  peerMatches map[uint64]uint64
  peerMatchChanges chan peerMatchResult
}

func (r *RaftImpl) mainLoop() {
  state := &raftState{
    voteIndex: 0,
    voteResults: make(chan voteResult, 1),
    votedFor: r.readLastVote(),
    peers: make(map[uint64]*raftPeer),
    peerMatches: make(map[uint64]uint64),
    peerMatchChanges: make(chan peerMatchResult, 1),
  }

  var stopDone chan bool
  for {
    switch r.GetState() {
    case Follower:
      glog.Infof("Node %d entering follower mode", r.id)
      stopDone = r.followerLoop(false, state)
    case Candidate:
      glog.Infof("Node %d entering candidate mode", r.id)
      stopDone = r.followerLoop(true, state)
    case Leader:
      glog.Infof("Node %d entering leader mode", r.id)
      stopDone = r.leaderLoop(state)
    case Stopping:
      r.cleanup()
      if stopDone != nil {
        stopDone <- true
      }
      glog.V(2).Infof("Node %d stop is complete", r.id)
      return
    case Stopped:
      return
    }
  }
}

func (r *RaftImpl) followerLoop(isCandidate bool, state *raftState) chan bool {
  if isCandidate {
    glog.V(2).Infof("Node %d starting an election", r.id)
    state.voteIndex++
    // Update term and vote for myself
    r.setCurrentTerm(r.GetCurrentTerm() + 1)
    state.votedFor = r.id
    r.writeLastVote(r.id)
    go r.sendVotes(state, state.voteIndex, state.voteResults)
  }

  timeout := time.NewTimer(r.randomElectionTimeout())
  for {
    select {
    case <- timeout.C:
      glog.V(2).Infof("Node %d: election timeout", r.id)
      if !r.followerOnly {
        r.setState(Candidate)
        r.setLeaderId(0)
        return nil
      }

    case voteCmd := <- r.voteCommands:
      granted := r.handleFollowerVote(state, voteCmd)
      if granted {
        // After voting yes, wait for a timeout until voting again
        timeout.Reset(r.randomElectionTimeout())
      }

    case appendCmd := <- r.appendCommands:
      // 5.1: If RPC request or response contains term T > currentTerm:
      // set currentTerm = T, convert to follower
      glog.V(2).Infof("Processing append command from leader %d", appendCmd.ar.LeaderId)
      if appendCmd.ar.Term > r.GetCurrentTerm() {
        glog.Infof("Append request from new leader at new term %d", appendCmd.ar.Term)
        r.setCurrentTerm(appendCmd.ar.Term)
        state.votedFor = 0
        r.writeLastVote(0)
        r.setState(Follower)
        r.setLeaderId(appendCmd.ar.LeaderId)
      } else if r.GetLeaderId() == 0 {
        glog.Infof("Seeing new leader %d for the first time", appendCmd.ar.LeaderId)
        r.setLeaderId(appendCmd.ar.LeaderId)
      }
      r.handleAppend(state, appendCmd)
      timeout.Reset(r.randomElectionTimeout())

    case prop := <- r.proposals:
      leaderId := r.GetLeaderId()
      if leaderId == 0 {
        pr := proposalResult{
          err: errors.New("Cannot accept proposal because there is no leader"),
        }
        prop.rc <- pr
      } else {
        go func() {
          glog.V(2).Infof("Forwarding proposal to leader node %d", leaderId)
          fr, err := r.comm.Propose(leaderId, &prop.entry)
          pr := proposalResult{
            index: fr.NewIndex,
          }
          if err != nil {
            pr.err = err
          } else if fr.Error != nil {
            pr.err = fr.Error
          }
          prop.rc <- pr
        }()
      }

    case vr := <- state.voteResults:
      if vr.index == state.voteIndex {
        // Avoid vote results that come back way too late
        state.votedFor = 0
        r.writeLastVote(0)
        glog.V(2).Infof("Node %d received the election result: %v", r.id, vr.result)
        if vr.result {
          r.setState(Leader)
          r.setLeaderId(0)
          return nil
        }
        // Voting failed. Try again after timeout.
        timeout.Reset(r.randomElectionTimeout())
      }

    case change := <- r.configChanges:
      // Discovery service now up to date, so not much to do
      glog.Infof("Received configuration change type %d for node %d",
        change.Action, change.Node.ID)

    case stopDone := <- r.stopChan:
      r.setState(Stopping)
      return stopDone
    }
  }
}

func (r *RaftImpl) leaderLoop(state *raftState) chan bool {
  // Upon election: send initial empty AppendEntries RPCs (heartbeat) to
  // each server; repeat during idle periods to prevent
  // election timeouts (§5.2)
  //   We will do this inside the "peers" module
  for _, n := range(r.disco.GetNodes()) {
    if n.ID == r.id {
      continue
    }
    state.peers[n.ID] = startPeer(n.ID, r, state.peerMatchChanges)
    state.peerMatches[n.ID] = 0
  }

  // Upon election: send initial empty AppendEntries RPCs (heartbeat) to
  // each server; repeat during idle periods to prevent
  // election timeouts (§5.2)
  _, err := r.makeProposal(nil, state)
  if err != nil {
    // Not sure what else to do, so abort being the leader
    glog.Infof("Error when initially trying to become leader: %s", err)
    state.votedFor = 0
    r.writeLastVote(0)
    r.setState(Follower)
    stopPeers(state)
    return nil
  }

  for {
    select {
    case voteCmd := <- r.voteCommands:
      r.voteNo(state, voteCmd)

    case appendCmd := <- r.appendCommands:
      // 5.1: If RPC request or response contains term T > currentTerm:
      // set currentTerm = T, convert to follower
      if appendCmd.ar.Term > r.GetCurrentTerm() {
        glog.Infof("Append request from new leader at new term %d. No longer leader",
          appendCmd.ar.Term)
        // Potential race condition averted because only this goroutine updates term
        r.setCurrentTerm(appendCmd.ar.Term)
        state.votedFor = 0
        r.writeLastVote(0)
        r.setState(Follower)
        r.setLeaderId(appendCmd.ar.LeaderId)
        stopPeers(state)
        r.handleAppend(state, appendCmd)
        return nil
      }
      r.handleAppend(state, appendCmd)

    case prop := <- r.proposals:
      // If command received from client: append entry to local log,
      // respond after entry applied to state machine (§5.3)
      index, err := r.makeProposal(&prop.entry, state)
      if len(state.peers) == 0 {
        // Special handling for a stand-alone node
        r.setCommitIndex(index)
        r.applyCommittedEntries(index)
      }
      pr := proposalResult{
        index: index,
        err: err,
      }
      prop.rc <- pr

    case peerMatch := <- state.peerMatchChanges:
      // Got back a changed applied index from a peer. Decide if we have a commit and
      // process it if we do.
      state.peerMatches[peerMatch.id] = peerMatch.newMatch
      newIndex := r.calculateCommitIndex(state)
      if r.setCommitIndex(newIndex) {
        r.applyCommittedEntries(newIndex)
        for _, p := range(state.peers) {
          p.poke()
        }
      }

    case change := <- r.configChanges:
      r.handleLeaderConfigChange(state, change)

    case stopDone := <- r.stopChan:
      r.setState(Stopping)
      stopPeers(state)
      return stopDone
    }
  }
}

func stopPeers(state *raftState) {
  for _, p := range(state.peers) {
    p.stop()
  }
}

func (r *RaftImpl) handleLeaderConfigChange(state *raftState, change discovery.Change) {
  id := change.Node.ID
  switch change.Action {
  case discovery.NewNode:
    glog.Infof("Adding new node %d", id)
    state.peers[id] = startPeer(id, r, state.peerMatchChanges)
    state.peerMatches[id] = 0

  case discovery.DeletedNode:
    glog.Infof("Stopping communications to node %d", id)
    state.peers[id].stop()
    delete(state.peers, id)
    delete(state.peerMatches, id)

  case discovery.UpdatedNode:
    glog.Infof("New address for node %d: %s", id, change.Node.Address)
  }
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
func (r *RaftImpl) calculateCommitIndex(state *raftState) uint64 {
  // Start with the max possible index and work our way down to the min
  var max uint64 = 0
  for _, mi := range(state.peerMatches) {
    if mi > max {
      max = mi
    }
  }

  // Test each term to see if we have consensus
  cur := max
  for ; cur > r.GetCommitIndex(); cur-- {
    if r.canCommit(cur, state) {
      glog.V(2).Infof("Returning new commit index of %d", cur)
      return cur
    }
  }
  return r.GetCommitIndex()
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
func (r *RaftImpl) canCommit(ix uint64, state *raftState) bool {
  votes := 0
  for _, m := range(state.peerMatches) {
    if m >= ix {
      votes++
    }
  }

  // Remember that we are also considering our own commit index elsewhere.
  // So look just for a simple majority and not for a quorum
  // (N / 2) rather than (N / 2) + 1
  if votes >= (len(state.peerMatches) / 2) {
    entry, err := r.stor.GetEntry(ix)
    if err != nil {
      glog.V(2).Infof("Error reading entry from log: %v", err)
    } else if entry != nil && entry.Term == r.GetCurrentTerm() {
      return true
    }
  }
  return false
}
