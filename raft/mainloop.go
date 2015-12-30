/*
 * This file contains the main loops for leaders and followers. This loop
 * runs in a single goroutine per Raft.
 */

package raft

import (
  "errors"
  "time"
  "revision.aeip.apigee.net/greg/changeagent/log"
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
    case StateFollower:
      log.Infof("Node %d entering follower mode", r.id)
      stopDone = r.followerLoop(false, state)
    case StateCandidate:
      log.Infof("Node %d entering candidate mode", r.id)
      stopDone = r.followerLoop(true, state)
    case StateLeader:
      log.Infof("Node %d entering leader mode", r.id)
      stopDone = r.leaderLoop(state)
    case StateStopping:
      r.cleanup()
      if stopDone != nil {
        stopDone <- true
      }
      return
    case StateStopped:
      return
    }
  }
}

func (r *RaftImpl) followerLoop(isCandidate bool, state *raftState) chan bool {
  if isCandidate {
    log.Debugf("Node %d starting an election", r.id)
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
      log.Debugf("Node %d: election timeout", r.id)
      if !r.followerOnly {
        r.setState(StateCandidate)
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
      if appendCmd.ar.Term > r.GetCurrentTerm() {
        log.Infof("Append request from new leader at new term %d", appendCmd.ar.Term)
        r.setCurrentTerm(appendCmd.ar.Term)
        state.votedFor = 0
        r.writeLastVote(0)
        r.setState(StateFollower)
      }
      r.handleAppend(state, appendCmd)
      timeout.Reset(r.randomElectionTimeout())

    case prop := <- r.proposals:
      prop.rc <- errors.New("Cannot accept proposal because we are not the leader")

    case vr := <- state.voteResults:
      if vr.index == state.voteIndex {
        // Avoid vote results that come back way too late
        state.votedFor = 0
        r.writeLastVote(0)
        log.Debugf("Node %d received the election result: %v", r.id, vr.result)
        if vr.result {
          r.setState(StateLeader)
          return nil
        }
        // Voting failed. Try again after timeout.
        timeout.Reset(r.randomElectionTimeout())
      }

    case stopDone := <- r.stopChan:
      r.setState(StateStopping)
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
    if n.Id == r.id {
      continue
    }
    state.peers[n.Id] = startPeer(n.Id, r, state.peerMatchChanges)
    state.peerMatches[n.Id] = 0
  }

  // Upon election: send initial empty AppendEntries RPCs (heartbeat) to
  // each server; repeat during idle periods to prevent
  // election timeouts (§5.2)
  err := r.makeProposal(nil, state)
  if err != nil {
    // Not sure what else to do, so abort being the leader
    log.Infof("Error when initially trying to become leader: %s", err)
    state.votedFor = 0
    r.writeLastVote(0)
    r.setState(StateFollower)
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
        log.Infof("Append request from new leader at new term %d. No longer leader",
          appendCmd.ar.Term)
        // Potential race condition averted because only this goroutine updates term
        r.setCurrentTerm(appendCmd.ar.Term)
        state.votedFor = 0
        r.writeLastVote(0)
        r.setState(StateFollower)
        stopPeers(state)
        r.handleAppend(state, appendCmd)
        return nil
      }
      r.handleAppend(state, appendCmd)

    case prop := <- r.proposals:
      // If command received from client: append entry to local log,
      // respond after entry applied to state machine (§5.3)
      err := r.makeProposal(prop.data, state)
      prop.rc <- err

    case peerMatch := <- state.peerMatchChanges:
      state.peerMatches[peerMatch.id] = peerMatch.newMatch
      newIndex := r.calculateCommitIndex(state)
      r.setCommitIndex(newIndex)
      r.applyCommittedEntries(newIndex)

    case stopDone := <- r.stopChan:
      r.setState(StateStopping)
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
      log.Debugf("Returning new commit index of %d", cur)
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
    term, _, err := r.stor.GetEntry(ix)
    if err != nil {
      log.Debugf("Error reading entry from log: %v", err)
    } else if term == r.GetCurrentTerm() {
      return true
    }
  }
  return false
}
