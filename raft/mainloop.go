/*
 * This file contains the main loops for leaders and followers. This loop
 * runs in a single goroutine per Raft.
 */

package raft

import (
  "time"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

type voteResult struct {
  index uint64
  result bool
  err error
}

type raftState struct {
  votedFor uint64
  voteIndex uint64              // Keep track of the voting channel in case something takes a long time
  voteResults chan voteResult
  peers map[uint64]*raftPeer
}

func (r *RaftImpl) mainLoop() {
  state := &raftState{
    voteIndex: 0,
    voteResults: make(chan voteResult),
    votedFor: r.readLastVote(),
    peers: make(map[uint64]*raftPeer),
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
      r.handleFollowerVote(state, voteCmd)

    case appendCmd := <- r.appendCommands:
      // 5.1: If RPC request or response contains term T > currentTerm:
      // set currentTerm = T, convert to follower
      if appendCmd.ar.Term > r.GetCurrentTerm() {
        r.setCurrentTerm(appendCmd.ar.Term)
        state.votedFor = 0
        r.writeLastVote(0)
        r.setState(StateFollower)
      }
      r.handleAppend(state, appendCmd)
      timeout.Reset(r.randomElectionTimeout())

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
  for _, n := range(r.disco.GetNodes()) {
    state.peers[n.Id] = startPeer(n.Id, r)
  }

  for {
    select {
    case voteCmd := <- r.voteCommands:
      r.voteNo(state, voteCmd)

    case appendCmd := <- r.appendCommands:
      // 5.1: If RPC request or response contains term T > currentTerm:
      // set currentTerm = T, convert to follower
      if appendCmd.ar.Term > r.GetCurrentTerm() {
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
