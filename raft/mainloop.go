package raft

import (
  "time"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

const (
  ElectionTimeout = 10 * time.Second
  HeartbeatTimeout = 2 * time.Second
)

type voteResult struct {
  index uint64
  result bool
  err error
}

type raftState struct {
  currentTerm uint64
  votedFor uint64
  commitIndex uint64
  lastApplied uint64
  voteIndex uint64              // Keep track of the voting channel in case something takes a long time
  voteResults chan voteResult
}

func (r *RaftImpl) mainLoop() {
  state := &raftState{
    voteIndex: 0,
    voteResults: make(chan voteResult),
    currentTerm: r.readCurrentTerm(),
    votedFor: r.readLastVote(),
    commitIndex: r.readLastCommit(),
    lastApplied: r.readLastApplied(),
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
  // TODO election timeout must be randomized

  if isCandidate {
    log.Debugf("Node %d starting an election", r.id)
    state.voteIndex++
    // Update term and vote for myself
    state.currentTerm++
    r.writeCurrentTerm(state.currentTerm)
    state.votedFor = r.id
    r.writeLastVote(r.id)
    go r.sendVotes(state, state.voteIndex, state.voteResults)
  }

  timeout := time.NewTimer(ElectionTimeout)
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
      if appendCmd.ar.Term > state.currentTerm {
        state.currentTerm = appendCmd.ar.Term
        r.writeCurrentTerm(state.currentTerm)
        state.votedFor = 0
        r.writeLastVote(0)
        r.setState(StateFollower)
      }
      r.handleFollowerAppend(state, appendCmd)
      timeout.Reset(ElectionTimeout)

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
        timeout.Reset(ElectionTimeout)
      }

    case stopDone := <- r.stopChan:
      r.setState(StateStopping)
      return stopDone
    }
  }
}

func (r *RaftImpl) leaderLoop(state *raftState) chan bool {
  // TODO if we get appendEntries from a different term, stop being leader
  r.sendEntries(state, nil)

  timeout := time.NewTimer(HeartbeatTimeout)
  for {
    select {
    case <- timeout.C:
      r.sendEntries(state, nil)
      timeout.Reset(HeartbeatTimeout)

    case voteCmd := <- r.voteCommands:
      r.voteNo(state, voteCmd)

    case <- r.appendCommands:
      // Nothing to do, at least for now!
      continue

    case stopDone := <- r.stopChan:
      r.setState(StateStopping)
      return stopDone
    }
  }
}
