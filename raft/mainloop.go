package raft

import (
  "time"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

const (
  ElectionTimeout = 10 * time.Second
  HeartbeatTimeout = 2 * time.Second
)

func (r *RaftImpl) mainLoop() {
  for r.state != StateStopped {
    switch r.state {
    case StateFollower:
      r.followerLoop()
    case StateCandidate:
      r.candidateLoop()
    case StateLeader:
      r.leaderLoop()
    }
  }
  log.Info("Main loop exiting")
}

func (r *RaftImpl) followerLoop() {
  log.Infof("Node %d entering follower mode", r.id)

  timeout := time.NewTimer(ElectionTimeout)
  for {
    select {
    case <- timeout.C:
      if r.votedFor == 0 {
        r.state = StateCandidate
      }
      return
    case <- r.receivedAppendChan:
      log.Debugf("Node %d received an append", r.id)
      timeout.Reset(ElectionTimeout)
    case <- r.stopChan:
      r.state = StateStopped
      return
    }
  }
}

func (r *RaftImpl) candidateLoop() {
  log.Infof("Node %d entering candidate mode", r.id)

  electionResult := make(chan bool, 1)
  r.startElection(electionResult)

  timeout := time.NewTimer(ElectionTimeout)
  for {
    select {
    case result := <- electionResult:
      if result {
        r.state = StateLeader
        return
      }
    case <- timeout.C:
      // Should we keep trying here?
      r.state = StateFollower
      return
    case <- r.receivedAppendChan:
      log.Debugf("Node %d received an append in candidate mode", r.id)
      r.state = StateFollower
      return
    case <- r.stopChan:
      r.state = StateStopped
      return
    }
  }
}

func (r *RaftImpl) startElection(electionResult chan bool) {
  r.updateCurrentTerm(r.currentTerm + 1, r.id)
  go r.sendVotes(electionResult)
}

func (r *RaftImpl) leaderLoop() {
  log.Infof("Node %d entering leader mode", r.id)

  resultChan := make(chan bool)
  r.sendEntries(nil, resultChan)
  <-resultChan

  timeout := time.NewTimer(HeartbeatTimeout)
  for {
    select {
    case <- timeout.C:
      r.sendEntries(nil, resultChan)
      <-resultChan
      timeout.Reset(HeartbeatTimeout)
    case <- r.stopChan:
      r.state = StateStopped
      return
    }
  }
}

func (r *RaftImpl) sendVotes(resultChan chan<- bool) {
  lastIndex, lastTerm, err := r.stor.GetLastIndex()
  if err != nil {
    log.Infof("Error reading database to start election: %v", err)
    resultChan <- false
    return
  }

  vr := communication.VoteRequest{
    Term: r.currentTerm,
    CandidateId: r.id,
    LastLogIndex: lastIndex,
    LastLogTerm: lastTerm,
  }

  nodes := r.disco.GetNodes()
  votes := 0
  log.Debugf("Node %d sending vote request to %d nodes for term %d",
    r.id, len(nodes), r.currentTerm)

  var responses []chan *communication.VoteResponse

  for _, node := range(nodes) {
    if node.Id == r.id {
      votes++
      continue
    }
    rc := make(chan *communication.VoteResponse)
    responses = append(responses, rc)
    r.comm.RequestVote(node.Id, &vr, rc)
  }

  for _, respChan := range(responses) {
    vresp := <- respChan
    if vresp.Error != nil {
      log.Debugf("Error receiving vote: from %d", vresp.NodeId, vresp.Error)
    } else if vresp.VoteGranted {
      log.Debugf("Node %d received a yes vote from %d",
        r.id, vresp.NodeId)
      votes++
    } else {
      log.Debugf("Node %d received a no vote from %d",
        r.id, vresp.NodeId)
    }
  }

  granted := votes >= ((len(nodes) / 2) + 1)
  log.Infof("Node %d: election request complete for term %d: %d votes for %d notes. Granted = %v",
    r.id, vr.Term, votes, len(nodes), granted)

  resultChan <- granted
}

func (r *RaftImpl) sendEntries(entries []storage.Entry, resultChan chan<- bool) {
  lastIndex, lastTerm, err := r.stor.GetLastIndex()
  if err != nil {
    log.Infof("Error reading database to send new entries: %v", err)
    resultChan <- false
    return
  }

  ar := communication.AppendRequest{
    Term: r.currentTerm,
    LeaderId: r.id,
    PrevLogIndex: lastIndex,
    PrevLogTerm: lastTerm,
    LeaderCommit: r.commitIndex,
    Entries: entries,
  }

  nodes := r.disco.GetNodes()
  log.Debugf("Sending append request to %d nodes for term %d", len(nodes), r.currentTerm)

  var responses []chan *communication.AppendResponse

  for _, node := range(nodes) {
    if node.Id == r.id {
      continue
    }
    rc := make(chan *communication.AppendResponse)
    responses = append(responses, rc)
    r.comm.Append(node.Id, &ar, rc)
  }

  for _, respChan := range(responses) {
    resp := <- respChan
    if resp.Error != nil {
      log.Debugf("Error on append: %v", resp.Error)
    } else {
      log.Debugf("Node %d append success = %v", r.id, resp.Success)
    }
  }

  resultChan <- true
}
