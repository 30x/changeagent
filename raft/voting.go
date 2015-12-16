package raft

import (
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

func (r *RaftImpl) handleFollowerVote(state *raftState, cmd voteCommand) {
  log.Debugf("Node %d got vote request from %d at term %d",
    r.id, cmd.vr.CandidateId, cmd.vr.Term)
  resp := communication.VoteResponse{
    NodeId: r.id,
    Term: state.currentTerm,
  }

  // 5.1: Reply false if term < currentTerm
  if cmd.vr.Term < state.currentTerm {
    resp.VoteGranted = false
    cmd.rc <- &resp
    return
  }

  // 5.2, 5.2: If votedFor is null or candidateId, and candidate’s log is at
  // least as up-to-date as receiver’s log, grant vote
  if (state.votedFor == 0 || state.votedFor == cmd.vr.CandidateId) &&
     cmd.vr.LastLogIndex >= state.commitIndex {
     err := r.stor.SetMetadata(VotedForKey, cmd.vr.CandidateId)
     if err != nil {
       resp.Error = err
       cmd.rc <- &resp
       return
     }

     state.votedFor = cmd.vr.CandidateId
     r.writeLastVote(cmd.vr.CandidateId)
     log.Debugf("Node %d voting for candidate %d", r.id, cmd.vr.CandidateId)
     resp.VoteGranted = true
   } else {
     resp.VoteGranted = false
   }
  cmd.rc <- &resp
}

func (r *RaftImpl) voteNo(state *raftState, cmd voteCommand) {
  resp := communication.VoteResponse{
    NodeId: r.id,
    Term: state.currentTerm,
    VoteGranted: false,
  }
  cmd.rc <- &resp
}

func (r *RaftImpl) sendVotes(state *raftState, index uint64, rc chan<- voteResult) {
  lastIndex, lastTerm, err := r.stor.GetLastIndex()
  if err != nil {
    log.Infof("Error reading database to start election: %v", err)
    dbErr := voteResult{
      index: index,
      err: err,
    }
    rc <- dbErr
    return
  }

  vr := communication.VoteRequest{
    Term: state.currentTerm,
    CandidateId: r.id,
    LastLogIndex: lastIndex,
    LastLogTerm: lastTerm,
  }

  nodes := r.disco.GetNodes()
  votes := 0
  log.Debugf("Node %d sending vote request to %d nodes for term %d",
    r.id, len(nodes), state.currentTerm)

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

  finalResponse := voteResult{
    index: index,
    result: granted,
  }
  rc <- finalResponse
}
