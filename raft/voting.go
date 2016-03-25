/*
 * Methods in this file handle the voting logic.
 */

package raft

import (
  "github.com/golang/glog"
  "revision.aeip.apigee.net/greg/changeagent/communication"
)

func (r *RaftImpl) handleFollowerVote(state *raftState, cmd voteCommand) bool {
  glog.V(2).Infof("Node %d got vote request from %d at term %d",
    r.id, cmd.vr.CandidateId, cmd.vr.Term)
  currentTerm := r.GetCurrentTerm()

  resp := communication.VoteResponse{
    NodeId: r.id,
    Term: currentTerm,
  }

  // 5.1: Reply false if term < currentTerm
  if cmd.vr.Term < currentTerm {
    resp.VoteGranted = false
    cmd.rc <- &resp
    return false
  }

  // Important to double-check state at this point as well since channels are buffered
  if r.GetState() != Follower {
    resp.VoteGranted = false
    cmd.rc <- &resp
    return false
  }

  // 5.2, 5.2: If votedFor is null or candidateId, and candidate’s log is at
  // least as up-to-date as receiver’s log, grant vote
  commitIndex := r.GetCommitIndex()
  if (state.votedFor == 0 || state.votedFor == cmd.vr.CandidateId) &&
     cmd.vr.LastLogIndex >= commitIndex {
     state.votedFor = cmd.vr.CandidateId
     r.writeLastVote(cmd.vr.CandidateId)
     glog.V(2).Infof("Node %d voting for candidate %d", r.id, cmd.vr.CandidateId)
     resp.VoteGranted = true
   } else {
     resp.VoteGranted = false
   }
  cmd.rc <- &resp
  return resp.VoteGranted
}

func (r *RaftImpl) voteNo(state *raftState, cmd voteCommand) {
  resp := communication.VoteResponse{
    NodeId: r.id,
    Term: r.GetCurrentTerm(),
    VoteGranted: false,
  }
  cmd.rc <- &resp
}

func (r *RaftImpl) sendVotes(state *raftState, index uint64, rc chan<- voteResult) {
  lastIndex, lastTerm, err := r.stor.GetLastIndex()
  if err != nil {
    glog.Infof("Error reading database to start election: %v", err)
    dbErr := voteResult{
      index: index,
      err: err,
    }
    rc <- dbErr
    return
  }

  currentTerm := r.GetCurrentTerm()
  vr := communication.VoteRequest{
    Term: currentTerm,
    CandidateId: r.id,
    LastLogIndex: lastIndex,
    LastLogTerm: lastTerm,
  }

  nodes := r.disco.GetNodes()
  votes := 0
  glog.V(2).Infof("Node %d sending vote request to %d nodes for term %d",
    r.id, len(nodes), currentTerm)

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
      glog.V(2).Infof("Error receiving vote: from %d", vresp.NodeId, vresp.Error)
    } else if vresp.VoteGranted {
      glog.V(2).Infof("Node %d received a yes vote from %d",
        r.id, vresp.NodeId)
      votes++
    } else {
      glog.V(2).Infof("Node %d received a no vote from %d",
        r.id, vresp.NodeId)
    }
  }

  granted := votes >= ((len(nodes) / 2) + 1)
  glog.Infof("Node %d: election request complete for term %d: %d votes for %d notes. Granted = %v",
    r.id, vr.Term, votes, len(nodes), granted)

  finalResponse := voteResult{
    index: index,
    result: granted,
  }
  rc <- finalResponse
}
