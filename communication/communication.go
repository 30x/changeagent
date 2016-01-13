package communication

import (
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

type Raft interface {
  MyId() uint64
  RequestVote(req *VoteRequest) (*VoteResponse, error)
  Append(req *AppendRequest) (*AppendResponse, error)
  Propose(data []byte) (uint64, error)
}

type VoteRequest struct {
  Term uint64
  CandidateId uint64
  LastLogIndex uint64
  LastLogTerm uint64
}

type VoteResponse struct {
  NodeId uint64
  Term uint64
  VoteGranted bool
  Error error
}

type AppendRequest struct {
  Term uint64
  LeaderId uint64
  PrevLogIndex uint64
  PrevLogTerm uint64
  LeaderCommit uint64
  Entries []storage.Entry
}

type AppendResponse struct {
  Term uint64
  Success bool
  CommitIndex uint64
  Error error
}

type ProposalResponse struct {
  NewIndex uint64
  Error error
}

type Communication interface {
  SetRaft(raft Raft)
  RequestVote(id uint64, req *VoteRequest, ch chan *VoteResponse)
  Append(id uint64, req *AppendRequest) (*AppendResponse, error)
  Propose(id uint64, data []byte) (*ProposalResponse, error)
}
