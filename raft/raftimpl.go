package raft

import (
  "fmt"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

const (
  CurrentTermKey = "currentTerm"
  VotedForKey = "votedFor"
  LocalIdKey = "localid"
)

const (
  StateFollower = iota
  StateCandidate = iota
  StateLeader = iota
  StateStopped = iota
)

type RaftImpl struct {
  id uint64
  state int
  comm communication.Communication
  disco discovery.Discovery
  stor storage.Storage
  mach StateMachine
  currentTerm uint64
  votedFor uint64
  commitIndex uint64
  lastApplied uint64
  stopChan chan bool
  receivedAppendChan chan bool
}

func StartRaft(id uint64,
               comm communication.Communication,
               disco discovery.Discovery,
               stor storage.Storage,
               mach StateMachine) (*RaftImpl, error) {
  r := &RaftImpl{
    state: StateFollower,
    comm: comm,
    disco: disco,
    stor: stor,
    mach: mach,
    stopChan: make(chan bool),
    receivedAppendChan: make(chan bool),
  }

  storedId, err := stor.GetMetadata(LocalIdKey)
  if err != nil { return nil, err }
  if storedId == 0 {
    err = stor.SetMetadata(LocalIdKey, id)
    if err != nil { return nil, err }
  } else if id != storedId {
    return nil, fmt.Errorf("ID in data store %d does not match requested value %d",
      storedId, id)
  }
  r.id = id

  if disco.GetAddress(r.id) == "" {
    return nil, fmt.Errorf("Id %d cannot be found in discovery data", r.id)
  }

  ct, err := stor.GetMetadata(CurrentTermKey)
  if err != nil { return nil, err }
  r.currentTerm = ct
  log.Debugf("Starting raft at term %d", ct)

  vf, err := stor.GetMetadata(VotedForKey)
  if err != nil { return nil, err }
  r.votedFor = vf

  mi, _, err := stor.GetLastIndex()
  if err != nil { return nil, err }
  r.commitIndex = mi

  la, err := mach.GetLastIndex()
  if err != nil { return nil, err }
  r.lastApplied = la

  go r.mainLoop()

  return r, nil
}

func (r *RaftImpl) Close() {
  r.stopChan <- true
}

func (r *RaftImpl) MyId() uint64 {
  return r.id
}

func (r *RaftImpl) RequestVote(req *communication.VoteRequest) (*communication.VoteResponse, error) {
  log.Debugf("Got vote request from %d at term %d",
    req.CandidateId, req.Term)
  resp := communication.VoteResponse{
    Term: r.currentTerm,
  }

  // 5.1: Reply false if term < currentTerm
  if req.Term < r.currentTerm {
    resp.VoteGranted = false
    return &resp, nil
  }

  // 5.2, 5.2: If votedFor is null or candidateId, and candidate’s log is at
  // least as up-to-date as receiver’s log, grant vote
  if (r.votedFor == 0 || r.votedFor == req.CandidateId) &&
     req.LastLogIndex >= r.commitIndex {
       err := r.stor.SetMetadata(VotedForKey, req.CandidateId)
       if err != nil { return nil, err }
       r.votedFor = req.CandidateId
       log.Debugf("Node %d voting for candidate %d", r.id, req.CandidateId)
       resp.VoteGranted = true
     } else {
       resp.VoteGranted = false
     }
  return &resp, nil
}

func (r *RaftImpl) Append(req *communication.AppendRequest) (*communication.AppendResponse, error) {
  log.Debugf("Got append request for term %d", req.Term)
  resp := communication.AppendResponse{
    Term: r.currentTerm,
  }

  r.receivedAppendChan <- true

  // 5.1: Reply false if term doesn't match current term
  if req.Term < r.currentTerm {
    resp.Success = false
    return &resp, nil
  }

  // 5.3: Reply false if log doesn’t contain an entry at prevLogIndex
  // whose term matches prevLogTerm
  ourTerm, _, err := r.stor.GetEntry(req.PrevLogIndex)
  if err != nil { return nil, err }
  if ourTerm != req.PrevLogTerm {
    resp.Success = false
    return &resp, nil
  }

  if len(req.Entries) > 0 {
    err = r.appendEntries(req.Entries)
    if err != nil { return nil, err }

    // If leaderCommit > commitIndex, set commitIndex =
    // min(leaderCommit, index of last new entry)
    if req.LeaderCommit > r.commitIndex {
      lastIndex := req.Entries[len(req.Entries) - 1].Index
      if req.LeaderCommit < lastIndex {
        r.commitIndex = req.LeaderCommit
      } else {
        r.commitIndex = lastIndex
      }

      // 5.3: If commitIndex > lastApplied: increment lastApplied,
      // apply log[lastApplied] to state machine
      for _, e := range(req.Entries) {
        if e.Index > r.lastApplied && e.Index < r.commitIndex {
          r.mach.ApplyEntry(e.Data)
          r.lastApplied++
        }
      }
    }
  }

  // 5.1: If RPC request or response contains term T > currentTerm:
  // set currentTerm = T, convert to follower
  if req.Term > r.currentTerm {
    r.updateCurrentTerm(req.Term, 0)
  }

  resp.Term = r.currentTerm
  resp.Success = true
  return &resp, nil
}

func (r *RaftImpl) appendEntries(entries []storage.Entry) error {
  // 5.3: If an existing entry conflicts with a new one (same index
  // but different terms), delete the existing entry and all that
  // follow it
  terms, err := r.stor.GetEntryTerms(entries[0].Index)
  if err != nil { return err }

  for _, e := range(entries) {
    if terms[e.Index] != 0 && terms[e.Index] != e.Term {
      // Yep, that happened. Once we delete we can break out of this here loop too
      err = r.stor.DeleteEntries(e.Index)
      if err != nil { return err }
      // Update list of entries to make sure that we don't overwrite improperly
      terms, err = r.stor.GetEntryTerms(entries[0].Index)
      if err != nil { return err }
      break
    }
  }

  // Append any new entries not already in the log
  for _, e := range(entries) {
    if terms[e.Index] == 0 {
      err = r.stor.AppendEntry(e.Index, e.Term, e.Data)
      if err != nil { return err }
    }
  }
  return nil
}

func (r *RaftImpl) updateCurrentTerm(newTerm uint64, votedFor uint64) {
  log.Debugf("Updating current term to %d", newTerm)
  r.stor.SetMetadata(VotedForKey, votedFor)
  r.votedFor = votedFor
  r.stor.SetMetadata(CurrentTermKey, newTerm)
  r.currentTerm = newTerm
}
