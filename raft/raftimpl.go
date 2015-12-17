package raft

import (
  "errors"
  "fmt"
  "sync"
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
  StateStopping = iota
  StateStopped = iota
)

type RaftImpl struct {
  id uint64
  state int
  comm communication.Communication
  disco discovery.Discovery
  stor storage.Storage
  mach StateMachine
  stopChan chan chan bool
  voteCommands chan voteCommand
  appendCommands chan appendCommand
  latch sync.Mutex
  followerOnly bool
}

type voteCommand struct {
  vr *communication.VoteRequest
  rc chan *communication.VoteResponse
}

type appendCommand struct {
  ar *communication.AppendRequest
  rc chan *communication.AppendResponse
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
    stopChan: make(chan chan bool),
    voteCommands: make(chan voteCommand),
    appendCommands: make(chan appendCommand),
    latch: sync.Mutex{},
    followerOnly: false,
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

  go r.mainLoop()

  return r, nil
}

func (r *RaftImpl) Close() {
  s := r.GetState()
  if s != StateStopped && s != StateStopping {
    done := make(chan bool)
    r.stopChan <- done
    <- done
  }
}

func (r *RaftImpl) cleanup() {
  for len(r.voteCommands) > 0 {
    log.Debug("Sending cleanup command for vote request")
    vc := <- r.voteCommands
    vc.rc <- &communication.VoteResponse{
      Error: errors.New("Raft is shutting down"),
    }
  }
  //close(r.voteCommands)

  for len(r.appendCommands) > 0 {
    log.Debug("Sending cleanup command for append request")
    vc := <- r.appendCommands
    vc.rc <- &communication.AppendResponse{
      Error: errors.New("Raft is shutting down"),
    }
  }
  //close(r.appendCommands)

  //close(r.receivedAppendChan)
}

func (r *RaftImpl) MyId() uint64 {
  return r.id
}

func (r *RaftImpl) GetState() int {
  r.latch.Lock()
  defer r.latch.Unlock()
  return r.state
}

// Used only in unit testing. Forces us to never become a leader.
func (r *RaftImpl) setFollowerOnly(f bool) {
  r.followerOnly = f
}

func (r *RaftImpl) setState(newState int) {
  r.latch.Lock()
  defer r.latch.Unlock()
  log.Debugf("Node %d: setting state to %d", r.id, newState)
  r.state = newState
}

func (r *RaftImpl) RequestVote(req *communication.VoteRequest) (*communication.VoteResponse, error) {
  if r.GetState() == StateStopping || r.GetState() == StateStopped {
    return nil, errors.New("Raft is stopped")
  }

  rc := make(chan *communication.VoteResponse)
  cmd := voteCommand{
    vr: req,
    rc: rc,
  }
  r.voteCommands <- cmd
  vr := <- rc
  return vr, vr.Error
}

func (r *RaftImpl) Append(req *communication.AppendRequest) (*communication.AppendResponse, error) {
  if r.GetState() == StateStopping || r.GetState() == StateStopped {
    return nil, errors.New("Raft is stopped")
  }

  rc := make(chan *communication.AppendResponse)
  cmd := appendCommand{
    ar: req,
    rc: rc,
  }

  log.Debugf("Gonna append. State is %v", r.GetState())
  r.appendCommands <- cmd
  resp := <- rc
  return resp, resp.Error
}

func (r *RaftImpl) readCurrentTerm() uint64 {
  ct, err := r.stor.GetMetadata(CurrentTermKey)
  if err != nil { panic("Fatal error reading state from database") }
  return ct
}

func (r *RaftImpl) writeCurrentTerm(ct uint64) {
  err := r.stor.SetMetadata(CurrentTermKey, ct)
  if err != nil { panic("Fatal error writing state to database") }
}

func (r *RaftImpl) readLastVote() uint64 {
  ct, err := r.stor.GetMetadata(VotedForKey)
  if err != nil { panic("Fatal error reading state from database") }
  return ct
}

func (r *RaftImpl) writeLastVote(ct uint64) {
  err := r.stor.SetMetadata(VotedForKey, ct)
  if err != nil { panic("Fatal error writing state to database") }
}

func (r *RaftImpl) readLastCommit() uint64 {
  mi, _, err := r.stor.GetLastIndex()
  if err != nil { panic("Fatal error reading state from database") }
  return mi
}

func (r *RaftImpl) readLastApplied() uint64 {
  la, err := r.mach.GetLastIndex()
  if err != nil { panic("Fatal error reading state from state machine") }
  return la
}
