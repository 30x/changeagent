package raft

import (
  "errors"
  "fmt"
  "sync"
  "time"
  "math/rand"
  "github.com/golang/glog"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

const (
  // Make these hard-coded rather than "iota" because they go in a database!
  CurrentTermKey = 1
  VotedForKey = 2
  LocalIdKey = 3
  LastAppliedKey = 4
  ElectionTimeout = 10 * time.Second
  HeartbeatTimeout = 2 * time.Second
)

type RaftState int

const (
  Follower RaftState = iota
  Candidate
  Leader
  Stopping
  Stopped
)

func (r RaftState) String() string {
  switch r {
  case Follower:
    return "Follower"
  case Candidate:
    return "Candidate"
  case Leader:
    return "Leader"
  case Stopping:
    return "Stopping"
  case Stopped:
    return "Stopped"
  default:
    return ""
  }
}

type RaftImpl struct {
  id uint64
  state RaftState
  leaderId uint64
  comm communication.Communication
  disco discovery.Discovery
  configChanges <-chan discovery.Change
  stor storage.Storage
  stopChan chan chan bool
  voteCommands chan voteCommand
  appendCommands chan appendCommand
  proposals chan proposalCommand
  latch sync.Mutex
  followerOnly bool
  currentTerm uint64
  commitIndex uint64
  lastApplied uint64
  lastIndex uint64
  lastTerm uint64
  appliedTracker *ChangeTracker
  stateMachine StateMachine
}

type voteCommand struct {
  vr *communication.VoteRequest
  rc chan *communication.VoteResponse
}

type appendCommand struct {
  ar *communication.AppendRequest
  rc chan *communication.AppendResponse
}

type proposalResult struct {
  index uint64
  err error
}

type proposalCommand struct {
  entry storage.Entry
  rc chan proposalResult
}

var raftRand *rand.Rand = makeRand()

func StartRaft(id uint64,
               comm communication.Communication,
               disco discovery.Discovery,
               stor storage.Storage,
               state StateMachine) (*RaftImpl, error) {
  r := &RaftImpl{
    state: Follower,
    comm: comm,
    disco: disco,
    stor: stor,
    stopChan: make(chan chan bool, 1),
    voteCommands: make(chan voteCommand, 1),
    appendCommands: make(chan appendCommand, 1),
    proposals: make(chan proposalCommand, 1),
    latch: sync.Mutex{},
    followerOnly: false,
    appliedTracker: CreateTracker(),
    stateMachine: state,
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

  r.lastIndex, r.lastTerm, err = r.stor.GetLastIndex()
  if err != nil { return nil, err }

  r.currentTerm = r.readCurrentTerm()
  r.commitIndex = r.readLastCommit()
  r.lastApplied = r.readLastApplied()

  r.configChanges = disco.Watch()

  go r.mainLoop()

  return r, nil
}

func (r *RaftImpl) Close() {
  s := r.GetState()
  if s != Stopped && s != Stopping {
    done := make(chan bool)
    r.stopChan <- done
    <- done
  }
  r.appliedTracker.Close()
}

func (r *RaftImpl) cleanup() {
  for len(r.voteCommands) > 0 {
    glog.V(2).Info("Sending cleanup command for vote request")
    vc := <- r.voteCommands
    vc.rc <- &communication.VoteResponse{
      Error: errors.New("Raft is shutting down"),
    }
  }
  //close(r.voteCommands)

  for len(r.appendCommands) > 0 {
    glog.V(2).Info("Sending cleanup command for append request")
    vc := <- r.appendCommands
    vc.rc <- &communication.AppendResponse{
      Error: errors.New("Raft is shutting down"),
    }
  }
  //close(r.appendCommands)

  //close(r.receivedAppendChan)
}

func (r *RaftImpl) RequestVote(req *communication.VoteRequest) (*communication.VoteResponse, error) {
  if r.GetState() == Stopping || r.GetState() == Stopped {
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
  glog.V(2).Infof("Node %d append request. State is %v", r.id, r.GetState())
  if r.GetState() == Stopping || r.GetState() == Stopped {
    return nil, errors.New("Raft is stopped")
  }

  rc := make(chan *communication.AppendResponse)
  cmd := appendCommand{
    ar: req,
    rc: rc,
  }

  r.appendCommands <- cmd
  resp := <- rc
  return resp, resp.Error
}

func (r *RaftImpl) Propose(e *storage.Entry) (uint64, error) {
  if r.GetState() == Stopping || r.GetState() == Stopped {
    return 0, errors.New("Raft is stopped")
  }

  rc := make(chan proposalResult, 1)
  cmd := proposalCommand{
    entry: *e,
    rc: rc,
  }

  glog.V(2).Infof("Going to propose a value of %d bytes", len(e.Data))
  r.proposals <- cmd

  result := <- rc
  return result.index, result.err
}

func (r *RaftImpl) MyId() uint64 {
  return r.id
}

func (r *RaftImpl) GetState() RaftState {
  r.latch.Lock()
  defer r.latch.Unlock()
  return r.state
}

func (r *RaftImpl) setState(newState RaftState) {
  r.latch.Lock()
  defer r.latch.Unlock()
  glog.V(2).Infof("Node %d: setting state to %d", r.id, newState)
  r.state = newState
}

func (r *RaftImpl) GetLeaderId() uint64 {
  r.latch.Lock()
  defer r.latch.Unlock()
  return r.leaderId
}

func (r *RaftImpl) setLeaderId(newId uint64) {
  r.latch.Lock()
  defer r.latch.Unlock()
  if newId == 0 {
    glog.V(2).Infof("Node %d: No leader present")
  } else {
    glog.V(2).Infof("Node %d: Node %d is now the leader", r.id, newId)
  }
  r.leaderId = newId
}

func (r *RaftImpl) GetCurrentTerm() uint64 {
  r.latch.Lock()
  defer r.latch.Unlock()
  return r.currentTerm
}

func (r *RaftImpl) setCurrentTerm(t uint64) {
  r.latch.Lock()
  defer r.latch.Unlock()
  r.currentTerm = t
  r.writeCurrentTerm(t)
}

func (r *RaftImpl) GetCommitIndex() uint64 {
  r.latch.Lock()
  defer r.latch.Unlock()
  return r.commitIndex
}

// Atomically update the commit index, and return whether it changed
func (r *RaftImpl) setCommitIndex(t uint64) bool {
  r.latch.Lock()
  defer r.latch.Unlock()

  if r.commitIndex == t {
    return false
  }
  r.commitIndex = t
  return true
}

func (r *RaftImpl) GetLastApplied() uint64 {
  r.latch.Lock()
  defer r.latch.Unlock()
  return r.lastApplied
}

func (r *RaftImpl) setLastApplied(t uint64) {
  entry, err := r.stor.GetEntry(t)
  if err != nil {
    glog.Errorf("Error reading entry from change %d for commit: %s", t, err)
    return
  }
  if entry == nil {
    glog.Errorf("Committed entry %d could not be read", t)
    return
  }

  err = r.stateMachine.Commit(entry)
  if err != nil {
    glog.Errorf("Error committing change %d: %s", t, err)
    return
  }

  err = r.stor.SetMetadata(LastAppliedKey, t)
  if err != nil {
    glog.Errorf("Error updating last applied key %d to the database: %s", t, err)
    return
  }

  r.latch.Lock()
  r.lastApplied = t
  r.latch.Unlock()

  r.appliedTracker.Update(nil, t)

  if entry.Tenant != nil {
    r.appliedTracker.Update(entry.Tenant, t)
  }
}

func (r *RaftImpl) GetAppliedTracker() *ChangeTracker {
  return r.appliedTracker
}

func (r *RaftImpl) GetLastIndex() (uint64, uint64) {
  r.latch.Lock()
  defer r.latch.Unlock()
  return r.lastIndex, r.lastTerm
}

func (r *RaftImpl) setLastIndex(ix uint64, term uint64) {
  r.latch.Lock()
  defer r.latch.Unlock()
  r.lastIndex = ix
  r.lastTerm = term
}

// Used only in unit testing. Forces us to never become a leader.
func (r *RaftImpl) setFollowerOnly(f bool) {
  r.followerOnly = f
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
  la, err := r.stor.GetMetadata(LastAppliedKey)
  if err != nil { panic("Fatal error reading state from state machine") }
  return la
}

// Election timeout is the default timeout, plus or minus one heartbeat interval
func (r *RaftImpl) randomElectionTimeout() time.Duration {
  rge := int64(HeartbeatTimeout * 2)
  min := int64(ElectionTimeout - HeartbeatTimeout)
  return time.Duration(raftRand.Int63n(rge) + min)
}

func makeRand() *rand.Rand {
  s := rand.NewSource(time.Now().UnixNano())
  return rand.New(s)
}
