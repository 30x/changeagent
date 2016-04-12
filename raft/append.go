/*
 * Methods in this file handle the append logic for a raft instance.
 */

package raft

import (
  "github.com/golang/glog"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

func (r *Service) handleAppend(state *raftState, cmd appendCommand) {
  glog.V(2).Infof("Got append request for term %d. prevIndex = %d prevTerm = %d leader = %d",
    cmd.ar.Term, cmd.ar.PrevLogIndex, cmd.ar.PrevLogTerm, cmd.ar.LeaderID)
  currentTerm := r.GetCurrentTerm()
  commitIndex := r.GetCommitIndex()

  resp := communication.AppendResponse{
    Term: currentTerm,
  }

  // 5.1: Reply false if term doesn't match current term
  if cmd.ar.Term < r.currentTerm {
    glog.V(2).Infof("Term does not match current term %d", r.currentTerm)
    resp.Success = false
    cmd.rc <- &resp
    return
  }

  // 5.3: Reply false if log doesn’t contain an entry at prevLogIndex
  // whose term matches prevLogTerm
  // but of course we have to be able to start from zero!
  if cmd.ar.PrevLogIndex > 0 {
    ourEntry, err := r.stor.GetEntry(cmd.ar.PrevLogIndex)
    if err != nil {
      resp.Error = err
      cmd.rc <- &resp
      return
    }
    ourTerm := uint64(0)
    if ourEntry != nil {
      ourTerm = ourEntry.Term
    }
    if ourTerm != cmd.ar.PrevLogTerm {
      glog.V(2).Infof("Term %d at index %d does not match %d in request",
        ourTerm, cmd.ar.PrevLogIndex, cmd.ar.PrevLogTerm)
      resp.Success = false
      cmd.rc <- &resp
      return
    }
  }

  if len(cmd.ar.Entries) > 0 {
    err := r.appendEntries(cmd.ar.Entries)
    if err != nil {
      resp.Error = err
      cmd.rc <- &resp
      return
    }
  }

  // If leaderCommit > commitIndex, set commitIndex =
  // min(leaderCommit, index of last new entry)

  glog.V(2).Infof("leader commit: %d commitIndex: %d",
    cmd.ar.LeaderCommit, commitIndex)
  if cmd.ar.LeaderCommit > commitIndex {
    lastIndex, _, err := r.stor.GetLastIndex()
    if err != nil {
      resp.Error = err
      cmd.rc <- &resp
      return
    }

    if cmd.ar.LeaderCommit < lastIndex {
      commitIndex = cmd.ar.LeaderCommit
    } else {
      commitIndex = lastIndex
    }
    r.setCommitIndex(commitIndex)
    glog.V(2).Infof("Node %d: Commit index now %d", r.id, commitIndex)

    err = r.applyCommittedEntries(commitIndex)
    if err != nil {
      resp.Error = err
      cmd.rc <- &resp
      return
    }
  }

  resp.Term = currentTerm
  resp.Success = true
  resp.CommitIndex = commitIndex

  cmd.rc <- &resp
}

func (r *Service) applyCommittedEntries(commitIndex uint64) error {
  // 5.3: If commitIndex > lastApplied: increment lastApplied,
  // apply log[lastApplied] to state machine.
  // In our implementation, we just move a pointer.
  r.setLastApplied(commitIndex)
  glog.V(2).Infof("Node %d: Last applied now %d", r.id, commitIndex)
  return nil
}

func (r *Service) sendAppend(id uint64, ar *communication.AppendRequest) (bool, error) {
  glog.V(2).Infof("Sending append request to node %d for term %d", id, ar.Term)

  resp, err := r.comm.Append(id, ar)
  if err == nil {
    return resp.Success, nil
  }
  return false, err
}

func (r *Service) appendEntries(entries []storage.Entry) error {
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
      err = r.stor.AppendEntry(&e)
      if err != nil { return err }
    }
  }

  // Update cached state so we don't have to read every time we send a heartbeat
  if len(entries) > 0 {
    last := entries[len(entries) - 1]
    r.setLastIndex(last.Index, last.Term)
  }

  return nil
}

func (r *Service) makeProposal(newEntry *storage.Entry, state *raftState) (uint64, error) {
    // If command received from client: append entry to local log,
    // respond after entry applied to state machine (§5.3)
    newIndex, _ := r.GetLastIndex()
    newIndex++
    term := r.GetCurrentTerm()

    if newEntry != nil {
      glog.V(2).Infof("Appending data for index %d term %d", newIndex, term)
      newEntry.Index = newIndex
      newEntry.Term = term
      err := r.appendEntries([]storage.Entry{*newEntry})
      if err != nil {
        return 0, err
      }
    }

    r.setLastIndex(newIndex, term)

    // Now fire off this change to all of the peers
    for _, p := range(state.peers) {
      p.propose(newIndex)
    }
    return newIndex, nil
}
