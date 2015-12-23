/*
 * Methods in this file handle the append logic for a raft instance.
 */

package raft

import (
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

func (r *RaftImpl) handleAppend(state *raftState, cmd appendCommand) {
  log.Debugf("Got append request for term %d. prevIndex = %d prevTerm = %d",
    cmd.ar.Term, cmd.ar.PrevLogIndex, cmd.ar.PrevLogTerm)
  currentTerm := r.GetCurrentTerm()
  commitIndex := r.GetCommitIndex()

  resp := communication.AppendResponse{
    Term: currentTerm,
  }

  // 5.1: Reply false if term doesn't match current term
  if cmd.ar.Term < r.currentTerm {
    log.Debugf("Term does not match current term %d", r.currentTerm)
    resp.Success = false
    cmd.rc <- &resp
    return
  }

  // 5.3: Reply false if log doesn’t contain an entry at prevLogIndex
  // whose term matches prevLogTerm
  // but of course we have to be able to start from zero!
  if cmd.ar.PrevLogIndex > 0 {
    ourTerm, _, err := r.stor.GetEntry(cmd.ar.PrevLogIndex)
    if err != nil {
      resp.Error = err
      cmd.rc <- &resp
      return
    }
    if ourTerm != cmd.ar.PrevLogTerm {
      log.Debugf("Term %d at index %d does not match %d in request",
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

  log.Debugf("leader commit: %d commitIndex: %d",
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
    log.Debugf("Node %d: Commit index now %d", r.id, commitIndex)

    // 5.3: If commitIndex > lastApplied: increment lastApplied,
    // apply log[lastApplied] to state machine
    lastApplied := r.GetLastApplied()
    for lastApplied < commitIndex {
      lastApplied++
      _, data, err := r.stor.GetEntry(lastApplied)
      if err != nil {
        resp.Error = err
        cmd.rc <- &resp
        return
      }

      r.mach.ApplyEntry(data)
    }

    r.setLastApplied(lastApplied)
    log.Debugf("Node %d: Last applied now %d", r.id, lastApplied)
  }

  resp.Term = currentTerm
  resp.Success = true
  resp.CommitIndex = commitIndex

  cmd.rc <- &resp
}

func (r *RaftImpl) sendAppend(id uint64, ar *communication.AppendRequest) (bool, error) {
  log.Debugf("Sending append request to node %d for term %d", id, ar.Term)

  resp, err := r.comm.Append(id, ar)
  if err == nil {
    return resp.Success, nil
  }
  return false, err
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

  // Update cached state so we don't have to read every time we send a heartbeat
  if len(entries) > 0 {
    last := entries[len(entries) - 1]
    r.setLastIndex(last.Index, last.Term)
  }

  return nil
}

func (r *RaftImpl) makeProposal(data []byte, state *raftState) error {
    // If command received from client: append entry to local log,
    // respond after entry applied to state machine (§5.3)
    newIndex, _ := r.GetLastIndex()
    newIndex++
    term := r.GetCurrentTerm()
    newEntry := storage.Entry{
      Index: newIndex,
      Term: term,
      Data: data,
    }

    log.Debugf("Appending %d bytes of data for index %d term %d",
      len(data), newIndex, term)
    err := r.appendEntries([]storage.Entry{newEntry})
    if err != nil {
      return err
    }

    r.setLastIndex(newIndex, term)

    // Now fire off this change to all of the peers
    for _, p := range(state.peers) {
      p.propose(newIndex)
    }
    return nil
}
