package raft

import (
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

func (r *RaftImpl) handleFollowerAppend(state *raftState, cmd appendCommand) {
  log.Debugf("Got append request for term %d", cmd.ar.Term)
  resp := communication.AppendResponse{
    Term: state.currentTerm,
  }

  // 5.1: Reply false if term doesn't match current term
  if cmd.ar.Term < state.currentTerm {
    resp.Success = false
    cmd.rc <- &resp
    return
  }

  // 5.3: Reply false if log doesn’t contain an entry at prevLogIndex
  // whose term matches prevLogTerm
  ourTerm, _, err := r.stor.GetEntry(cmd.ar.PrevLogIndex)
  if err != nil {
    resp.Error = err
    cmd.rc <- &resp
    return
  }
  if ourTerm != cmd.ar.PrevLogTerm {
    resp.Success = false
    cmd.rc <- &resp
    return
  }

  if len(cmd.ar.Entries) > 0 {
    err = r.appendEntries(cmd.ar.Entries)
    if err != nil {
      resp.Error = err
      cmd.rc <- &resp
      return
    }

    // If leaderCommit > commitIndex, set commitIndex =
    // min(leaderCommit, index of last new entry)
    if cmd.ar.LeaderCommit > state.commitIndex {
      lastIndex := cmd.ar.Entries[len(cmd.ar.Entries) - 1].Index
      if cmd.ar.LeaderCommit < lastIndex {
        state.commitIndex = cmd.ar.LeaderCommit
      } else {
        state.commitIndex = lastIndex
      }
      log.Debugf("Node %d: Commit index now %d", r.id, state.commitIndex)

      // 5.3: If commitIndex > lastApplied: increment lastApplied,
      // apply log[lastApplied] to state machine
      for _, e := range(cmd.ar.Entries) {
        if e.Index > state.lastApplied && e.Index < state.commitIndex {
          r.mach.ApplyEntry(e.Data)
          state.lastApplied++
        }
        log.Debugf("Node %d: Last applied now %d", r.id, state.lastApplied)
      }
    }
  }

  resp.Term = state.currentTerm
  resp.Success = true

  cmd.rc <- &resp
}

func (r *RaftImpl) sendEntries(state *raftState, entries []storage.Entry) {
  lastIndex, lastTerm, err := r.stor.GetLastIndex()
  if err != nil {
    log.Infof("Error reading database to send new entries: %v", err)
    return
  }

  ar := communication.AppendRequest{
    Term: state.currentTerm,
    LeaderId: r.id,
    PrevLogIndex: lastIndex,
    PrevLogTerm: lastTerm,
    LeaderCommit: state.commitIndex,
    Entries: entries,
  }

  nodes := r.disco.GetNodes()
  log.Debugf("Sending append request to %d nodes for term %d", len(nodes), state.currentTerm)

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
