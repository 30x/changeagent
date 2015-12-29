package raft

import (
  "testing"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

/*
 * Initial state, set up on raft_main_test.go:
 * term 2, commit index 1.
 * Log:
 *  Index 1, term 1
 *  Index 2, term 2
 *  Index 3, term 2
 */

/*
 * Vote RPC tests, from the spec.
 */

var lastIndex uint64 = 3

// Reply false if term < currentTerm (§5.1)
func TestVoteOldTerm(t *testing.T) {
  req := &communication.VoteRequest{
    Term: 1,
    CandidateId: 2,
    LastLogIndex: 3,
    LastLogTerm: 2,
  }

  resp, err := unitTestRaft.RequestVote(req)
  if err != nil { t.Fatalf("Error in vote: %v", err) }
  if resp.VoteGranted {
    t.Fatal("Expected vote not to be granted due to old term")
  }
}

// If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func TestVoteOutOfDate(t *testing.T) {
  req := &communication.VoteRequest{
    Term: 1,
    CandidateId: 2,
    LastLogIndex: 2,
    LastLogTerm: 2,
  }

  resp, err := unitTestRaft.RequestVote(req)
  if err != nil { t.Fatalf("Error in vote: %v", err) }
  if resp.VoteGranted {
    t.Fatal("Expected vote not to be granted due to old index")
  }
}

// Test valid voting, and that we keep track of who we voted for
func TestVoting(t *testing.T) {
  req := &communication.VoteRequest{
    Term: 3,
    CandidateId: 2,
    LastLogIndex: 3,
    LastLogTerm: 2,
  }

  resp, err := unitTestRaft.RequestVote(req)
  if err != nil { t.Fatalf("Error in vote: %v", err) }
  if !resp.VoteGranted {
    t.Fatal("Expected vote to be granted")
  }

  req = &communication.VoteRequest{
    Term: 3,
    CandidateId: 2,
    LastLogIndex: 3,
    LastLogTerm: 2,
  }

  resp, err = unitTestRaft.RequestVote(req)
  if err != nil { t.Fatalf("Error in vote: %v", err) }
  if !resp.VoteGranted {
    t.Fatal("Expected vote to be granted even the second time")
  }

  req = &communication.VoteRequest{
    Term: 3,
    CandidateId: 3,
    LastLogIndex: 3,
    LastLogTerm: 2,
  }

  resp, err = unitTestRaft.RequestVote(req)
  if err != nil { t.Fatalf("Error in vote: %v", err) }
  if resp.VoteGranted {
    t.Fatal("Expected vote not to be granted because we already voted")
  }
}

/*
 * AppendEntries RPC tests, from the spec.
 */

// Reply false if term < currentTerm (§5.1)
func TestOldTermAppend(t *testing.T) {
  req := &communication.AppendRequest{
    Term: 1,
    LeaderId: 1,
  }
  resp, err := unitTestRaft.Append(req)
  if err != nil { t.Fatalf("Error in append: %v", err) }
  if resp.Success {
    t.Fatalf("Expected false due to old term")
  }
}

// Reply false if log doesn’t contain an entry at prevLogIndex
// whose term matches prevLogTerm (§5.3)
func TestLogNoMatch(t *testing.T) {
  req := &communication.AppendRequest{
    Term: 1,
    LeaderId: 1,
    PrevLogIndex: 10,
    PrevLogTerm: 2,
  }
  resp, err := unitTestRaft.Append(req)
  if err != nil { t.Fatalf("Error in append: %v", err) }
  if resp.Success {
    t.Fatalf("Expected false due to missing log")
  }
}

func TestLogNoMatchTerm(t *testing.T) {
  req := &communication.AppendRequest{
    Term: 1,
    LeaderId: 1,
    PrevLogIndex: 1,
    PrevLogTerm: 2,
  }
  resp, err := unitTestRaft.Append(req)
  if err != nil { t.Fatalf("Error in append: %v", err) }
  if resp.Success {
    t.Fatalf("Expected false due to mismatched log")
  }
}

func TestAppend(t *testing.T) {
  // Append any new entries not already in the log
  req := &communication.AppendRequest{
    Term: 2,
    LeaderId: 1,
    PrevLogIndex: 3,
    PrevLogTerm: 2,
    Entries: []storage.Entry{
      storage.Entry{
        Term: 2,
        Index: 4,
      },
      storage.Entry{
        Term: 2,
        Index: 5,
      },
    },
  }
  resp, err := unitTestRaft.Append(req)
  if err != nil { t.Fatalf("Error in append: %v", err) }
  if !resp.Success {
    t.Fatalf("Expected this to be a successful append")
  }
  lastIndex = 5

  term, _, err := unitTestRaft.stor.GetEntry(4)
  if err != nil { t.Fatalf("Error in get: %v", err) }
  if term != 2 {
    t.Fatalf("Expected term 2 and got %d", term)
  }

  term, _, err = unitTestRaft.stor.GetEntry(5)
  if err != nil { t.Fatalf("Error in get: %v", err) }
  if term != 2 {
    t.Fatalf("Expected term 2 and got %d", term)
  }

  // If an existing entry conflicts with a new one (same index
  // but different terms), delete the existing entry and all that
  // follow it (§5.3)
  req = &communication.AppendRequest{
    Term: 3,
    LeaderId: 1,
    PrevLogIndex: 3,
    PrevLogTerm: 2,
    Entries: []storage.Entry{
      storage.Entry{
        Term: 3,
        Index: 4,
      },
    },
  }
  resp, err = unitTestRaft.Append(req)
  if err != nil { t.Fatalf("Error in append: %v", err) }
  if !resp.Success {
    t.Fatalf("Expected this to be a successful append")
  }
  lastIndex = 4

  term, _, err = unitTestRaft.stor.GetEntry(4)
  if err != nil { t.Fatalf("Error in get: %v", err) }
  if term != 3 {
    t.Fatalf("Expected term 3 and got %d", term)
  }

  term, _, err = unitTestRaft.stor.GetEntry(5)
  if err != nil { t.Fatalf("Error in get: %v", err) }
  if term != 0 {
    t.Fatalf("Expected term 0 and got %d", term)
  }

  // Append any new entries not already in the log, again
  req = &communication.AppendRequest{
    Term: 3,
    LeaderId: 1,
    PrevLogIndex: 3,
    PrevLogTerm: 2,
    Entries: []storage.Entry{
      storage.Entry{
        Term: 3,
        Index: 5,
      },
      storage.Entry{
        Term: 3,
        Index: 6,
      },
    },
  }
  resp, err = unitTestRaft.Append(req)
  if err != nil { t.Fatalf("Error in append: %v", err) }
  if !resp.Success {
    t.Fatalf("Expected this to be a successful append")
  }
  lastIndex = 6

  term, _, err = unitTestRaft.stor.GetEntry(5)
  if err != nil { t.Fatalf("Error in get: %v", err) }
  if term != 3 {
    t.Fatalf("Expected term 3 and got %d", term)
  }

  term, _, err = unitTestRaft.stor.GetEntry(6)
  if err != nil { t.Fatalf("Error in get: %v", err) }
  if term != 3 {
    t.Fatalf("Expected term 3 and got %d", term)
  }

  // If leaderCommit > commitIndex, set commitIndex =
  // min(leaderCommit, index of last new entry)
  // TODO does this have to work even if we have no entries to append?
  req = &communication.AppendRequest{
    Term: 3,
    LeaderId: 1,
    LeaderCommit: 3,
  }
  resp, err = unitTestRaft.Append(req)
  if err != nil { t.Fatalf("Error in append: %v", err) }
  if !resp.Success {
    t.Fatalf("Expected this to be a successful append")
  }
  if resp.CommitIndex != 3 {
    t.Fatalf("Expected new commit index at 3, not %d", resp.CommitIndex)
  }

  req = &communication.AppendRequest{
    Term: 3,
    LeaderId: 1,
    LeaderCommit: 5,
    Entries: []storage.Entry{
      storage.Entry{
        Term: 3,
        Index: 7,
      },
    },
  }
  resp, err = unitTestRaft.Append(req)
  if err != nil { t.Fatalf("Error in append: %v", err) }
  if !resp.Success {
    t.Fatalf("Expected this to be a successful append")
  }
  if resp.CommitIndex != 5 {
    t.Fatalf("Expected new commit index at 5, not %d", resp.CommitIndex)
  }

  req = &communication.AppendRequest{
    Term: 3,
    LeaderId: 1,
    LeaderCommit: 99,
    Entries: []storage.Entry{
      storage.Entry{
        Term: 3,
        Index: 8,
      },
    },
  }
  resp, err = unitTestRaft.Append(req)
  if err != nil { t.Fatalf("Error in append: %v", err) }
  if !resp.Success {
    t.Fatalf("Expected this to be a successful append")
  }
  if resp.CommitIndex != 8 {
    t.Fatalf("Expected top commit index at 8, not %d", resp.CommitIndex)
  }
  lastIndex = 8
}

/*
 * Commit index calculation, from the spec.
 * Testing it using an even number since this particular algorithm doesn't
 * account for the state of the leader.
 */

func TestNoCommitTooOld(t *testing.T) {
  matches := map[uint64]uint64{
    1: 0,
    2: 0,
    3: 0,
    4: 0,
  }
  state := &raftState{
    peerMatches: matches,
  }
  newIndex := unitTestRaft.calculateCommitIndex(state)
  if newIndex != unitTestRaft.GetCommitIndex() {
    t.Fatalf("commit index should be %d, not %d",
      unitTestRaft.GetCommitIndex(), newIndex)
  }
}

func TestNoCommitNoConsensus(t *testing.T) {
  matches := map[uint64]uint64{
    1: lastIndex,
    2: 1,
    3: 1,
    4: 1,
  }
  state := &raftState{
    peerMatches: matches,
  }
  newIndex := unitTestRaft.calculateCommitIndex(state)
  if newIndex != unitTestRaft.GetCommitIndex() {
    t.Fatalf("commit index should be %d, not %d",
      unitTestRaft.GetCommitIndex(), newIndex)
  }
}

func TestCommitConsensus(t *testing.T) {
  oldIndex := unitTestRaft.GetCommitIndex()
  defer unitTestRaft.setCommitIndex(oldIndex)
  unitTestRaft.setCommitIndex(lastIndex - 2)

  matches := map[uint64]uint64{
    1: lastIndex,
    2: lastIndex,
    3: lastIndex,
    4: 1,
  }
  state := &raftState{
    peerMatches: matches,
  }
  newIndex := unitTestRaft.calculateCommitIndex(state)
  if newIndex != lastIndex {
    t.Fatalf("commit index should be %d, not %d",
      lastIndex, newIndex)
  }
}

func TestCommitConsensus2(t *testing.T) {
  oldIndex := unitTestRaft.GetCommitIndex()
  defer unitTestRaft.setCommitIndex(oldIndex)
  unitTestRaft.setCommitIndex(lastIndex - 2)

  matches := map[uint64]uint64{
    1: 1,
    2: lastIndex,
    3: lastIndex - 1,
    4: lastIndex - 1,
  }
  state := &raftState{
    peerMatches: matches,
  }
  newIndex := unitTestRaft.calculateCommitIndex(state)
  if newIndex != lastIndex - 1 {
    t.Fatalf("commit index should be %d, not %d",
      lastIndex - 1, newIndex)
  }
}
