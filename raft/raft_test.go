package raft

import (
  "bytes"
  "testing"
  "time"
)

const (
  MaxWaitTime = 60 * time.Second
)

func TestWaitForLeader(t *testing.T) {
  waitForLeader(t)
  appendAndVerify(t, "First test", 3)
}

/*
func TestStopFollower(t *testing.T) {
  waitForLeader(t)

  var follower *RaftImpl = nil
  //var i int
  var r *RaftImpl
  for _, r = range(testRafts) {
    if r.GetState() == StateFollower {
      follower = r
    }
  }

  follower.Close()
  //testRafts[i] = nil
  time.Sleep(time.Second)
  waitForLeader(t)
}
*/

// After stopping the leader, a new one is elected
func TestStopLeader(t *testing.T) {
  waitForLeader(t)

  var leaderIndex int
  for i, r := range(testRafts) {
    if r.GetState() == StateLeader {
      leaderIndex = i
    }
  }

  testRafts[leaderIndex].Close()
  testListener[leaderIndex].Close()
  time.Sleep(time.Second)
  waitForLeader(t)
  appendAndVerify(t, "Second test. Yay!", 2)
}

func waitForLeader(t *testing.T) {
  for i := 0; i < 40; i++ {
    _, leaders := countRafts(t)
    if leaders == 0 {
      time.Sleep(time.Second)
    } else if leaders == 1 {
      return
    } else {
      t.Fatalf("Expected only one leader but found %d", leaders)
    }
  }
  t.Fatal("Waited too long for a leader to emerge")
}

func appendAndVerify(t *testing.T, msg string, expectedCount int) {
  data := []byte(msg)
  leader := getLeader(t)
  lastIndex, _ := leader.GetLastIndex()
  err := leader.Propose(data)
  if err != nil { t.Fatalf("Proposal failed: %v", err) }
  t.Logf("Wrote data at index %d", lastIndex + 1)

  for i := 0; i < 10; i++ {
    if verifyIndex(t, lastIndex + 1, data, expectedCount) {
      t.Log("Index now matches")
      if verifyCommit(t, lastIndex + 1, expectedCount) {
        t.Log("Commit index matches too")
        return
      }
    }
    time.Sleep(time.Second)
  }
  t.Fatal("Indices not replicated in time")
}

func countRafts(t *testing.T) (int, int) {
  var followers, leaders int

  for _, r := range(testRafts) {
    switch r.GetState() {
    case StateFollower:
      followers++
    case StateLeader:
      leaders++
    }
  }

  t.Logf("total = %d followers = %d leaders = %d", len(testRafts), followers, leaders)

  return followers, leaders
}

func getLeader(t *testing.T) *RaftImpl {
  for _, r := range(testRafts) {
    if r.GetState() == StateLeader {
      return r
    }
  }
  t.Fatal("No leader present")
  return nil
}

func verifyIndex(t *testing.T, ix uint64, expected []byte, expectedCount int) bool {
  correctCount := 0
  for _, raft := range(testRafts) {
    verified := true
    _, data, err := raft.stor.GetEntry(ix)
    if err != nil { t.Fatalf("Error getting entry: %v", err) }
    if data == nil {
      t.Logf("Index %d not replicated to raft %d", ix, raft.id)
      verified = false
    }
    if !bytes.Equal(expected, data) {
      t.Log("Data in log does not match")
      verified = false
    }
    if verified {
      correctCount++
    }
  }
  t.Logf("%d peers updated out of %d expected", correctCount, expectedCount)
  return correctCount >= expectedCount
}

func verifyCommit(t *testing.T, ix uint64, expectedCount int) bool {
  correctCount := 0
  for _, raft := range(testRafts) {
    t.Logf("Node %d has commit index %d", raft.id, raft.GetCommitIndex())
    if raft.GetCommitIndex() >= ix {
      correctCount++
    }
  }
  t.Logf("%d peers have right commit index out of %d expected",
    correctCount, expectedCount)
  return correctCount >= expectedCount
}
