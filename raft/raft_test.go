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
  appendAndVerify(t, "First test")
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
  appendAndVerify(t, "Second test. Yay!")
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

func appendAndVerify(t *testing.T, msg string) {
  data := []byte(msg)
  leader := getLeader(t)
  lastIndex, _ := leader.GetLastIndex()
  err := leader.Propose(data)
  if err != nil { t.Fatalf("Proposal failed: %v", err) }
  t.Logf("Wrote data at index %d", lastIndex + 1)

  // TODO In loop, call verifyIndex. If it returns false, test fails.
  // Do it in a loop with delay!
  for i := 0; i < 10; i++ {
    if verifyIndex(t, lastIndex + 1, data) {
      t.Log("Index now matches")
      return
    }
    time.Sleep(time.Second)
  }
  // TODO also check commitIndex.
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

func verifyIndex(t *testing.T, ix uint64, expected []byte) bool {
  verified := true
  for _, raft := range(testRafts) {
    term, data, err := raft.stor.GetEntry(ix)
    if err != nil { t.Fatalf("Error getting entry: %v", err) }
    t.Logf("Node %d:, Index %d has term %d", raft.id, ix, term)
    if data == nil {
      t.Logf("Index %d not replicated to raft %d", ix, raft.id)
      verified = false
    }
    if !bytes.Equal(expected, data) {
      t.Log("Data in log does not match")
      verified = false
    }
  }
  return verified
}
