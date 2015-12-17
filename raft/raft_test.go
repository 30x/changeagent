package raft

import (
  "testing"
  "time"
)

const (
  MaxWaitTime = 60 * time.Second
)

func TestWaitForLeader(t *testing.T) {
  waitForLeader(t)
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

  var leader *RaftImpl = nil
  for _, r := range(testRafts) {
    if r.GetState() == StateLeader {
      leader = r
    }
  }

  leader.Close()
  time.Sleep(time.Second)
  waitForLeader(t)
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
