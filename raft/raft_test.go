package raft

import (
  "bytes"
  "net"
  "strconv"
  "testing"
  "time"
)

const (
  MaxWaitTime = 60 * time.Second
)

func TestWaitForLeader(t *testing.T) {
  assertOneLeader(t)
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
  assertOneLeader(t)

  var leaderIndex int
  for i, r := range(testRafts) {
    if r.GetState() == StateLeader {
      leaderIndex = i
    }
  }

  t.Logf("Stopping leader node %d", testRafts[leaderIndex].id)
  savedId, savedPath, savedPort := stopOneNode(leaderIndex)
  time.Sleep(time.Second)

  assertOneLeader(t)
  appendAndVerify(t, "Second test. Yay!", 2)
  // Previous step read the storage so we can close it now.
  testRafts[leaderIndex].stor.Close()

  t.Logf("Restarting node %d on port %d", savedId, savedPort)
  err := restartOneNode(leaderIndex, savedId, savedPath, savedPort)
  if err != nil { t.Fatalf("Error restarting node: %v", err) }

  time.Sleep(time.Second)
  assertOneLeader(t)
  appendAndVerify(t, "Restarted third node. Yay!", 3)
}

// After stopping one follower, things are pretty normal actually.
func TestStopFollower(t *testing.T) {
  assertOneLeader(t)

  var followerIndex int
  for i, r := range(testRafts) {
    if r.GetState() == StateFollower {
      followerIndex = i
    }
  }

  t.Logf("Stopping follower node %d", testRafts[followerIndex].id)
  savedId, savedPath, savedPort := stopOneNode(followerIndex)
  time.Sleep(time.Second)

  assertOneLeader(t)
  appendAndVerify(t, "Second test. Yay!", 2)
  // Previous step read the storage so we can close it now.
  testRafts[followerIndex].stor.Close()

  t.Logf("Restarting node %d on port %d", savedId, savedPort)
  err := restartOneNode(followerIndex, savedId, savedPath, savedPort)
  if err != nil { t.Fatalf("Error restarting node: %v", err) }

  time.Sleep(time.Second)
  assertOneLeader(t)
  appendAndVerify(t, "Restarted third node. Yay!", 3)
}

func stopOneNode(stopId int) (uint64, string, int) {
  savedId := testRafts[stopId].id
  savedPath := testRafts[stopId].stor.GetDataPath()
  _, savedPortStr, _ := net.SplitHostPort(testListener[stopId].Addr().String())
  savedPort, _ := strconv.Atoi(savedPortStr)
  testRafts[stopId].Close()
  testListener[stopId].Close()
  return savedId, savedPath, savedPort
}

func restartOneNode(ix int, savedId uint64, savedPath string, savedPort int) error {
  restartedListener, err := net.ListenTCP("tcp4", &net.TCPAddr{Port: savedPort})
  if err != nil { return err }
  testListener[ix] = restartedListener
  restartedRaft, err :=
    startRaft(savedId, testDiscovery, testListener[ix], savedPath)
  if err != nil { return err }
  testRafts[ix] = restartedRaft
  return nil
}

func waitForLeader() int {
  time.Sleep(time.Second)
  for i := 0; i < 40; i++ {
    _, leaders := countRafts()
    if leaders == 0 {
      time.Sleep(time.Second)
    } else {
      return leaders
    }
  }
  return 0
}

func assertOneLeader(t *testing.T) {
  leaders := waitForLeader()
  switch leaders {
  case 0:
    t.Fatal("No leader present in time")
  case 1:
    return
  default:
    t.Fatalf("Found %d leaders when one will do", leaders)
  }
}

func appendAndVerify(t *testing.T, msg string, expectedCount int) uint64 {
  data := []byte(msg)
  leader := getLeader()
  if leader == nil { t.Fatal("No leader present") }
  lastIndex, _ := leader.GetLastIndex()
  index, err := leader.Propose(data)
  if err != nil { t.Fatalf("Proposal failed: %v", err) }
  if index != (lastIndex + 1) {
    t.Fatalf("Expected index %d and got %d", lastIndex + 1, index)
  }
  t.Logf("Wrote data at index %d", lastIndex + 1)

  for i := 0; i < 10; i++ {
    if verifyIndex(t, index, data, expectedCount) {
      t.Log("Index now matches")
      if verifyCommit(t, index, expectedCount) {
        t.Log("Commit index matches too")
        if verifyApplied(t, index, data, expectedCount) {
          t.Log("Applied data matches too")
          return index
        }
      }
    }
    time.Sleep(time.Second)
  }
  t.Fatal("Indices not replicated in time")
  return lastIndex
}

func countRafts() (int, int) {
  var followers, leaders int

  for _, r := range(testRafts) {
    switch r.GetState() {
    case StateFollower:
      followers++
    case StateLeader:
      leaders++
    }
  }

  return followers, leaders
}

func getLeader() *RaftImpl {
  for _, r := range(testRafts) {
    if r.GetState() == StateLeader {
      return r
    }
  }
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

func verifyApplied(t *testing.T, ix uint64, expectedData []byte, expectedCount int) bool {
  correctCount := 0
  for _, raft := range(testRafts) {
    if raft.GetLastApplied() >= ix {
      correctCount++
    }
  }
  t.Logf("%d peers have right data applied out of %d expected",
    correctCount, expectedCount)
  return correctCount >= expectedCount
}
