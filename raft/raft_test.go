package raft

import (
  "bytes"
  "fmt"
  "net"
  "strconv"
  "time"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

const (
  MaxWaitTime = 60 * time.Second
)

var _ = Describe("Raft Tests", func() {
  BeforeEach(func() {
    assertOneLeader()
  })

  It("Wait for leader", func() {
    appendAndVerify("First test", 3)
  })

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
  It("Stop Leader", func() {
    var leaderIndex int
    for i, r := range (testRafts) {
      if r.GetState() == Leader {
        leaderIndex = i
      }
    }

    fmt.Fprintf(GinkgoWriter, "Stopping leader node %d\n", testRafts[leaderIndex].id)
    savedID, savedPath, savedPort := stopOneNode(leaderIndex)
    time.Sleep(time.Second)

    assertOneLeader()
    appendAndVerify("Second test. Yay!", 2)
    // Previous step read the storage so we can close it now.
    testRafts[leaderIndex].stor.Close()

    fmt.Fprintf(GinkgoWriter, "Restarting node %d on port %d\n", savedID, savedPort)
    err := restartOneNode(leaderIndex, savedID, savedPath, savedPort)
    Expect(err).Should(Succeed())

    time.Sleep(time.Second)
    assertOneLeader()
    appendAndVerify("Restarted third node. Yay!", 3)
  })

  // After stopping one follower, things are pretty normal actually.
  It("Stop Follower", func() {
    var followerIndex int
    for i, r := range (testRafts) {
      if r.GetState() == Follower {
        followerIndex = i
      }
    }

    fmt.Fprintf(GinkgoWriter, "Stopping follower node %d\n", testRafts[followerIndex].id)
    savedID, savedPath, savedPort := stopOneNode(followerIndex)
    time.Sleep(time.Second)

    assertOneLeader()
    appendAndVerify("Second test. Yay!", 2)
    // Previous step read the storage so we can close it now.
    testRafts[followerIndex].stor.Close()

    fmt.Fprintf(GinkgoWriter, "Restarting node %d on port %d\n", savedID, savedPort)
    err := restartOneNode(followerIndex, savedID, savedPath, savedPort)
    Expect(err).Should(Succeed())

    time.Sleep(time.Second)
    assertOneLeader()
    appendAndVerify("Restarted third node. Yay!", 3)
  })
})

func stopOneNode(stopID int) (uint64, string, int) {
  savedID := testRafts[stopID].id
  savedPath := testRafts[stopID].stor.GetDataPath()
  _, savedPortStr, _ := net.SplitHostPort(testListener[stopID].Addr().String())
  savedPort, _ := strconv.Atoi(savedPortStr)
  testRafts[stopID].Close()
  testListener[stopID].Close()
  return savedID, savedPath, savedPort
}

func restartOneNode(ix int, savedID uint64, savedPath string, savedPort int) error {
  restartedListener, err := net.ListenTCP("tcp4", &net.TCPAddr{Port: savedPort})
  if err != nil { return err }
  testListener[ix] = restartedListener
  restartedRaft, err :=
    startRaft(savedID, testDiscovery, testListener[ix], savedPath)
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

func assertOneLeader() {
  leaders := waitForLeader()
  Expect(leaders).Should(Equal(1))
}

func appendAndVerify(msg string, expectedCount int) uint64 {
  data := []byte(msg)
  leader := getLeader()
  Expect(leader).ShouldNot(BeNil())
  lastIndex, _ := leader.GetLastIndex()
  newEntry := storage.Entry{
    Data: data,
    Timestamp: time.Now(),
  }
  index, err := leader.Propose(&newEntry)
  Expect(err).Should(Succeed())
  Expect(index).Should(Equal(lastIndex + 1))
  fmt.Fprintf(GinkgoWriter, "Wrote data at index %d\n", lastIndex + 1)

  for i := 0; i < 10; i++ {
    if verifyIndex(index, data, expectedCount) {
      fmt.Fprintf(GinkgoWriter, "Index now matches")
      if verifyCommit(index, expectedCount) {
        fmt.Fprintf(GinkgoWriter, "Commit index matches too")
        if verifyApplied(index, data, expectedCount) {
          fmt.Fprintf(GinkgoWriter, "Applied data matches too")
          return index
        }
      }
    }
    time.Sleep(time.Second)
  }
  Expect(false).Should(BeTrue())
  return lastIndex
}

func countRafts() (int, int) {
  var followers, leaders int

  for _, r := range(testRafts) {
    switch r.GetState() {
    case Follower:
      followers++
    case Leader:
      leaders++
    }
  }

  return followers, leaders
}

func getLeader() *Service {
  for _, r := range(testRafts) {
    if r.GetState() == Leader {
      return r
    }
  }
  return nil
}

func verifyIndex(ix uint64, expected []byte, expectedCount int) bool {
  correctCount := 0
  for _, raft := range(testRafts) {
    verified := true
    entry, err := raft.stor.GetEntry(ix)
    Expect(err).Should(Succeed())
    if entry == nil {
      fmt.Fprintf(GinkgoWriter, "Index %d not replicated to raft %d\n", ix, raft.id)
      verified = false
    } else if !bytes.Equal(expected, entry.Data) {
      fmt.Fprintf(GinkgoWriter, "Data in log does not match\n")
      verified = false
    }
    if verified {
      correctCount++
    }
  }
  fmt.Fprintf(GinkgoWriter, "%d peers updated out of %d expected\n", correctCount, expectedCount)
  return correctCount >= expectedCount
}

func verifyCommit(ix uint64, expectedCount int) bool {
  correctCount := 0
  for _, raft := range(testRafts) {
    fmt.Fprintf(GinkgoWriter, "Node %d has commit index %d\n", raft.id, raft.GetCommitIndex())
    if raft.GetCommitIndex() >= ix {
      correctCount++
    }
  }
  fmt.Fprintf(GinkgoWriter, "%d peers have right commit index out of %d expected\n",
    correctCount, expectedCount)
  return correctCount >= expectedCount
}

func verifyApplied(ix uint64, expectedData []byte, expectedCount int) bool {
  correctCount := 0
  for _, raft := range(testRafts) {
    if raft.GetLastApplied() >= ix {
      correctCount++
    }
  }
  fmt.Fprintf(GinkgoWriter, "%d peers have right data applied out of %d expected\n",
    correctCount, expectedCount)
  return correctCount >= expectedCount
}
