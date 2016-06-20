package raft

import (
	"bytes"
	"fmt"
	"net"
	"path"
	"strconv"
	"time"

	"github.com/30x/changeagent/communication"
	"github.com/30x/changeagent/hooks"
	"github.com/30x/changeagent/storage"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	pollInterval = 250 * time.Millisecond
	testTimeout  = 10 * time.Second
)

var _ = Describe("Raft Tests", func() {
	BeforeEach(func() {
		assertOneLeader()
	})

	It("Basic Append", func() {
		appendAndVerify("First test", 3)
	})

	It("Append to Non-Leader", func() {
		entry := storage.Entry{
			Data:      []byte("Non-Leader Append"),
			Timestamp: time.Now(),
		}
		ix, err := getNonLeader().Propose(entry)
		Expect(err).Should(Succeed())
		waitForApply(ix, 3)
	})

	// Verify that we can add a webhook that will block invalid requests
	It("WebHook", func() {
		appendAndVerify("FailMe", 3)

		wh := []hooks.WebHook{
			hooks.WebHook{URI: fmt.Sprintf("http://%s", webHookAddr)},
		}
		ix, err := getLeader().UpdateWebHooks(wh)
		Expect(err).Should(Succeed())
		waitForApply(ix, 3)

		entry := storage.Entry{
			Data:      []byte("FailMe"),
			Timestamp: time.Now(),
		}
		ix, err = getLeader().Propose(entry)
		Expect(err).ShouldNot(Succeed())
		waitForApply(ix, 3)

		appendAndVerify("DontFailMe", 3)

		ix, err = getLeader().UpdateWebHooks([]hooks.WebHook{})
		Expect(err).Should(Succeed())
		waitForApply(ix, 3)

		appendAndVerify("FailMe", 3)
	})

	// After stopping the leader, a new one is elected
	It("Stop Leader", func() {
		var leaderIndex int
		for i, r := range testRafts {
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
		err := restartOneNode(leaderIndex, savedPath, savedPort)
		Expect(err).Should(Succeed())

		time.Sleep(time.Second)
		assertOneLeader()
		appendAndVerify("Restarted third node. Yay!", 3)
	})

	// After stopping one follower, things are pretty normal actually.
	It("Stop Follower", func() {
		var followerIndex int
		for i, r := range testRafts {
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
		err := restartOneNode(followerIndex, savedPath, savedPort)
		Expect(err).Should(Succeed())

		time.Sleep(time.Second)
		assertOneLeader()
		appendAndVerify("Restarted third node. Yay!", 3)
	})

	It("Add Node", func() {
		appendAndVerify("Before leadership change", 3)

		listener, addr := startListener()
		newRaft, err := startRaft(testDiscovery, listener, path.Join(DataDir, "test4"))
		Expect(err).Should(Succeed())
		testListener = append(testListener, listener)
		testRafts = append(testRafts, newRaft)

		time.Sleep(time.Second)
		fmt.Fprintf(GinkgoWriter, "Adding new configuration for node 4\n")
		testDiscovery.AddNode(addr)
		time.Sleep(time.Second)
		assertOneLeader()
		appendAndVerify("New leader elected. Yay!", 4)
	})
})

func stopOneNode(stopID int) (communication.NodeID, string, int) {
	savedID := testRafts[stopID].id
	savedPath := testRafts[stopID].stor.GetDataPath()
	_, savedPortStr, _ := net.SplitHostPort(testListener[stopID].Addr().String())
	savedPort, _ := strconv.Atoi(savedPortStr)
	testRafts[stopID].Close()
	testListener[stopID].Close()
	return savedID, savedPath, savedPort
}

func restartOneNode(ix int, savedPath string, savedPort int) error {
	restartedListener, err := net.ListenTCP("tcp4", &net.TCPAddr{Port: savedPort})
	if err != nil {
		return err
	}
	testListener[ix] = restartedListener
	restartedRaft, err :=
		startRaft(testDiscovery, testListener[ix], savedPath)
	if err != nil {
		return err
	}
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
		Data:      data,
		Timestamp: time.Now(),
	}
	index, err := leader.Propose(newEntry)
	Expect(err).Should(Succeed())
	Expect(index).Should(Equal(lastIndex + 1))
	fmt.Fprintf(GinkgoWriter, "Wrote data at index %d\n", lastIndex+1)

	Eventually(func() bool {
		if verifyIndex(index, data, expectedCount) {
			fmt.Fprintf(GinkgoWriter, "Index now matches")
			if verifyCommit(index, expectedCount) {
				fmt.Fprintf(GinkgoWriter, "Commit index matches too")
				if verifyApplied(index, expectedCount) {
					fmt.Fprintf(GinkgoWriter, "Applied data matches too")
					return true
				}
			}
		}
		return false
	}, testTimeout, pollInterval).Should(BeTrue())

	return lastIndex
}

func countRafts() (int, int) {
	var followers, leaders int

	for _, r := range testRafts {
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
	for _, r := range testRafts {
		if r.GetState() == Leader {
			return r
		}
	}
	return nil
}

func getNonLeader() *Service {
	for _, r := range testRafts {
		if r.GetState() != Leader {
			return r
		}
	}
	return nil
}

func verifyIndex(ix uint64, expected []byte, expectedCount int) bool {
	correctCount := 0
	for _, raft := range testRafts {
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
	for _, raft := range testRafts {
		fmt.Fprintf(GinkgoWriter, "Node %d has commit index %d\n", raft.id, raft.GetCommitIndex())
		if raft.GetCommitIndex() >= ix {
			correctCount++
		}
	}
	fmt.Fprintf(GinkgoWriter, "%d peers have right commit index out of %d expected\n",
		correctCount, expectedCount)
	return correctCount >= expectedCount
}

func verifyApplied(ix uint64, expectedCount int) bool {
	correctCount := 0
	for _, raft := range testRafts {
		if raft.GetLastApplied() >= ix {
			correctCount++
		}
	}
	fmt.Fprintf(GinkgoWriter, "%d peers have right data applied out of %d expected\n",
		correctCount, expectedCount)
	return correctCount >= expectedCount
}

func waitForApply(ix uint64, expectedCount int) {
	Eventually(func() bool {
		return verifyApplied(ix, expectedCount)
	}, testTimeout, pollInterval).Should(BeTrue())
}
