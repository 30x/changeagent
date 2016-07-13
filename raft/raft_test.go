package raft

import (
	"bytes"
	"fmt"
	"net"
	"path"
	"strconv"
	"time"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/hooks"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	pollInterval = 250 * time.Millisecond
	testTimeout  = 20 * time.Second
)

var _ = Describe("Raft Tests", func() {
	BeforeEach(func() {
		assertOneLeader()
	})

	It("Basic Append", func() {
		appendAndVerify("First test", clusterSize)
	})

	It("Append to Non-Leader", func() {
		entry := common.Entry{
			Data:      []byte("Non-Leader Append"),
			Timestamp: time.Now(),
		}
		ix, err := getNonLeader().Propose(&entry)
		Expect(err).Should(Succeed())
		waitForApply(ix, clusterSize, pollInterval)
	})

	// Verify that we can add a webhook that will block invalid requests
	It("WebHook", func() {
		appendAndVerify("FailMe", clusterSize)

		wh := []hooks.WebHook{
			hooks.WebHook{URI: fmt.Sprintf("http://%s", webHookAddr)},
		}
		ix, err := getLeader().UpdateWebHooks(wh)
		Expect(err).Should(Succeed())
		waitForApply(ix, clusterSize, pollInterval)

		entry := common.Entry{
			Data:      []byte("FailMe"),
			Timestamp: time.Now(),
		}
		ix, err = getLeader().Propose(&entry)
		Expect(err).ShouldNot(Succeed())
		waitForApply(ix, clusterSize, pollInterval)

		appendAndVerify("DontFailMe", clusterSize)

		ix, err = getLeader().UpdateWebHooks([]hooks.WebHook{})
		Expect(err).Should(Succeed())
		waitForApply(ix, clusterSize, pollInterval)

		appendAndVerify("FailMe", clusterSize)
	})

	It("Purge", func() {
		appendAndVerify("Purge 1", clusterSize)
		appendAndVerify("Purge 2", clusterSize)
		appendAndVerify("Purge 3", clusterSize)
		appendAndVerify("Purge 4", clusterSize)

		cfg := Config{
			MinPurgeRecords:  2,
			MinPurgeDuration: 0,
		}
		fmt.Fprintf(GinkgoWriter, "Updating raft configuration\n")
		ix, err := getLeader().UpdateRaftConfiguration(&cfg)
		Expect(err).Should(Succeed())
		Eventually(func() bool {
			return verifyCommit(ix, clusterSize)
		}, testTimeout, pollInterval).Should(BeTrue())
		fmt.Fprintf(GinkgoWriter, "Updated raft configuration at index %d\n", ix)

		for _, raft := range testRafts {
			Expect(raft.GetRaftConfig().MinPurgeRecords).Should(BeEquivalentTo(2))
			Expect(raft.GetRaftConfig().MinPurgeDuration).Should(BeZero())
		}

		Eventually(func() bool {
			// leave one for purging..., and one for purge request -- but not sure why!
			return verifyRecordCount(4)
		}, testTimeout, pollInterval).Should(BeTrue())

		fmt.Fprintf(GinkgoWriter, "Replacing original raft configuration\n")
		cfg = Config{}
		ix, err = getLeader().UpdateRaftConfiguration(&cfg)
		Expect(err).Should(Succeed())
		Eventually(func() bool {
			return verifyCommit(ix, clusterSize)
		}, testTimeout, pollInterval).Should(BeTrue())
		fmt.Fprintf(GinkgoWriter, "Updated raft configuration at index %d\n", ix)

		appendAndVerify("After Purge", clusterSize)
		Consistently(func() bool {
			// Added two records -- one config change and the real record
			return verifyRecordCount(6)
		}).Should(BeTrue())

		for _, raft := range testRafts {
			Expect(raft.GetRaftConfig().MinPurgeRecords).Should(BeZero())
			Expect(raft.GetRaftConfig().MinPurgeDuration).Should(BeZero())
		}
	})

	// After stopping the leader, a new one is elected
	It("Stop Leader", func() {
		var leaderIndex int
		for i, r := range testRafts {
			if r.GetState() == Leader {
				leaderIndex = i
			}
		}

		fmt.Fprintf(GinkgoWriter, "Stopping leader node %s\n", testRafts[leaderIndex].id)
		savedID, savedPath, savedPort := stopOneNode(leaderIndex)
		time.Sleep(time.Second)

		assertOneLeader()
		appendAndVerify("Second test. Yay!", clusterSize-1)
		// Previous step read the storage so we can close it now.
		testRafts[leaderIndex].stor.Close()

		fmt.Fprintf(GinkgoWriter, "Restarting node %d on port %d\n", savedID, savedPort)
		err := restartOneNode(leaderIndex, savedPath, savedPort)
		Expect(err).Should(Succeed())

		time.Sleep(time.Second)
		assertOneLeader()
		appendAndVerify("Restarted third node. Yay!", clusterSize)
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
		appendAndVerify("Second test. Yay!", clusterSize-1)
		// Previous step read the storage so we can close it now.
		testRafts[followerIndex].stor.Close()

		fmt.Fprintf(GinkgoWriter, "Restarting node %d on port %d\n", savedID, savedPort)
		err := restartOneNode(followerIndex, savedPath, savedPort)
		Expect(err).Should(Succeed())

		time.Sleep(time.Second)
		assertOneLeader()
		appendAndVerify("Restarted third node. Yay!", clusterSize)
	})

	It("Add Node", func() {
		appendAndVerify("Before leadership change", clusterSize)

		listener, addr := startListener()
		newRaft, err := startRaft(listener, path.Join(DataDir, "test4"))
		Expect(err).Should(Succeed())
		testListener = append(testListener, listener)
		testRafts = append(testRafts, newRaft)

		time.Sleep(time.Second)
		fmt.Fprintf(GinkgoWriter, "Adding new configuration for node 4\n")
		err = getLeader().AddNode(addr)
		Expect(err).Should(Succeed())
		time.Sleep(time.Second)
		assertOneLeader()
		clusterSize++
		appendAndVerify("New leader elected. Yay!", clusterSize)
	})

	It("Remove Follower", func() {
		appendAndVerify("Before leadership change", clusterSize)

		nonLeader := getNonLeader()
		fmt.Fprintf(GinkgoWriter, "Removing non-leader node %s\n", nonLeader.id)

		err := getLeader().RemoveNode(nonLeader.id)
		Expect(err).Should(Succeed())
		time.Sleep(time.Second)
		// Removed node is still running now, and should be in standalone mode
		assertOneLeader()
		clusterSize--
		appendAndVerify("Follower removed. Yay!", clusterSize)
		// TODO It seems like sometimes the follower gets an append and re-joins
		//Expect(nonLeader.GetState()).Should(Equal(Standalone))
	})

	It("Remove Leader", func() {
		appendAndVerify("Before leadership change", clusterSize)

		leader := getLeader()
		fmt.Fprintf(GinkgoWriter, "Removing leader node %s\n", leader.id)

		err := leader.RemoveNode(leader.id)
		Expect(err).Should(Succeed())
		time.Sleep(time.Second)
		// Removed node is still running now, and should be in standalone mode
		assertOneLeader()
		clusterSize--
		appendAndVerify("Follower removed. Yay!", clusterSize)
		Expect(leader.GetState()).Should(Equal(Standalone))
	})
})

func stopOneNode(stopID int) (common.NodeID, string, int) {
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
		startRaft(testListener[ix], savedPath)
	if err != nil {
		return err
	}
	testRafts[ix] = restartedRaft
	return nil
}

func assertOneLeader() {
	Eventually(countLeaders, testTimeout, pollInterval).Should(Equal(1))
}

func appendAndVerify(msg string, expectedCount int) uint64 {
	data := []byte(msg)
	leader := getLeader()
	Expect(leader).ShouldNot(BeNil())
	lastIndex, _ := leader.GetLastIndex()
	newEntry := common.Entry{
		Data:      data,
		Timestamp: time.Now(),
	}
	index, err := leader.Propose(&newEntry)
	Expect(err).Should(Succeed())
	Expect(index).Should(Equal(lastIndex + 1))
	fmt.Fprintf(GinkgoWriter, "Wrote data at index %d\n", lastIndex+1)

	Eventually(func() bool {
		if verifyIndex(index, data, expectedCount) {
			fmt.Fprintf(GinkgoWriter, "Index now matches\n")
			if verifyCommit(index, expectedCount) {
				fmt.Fprintf(GinkgoWriter, "Commit index matches too\n")
				if verifyApplied(index, expectedCount) {
					fmt.Fprintf(GinkgoWriter, "Applied data matches too\n")
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
		case Follower, Standalone:
			followers++
		case Leader:
			leaders++
		}
	}
	fmt.Fprintf(GinkgoWriter, "%d followers and %d leaders\n", followers, leaders)

	return followers, leaders
}

func countLeaders() int {
	_, leaders := countRafts()
	return leaders
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
			fmt.Fprintf(GinkgoWriter, "Index %d not replicated to raft %s\n", ix, raft.id)
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
		fmt.Fprintf(GinkgoWriter, "Node %s has commit index %d\n", raft.id, raft.GetCommitIndex())
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

func waitForApply(ix uint64, expectedCount int, interval time.Duration) {
	Eventually(func() bool {
		return verifyApplied(ix, expectedCount)
	}, testTimeout, interval).Should(BeTrue())
}

func verifyRecordCount(expectedCount int) bool {
	for _, raft := range testRafts {
		entries, _ :=
			raft.stor.GetEntries(0, uint(expectedCount+10),
				func(e *common.Entry) bool { return true })
		if len(entries) != expectedCount {
			fmt.Fprintf(GinkgoWriter, "Raft %s has %d entries, not %d\n",
				raft.id, len(entries), expectedCount)
			for i, e := range entries {
				fmt.Fprintf(GinkgoWriter, "  e[%d] = %d type %d\n", i, e.Index, e.Type)
			}
			return false
		}
	}
	return true
}
