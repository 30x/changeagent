package raft

import (
	"errors"
	"fmt"

	"github.com/30x/changeagent/communication"
	"github.com/30x/changeagent/discovery"
	"github.com/30x/changeagent/storage"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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

var _ = Describe("Raft Unit Tests", func() {

	initialized := false
	var lastIndex uint64 = 3

	BeforeEach(func() {
		if initialized {
			return
		}

		unitTestRaft.setFollowerOnly(true)

		ar := communication.AppendRequest{
			Term:         2,
			LeaderID:     1,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			LeaderCommit: 1,
			Entries: []storage.Entry{
				storage.Entry{
					Index: 1,
					Term:  1,
				},
				storage.Entry{
					Index: 2,
					Term:  2,
				},
				storage.Entry{
					Index: 3,
					Term:  2,
				},
			},
		}
		resp, err := unitTestRaft.Append(ar)
		Expect(err).Should(Succeed())
		Expect(resp.Success).Should(BeTrue())
		Expect(resp.CommitIndex).Should(BeEquivalentTo(1))

		initialized = true
	})

	It("Vote Old Term", func() {
		// Reply false if term < currentTerm (§5.1)
		req := communication.VoteRequest{
			Term:         1,
			CandidateID:  2,
			LastLogIndex: 3,
			LastLogTerm:  2,
		}

		resp, err := unitTestRaft.RequestVote(req)
		Expect(err).Should(Succeed())
		Expect(resp.VoteGranted).Should(BeFalse())
	})

	It("Vote Out Of Date", func() {
		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		req := communication.VoteRequest{
			Term:         1,
			CandidateID:  2,
			LastLogIndex: 2,
			LastLogTerm:  2,
		}

		resp, err := unitTestRaft.RequestVote(req)
		Expect(err).Should(Succeed())
		Expect(resp.VoteGranted).Should(BeFalse())
	})

	It("Voting", func() {
		// Test valid voting, and that we keep track of who we voted for
		req := communication.VoteRequest{
			Term:         3,
			CandidateID:  2,
			LastLogIndex: 3,
			LastLogTerm:  2,
		}

		resp, err := unitTestRaft.RequestVote(req)
		Expect(err).Should(Succeed())
		Expect(resp.VoteGranted).Should(BeTrue())

		req = communication.VoteRequest{
			Term:         3,
			CandidateID:  2,
			LastLogIndex: 3,
			LastLogTerm:  2,
		}

		resp, err = unitTestRaft.RequestVote(req)
		Expect(err).Should(Succeed())
		Expect(resp.VoteGranted).Should(BeTrue())

		req = communication.VoteRequest{
			Term:         3,
			CandidateID:  3,
			LastLogIndex: 3,
			LastLogTerm:  2,
		}

		resp, err = unitTestRaft.RequestVote(req)
		Expect(err).Should(Succeed())
		Expect(resp.VoteGranted).Should(BeFalse())
	})

	/*
	 * AppendEntries RPC tests, from the spec.
	 */

	It("Old Term Append", func() {
		// Reply false if term < currentTerm (§5.1)
		req := communication.AppendRequest{
			Term:     1,
			LeaderID: 1,
		}
		resp, err := unitTestRaft.Append(req)
		Expect(err).Should(Succeed())
		Expect(resp.Success).Should(BeFalse())
	})

	It("Log No Match", func() {
		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
		req := communication.AppendRequest{
			Term:         1,
			LeaderID:     1,
			PrevLogIndex: 10,
			PrevLogTerm:  2,
		}
		resp, err := unitTestRaft.Append(req)
		Expect(err).Should(Succeed())
		Expect(resp.Success).Should(BeFalse())
	})

	It("Log No Match Term", func() {
		req := communication.AppendRequest{
			Term:         1,
			LeaderID:     1,
			PrevLogIndex: 1,
			PrevLogTerm:  2,
		}
		resp, err := unitTestRaft.Append(req)
		Expect(err).Should(Succeed())
		Expect(resp.Success).Should(BeFalse())
	})

	It("Append", func() {
		// Append any new entries not already in the log
		req := communication.AppendRequest{
			Term:         2,
			LeaderID:     1,
			PrevLogIndex: 3,
			PrevLogTerm:  2,
			Entries: []storage.Entry{
				storage.Entry{
					Term:  2,
					Index: 4,
				},
				storage.Entry{
					Term:  2,
					Index: 5,
				},
			},
		}
		resp, err := unitTestRaft.Append(req)
		Expect(err).Should(Succeed())
		Expect(resp.Success).Should(BeTrue())
		lastIndex = 5

		entry, err := unitTestRaft.stor.GetEntry(4)
		Expect(err).Should(Succeed())
		Expect(entry.Term).Should(BeEquivalentTo(2))

		entry, err = unitTestRaft.stor.GetEntry(5)
		Expect(err).Should(Succeed())
		Expect(entry.Term).Should(BeEquivalentTo(2))

		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		req = communication.AppendRequest{
			Term:         3,
			LeaderID:     1,
			PrevLogIndex: 3,
			PrevLogTerm:  2,
			Entries: []storage.Entry{
				storage.Entry{
					Term:  3,
					Index: 4,
				},
			},
		}
		resp, err = unitTestRaft.Append(req)
		Expect(err).Should(Succeed())
		Expect(resp.Success).Should(BeTrue())
		lastIndex = 4

		entry, err = unitTestRaft.stor.GetEntry(4)
		Expect(err).Should(Succeed())
		Expect(entry.Term).Should(BeEquivalentTo(3))

		entry, err = unitTestRaft.stor.GetEntry(5)
		Expect(err).Should(Succeed())
		Expect(entry).Should(BeNil())

		// Append any new entries not already in the log, again
		req = communication.AppendRequest{
			Term:         3,
			LeaderID:     1,
			PrevLogIndex: 3,
			PrevLogTerm:  2,
			Entries: []storage.Entry{
				storage.Entry{
					Term:  3,
					Index: 5,
				},
				storage.Entry{
					Term:  3,
					Index: 6,
				},
			},
		}
		resp, err = unitTestRaft.Append(req)
		Expect(err).Should(Succeed())
		Expect(resp.Success).Should(BeTrue())
		lastIndex = 6

		entry, err = unitTestRaft.stor.GetEntry(5)
		Expect(err).Should(Succeed())
		Expect(entry.Term).Should(BeEquivalentTo(3))

		entry, err = unitTestRaft.stor.GetEntry(6)
		Expect(err).Should(Succeed())
		Expect(entry.Term).Should(BeEquivalentTo(3))

		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		// TODO does this have to work even if we have no entries to append?
		req = communication.AppendRequest{
			Term:         3,
			LeaderID:     1,
			LeaderCommit: 3,
		}
		resp, err = unitTestRaft.Append(req)
		Expect(err).Should(Succeed())
		Expect(resp.Success).Should(BeTrue())
		Expect(resp.CommitIndex).Should(BeEquivalentTo(3))

		req = communication.AppendRequest{
			Term:         3,
			LeaderID:     1,
			LeaderCommit: 5,
			Entries: []storage.Entry{
				storage.Entry{
					Term:  3,
					Index: 7,
				},
			},
		}
		resp, err = unitTestRaft.Append(req)
		Expect(err).Should(Succeed())
		Expect(resp.Success).Should(BeTrue())
		Expect(resp.CommitIndex).Should(BeEquivalentTo(5))

		req = communication.AppendRequest{
			Term:         3,
			LeaderID:     1,
			LeaderCommit: 99,
			Entries: []storage.Entry{
				storage.Entry{
					Term:  3,
					Index: 8,
				},
			},
		}
		resp, err = unitTestRaft.Append(req)
		Expect(err).Should(Succeed())
		Expect(resp.Success).Should(BeTrue())
		Expect(resp.CommitIndex).Should(BeEquivalentTo(8))
		lastIndex = 8
	})

	/*
	 * Commit index calculation, from the spec.
	 * Testing it using an even number since this particular algorithm doesn't
	 * account for the state of the leader.
	 */

	It("No Commit Too Old", func() {
		cur := discovery.NodeList{
			New: []string{"nodeSelf", "node1", "node2", "node3", "node4"},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}
		matches := map[string]uint64{
			"node1": 0,
			"node2": 0,
			"node3": 0,
			"node4": 0,
		}
		state := &raftState{
			peerMatches: matches,
		}
		newIndex := unitTestRaft.calculateCommitIndex(state, cfg)
		Expect(newIndex).Should(BeEquivalentTo(unitTestRaft.GetCommitIndex()))
	})

	It("No commit No Consensus", func() {
		cur := discovery.NodeList{
			New: []string{"nodeSelf", "node1", "node2", "node3", "node4"},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}
		matches := map[string]uint64{
			"node1": lastIndex,
			"node2": 1,
			"node3": 1,
			"node4": 1,
		}
		state := &raftState{
			peerMatches: matches,
		}
		newIndex := unitTestRaft.calculateCommitIndex(state, cfg)
		Expect(newIndex).Should(BeEquivalentTo(unitTestRaft.GetCommitIndex()))
	})

	It("Commit Consensus", func() {
		oldIndex := unitTestRaft.GetCommitIndex()
		defer unitTestRaft.setCommitIndex(oldIndex)
		unitTestRaft.setCommitIndex(lastIndex - 2)

		cur := discovery.NodeList{
			New: []string{"nodeSelf", "node1", "node2", "node3", "node4"},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}
		matches := map[string]uint64{
			"node1": lastIndex,
			"node2": lastIndex,
			"node3": lastIndex,
			"node4": 1,
		}
		state := &raftState{
			peerMatches: matches,
		}
		newIndex := unitTestRaft.calculateCommitIndex(state, cfg)
		Expect(newIndex).Should(BeEquivalentTo(lastIndex))
	})

	It("Commit Consensus Even", func() {
		oldIndex := unitTestRaft.GetCommitIndex()
		defer unitTestRaft.setCommitIndex(oldIndex)
		unitTestRaft.setCommitIndex(lastIndex - 2)

		cur := discovery.NodeList{
			New: []string{"nodeSelf", "node1", "node2", "node3"},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}
		matches := map[string]uint64{
			"node1": lastIndex - 2,
			"node2": lastIndex - 1,
			"node3": lastIndex,
		}
		state := &raftState{
			peerMatches: matches,
		}
		newIndex := unitTestRaft.calculateCommitIndex(state, cfg)
		Expect(newIndex).Should(BeEquivalentTo(lastIndex - 2))
	})

	It("Commit Consensus 2", func() {
		oldIndex := unitTestRaft.GetCommitIndex()
		defer unitTestRaft.setCommitIndex(oldIndex)
		unitTestRaft.setCommitIndex(lastIndex - 2)

		cur := discovery.NodeList{
			New: []string{"nodeSelf", "node1", "node2", "node3", "node4"},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}
		matches := map[string]uint64{
			"node1": 1,
			"node2": lastIndex,
			"node3": lastIndex - 1,
			"node4": lastIndex - 1,
		}
		state := &raftState{
			peerMatches: matches,
		}
		newIndex := unitTestRaft.calculateCommitIndex(state, cfg)
		Expect(newIndex).Should(BeEquivalentTo(lastIndex - 1))
	})

	It("Commit Joint Consensus", func() {
		oldIndex := unitTestRaft.GetCommitIndex()
		defer unitTestRaft.setCommitIndex(oldIndex)
		unitTestRaft.setCommitIndex(lastIndex - 2)

		cur := discovery.NodeList{
			New: []string{"nodeSelf", "node1", "node2", "node3", "node4", "node5"},
			Old: []string{"nodeSelf", "node1", "node2"},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}

		li, _ := unitTestRaft.GetLastIndex()
		fmt.Fprintf(GinkgoWriter, "Joint consensus. lastIndex = %d last applied = %d\n",
			lastIndex, li)
		matches := map[string]uint64{
			"node1": lastIndex - 2,
			"node2": lastIndex,
			"node3": lastIndex - 1,
			"node4": lastIndex - 1,
			"node5": lastIndex - 1,
		}
		state := &raftState{
			peerMatches: matches,
		}
		newIndex := unitTestRaft.calculateCommitIndex(state, cfg)
		fmt.Fprintf(GinkgoWriter, "Joint consensus. result = %d\n", newIndex)
		Expect(newIndex).Should(BeEquivalentTo(lastIndex - 2))
	})

	It("Commit Joint Consensus 2", func() {
		oldIndex := unitTestRaft.GetCommitIndex()
		defer unitTestRaft.setCommitIndex(oldIndex)
		unitTestRaft.setCommitIndex(lastIndex - 2)

		cur := discovery.NodeList{
			New: []string{"nodeSelf", "node1", "node2", "node3", "node4", "node5"},
			Old: []string{"nodeSelf", "node1", "node2"},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}
		matches := map[string]uint64{
			"node1": 1,
			"node2": lastIndex,
			"node3": lastIndex,
			"node4": lastIndex,
			"node5": lastIndex - 1,
		}
		state := &raftState{
			peerMatches: matches,
		}
		newIndex := unitTestRaft.calculateCommitIndex(state, cfg)
		Expect(newIndex).Should(BeEquivalentTo(lastIndex - 2))
	})

	/*
	 * Voting. Handle both joint and regular consensus.
	 */

	It("Vote several nodes", func() {
		cur := discovery.NodeList{
			New: []string{unitTestAddr, "node1", "node2"},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}

		// Majority of nodes, plus ourselves, voted yes.
		responses := []communication.VoteResponse{
			{NodeAddress: "node1", VoteGranted: true},
			{NodeAddress: "node2", VoteGranted: true},
		}
		granted := unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeTrue())

		responses = []communication.VoteResponse{
			{NodeAddress: "node1", VoteGranted: true},
			{NodeAddress: "node2", VoteGranted: false},
		}
		granted = unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeTrue())

		responses = []communication.VoteResponse{
			{NodeAddress: "node1", VoteGranted: false},
			{NodeAddress: "node2", VoteGranted: false},
		}
		granted = unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeFalse())

		responses = []communication.VoteResponse{
			{NodeAddress: "node1", Error: errors.New("Pow")},
			{NodeAddress: "node2", VoteGranted: false},
		}
		granted = unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeFalse())
	})

	It("Vote one node", func() {
		cur := discovery.NodeList{
			New: []string{unitTestAddr},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}

		responses := []communication.VoteResponse{}
		granted := unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeTrue())
	})

	It("Vote even nodes", func() {
		cur := discovery.NodeList{
			New: []string{unitTestAddr, "node1", "node2", "node3"},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}

		responses := []communication.VoteResponse{
			{NodeAddress: "node1", VoteGranted: true},
			{NodeAddress: "node2", VoteGranted: true},
			{NodeAddress: "node3", VoteGranted: true},
		}
		granted := unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeTrue())

		responses = []communication.VoteResponse{
			{NodeAddress: "node1", VoteGranted: true},
			{NodeAddress: "node2", VoteGranted: false},
			{NodeAddress: "node3", VoteGranted: true},
		}
		granted = unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeTrue())

		responses = []communication.VoteResponse{
			{NodeAddress: "node1", VoteGranted: true},
			{NodeAddress: "node2", VoteGranted: false},
			{NodeAddress: "node3", VoteGranted: false},
		}
		granted = unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeFalse())
	})

	It("Vote joint consensus", func() {
		cur := discovery.NodeList{
			New: []string{unitTestAddr, "node1", "node2", "node3", "node4", "node5"},
			Old: []string{unitTestAddr, "node1", "node2"},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}

		// Consensus from both clusters
		responses := []communication.VoteResponse{
			{NodeAddress: "node1", VoteGranted: true},
			{NodeAddress: "node2", VoteGranted: true},
			{NodeAddress: "node3", VoteGranted: true},
			{NodeAddress: "node4", VoteGranted: true},
			{NodeAddress: "node5", VoteGranted: true},
		}
		granted := unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeTrue())

		// Consensus from old but not new
		responses = []communication.VoteResponse{
			{NodeAddress: "node1", VoteGranted: true},
			{NodeAddress: "node2", VoteGranted: true},
			{NodeAddress: "node3", VoteGranted: false},
			{NodeAddress: "node4", VoteGranted: false},
			{NodeAddress: "node5", VoteGranted: false},
		}
		granted = unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeFalse())
	})

	It("Vote joint consensus no leader", func() {
		// Simulate a situation where we're the leader and we're leaving
		cur := discovery.NodeList{
			Old: []string{unitTestAddr, "node1", "node2", "node3", "node4"},
			New: []string{"node1", "node2", "node3"},
		}
		cfg := &discovery.NodeConfig{
			Current: &cur,
		}

		// Consensus from both clusters
		responses := []communication.VoteResponse{
			{NodeAddress: "node1", VoteGranted: true},
			{NodeAddress: "node2", VoteGranted: true},
			{NodeAddress: "node3", VoteGranted: true},
			{NodeAddress: "node4", VoteGranted: true},
		}
		granted := unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeTrue())

		// Old config had consensus but not new config
		responses = []communication.VoteResponse{
			{NodeAddress: "node1", VoteGranted: false},
			{NodeAddress: "node2", VoteGranted: false},
			{NodeAddress: "node3", VoteGranted: true},
			{NodeAddress: "node4", VoteGranted: true},
		}
		granted = unitTestRaft.countVotes(responses, cfg)
		Expect(granted).Should(BeFalse())
	})
})
