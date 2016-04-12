package raft

import (
  "errors"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/storage"
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

    ar := &communication.AppendRequest{
      Term: 2,
      LeaderID: 1,
      PrevLogIndex: 0,
      PrevLogTerm: 0,
      LeaderCommit: 1,
      Entries: []storage.Entry{
        storage.Entry{
          Index: 1,
          Term: 1,
        },
        storage.Entry{
          Index: 2,
          Term: 2,
        },
        storage.Entry{
          Index: 3,
          Term: 2,
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
    req := &communication.VoteRequest{
      Term: 1,
      CandidateID: 2,
      LastLogIndex: 3,
      LastLogTerm: 2,
    }

    resp, err := unitTestRaft.RequestVote(req)
    Expect(err).Should(Succeed())
    Expect(resp.VoteGranted).Should(BeFalse())
  })

  It("Vote Out Of Date", func() {
    // If votedFor is null or candidateId, and candidate’s log is at
    // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
    req := &communication.VoteRequest{
      Term: 1,
      CandidateID: 2,
      LastLogIndex: 2,
      LastLogTerm: 2,
    }

    resp, err := unitTestRaft.RequestVote(req)
    Expect(err).Should(Succeed())
    Expect(resp.VoteGranted).Should(BeFalse())
  })

  It("Voting", func() {
    // Test valid voting, and that we keep track of who we voted for
    req := &communication.VoteRequest{
      Term: 3,
      CandidateID: 2,
      LastLogIndex: 3,
      LastLogTerm: 2,
    }

    resp, err := unitTestRaft.RequestVote(req)
    Expect(err).Should(Succeed())
    Expect(resp.VoteGranted).Should(BeTrue())

    req = &communication.VoteRequest{
      Term: 3,
      CandidateID: 2,
      LastLogIndex: 3,
      LastLogTerm: 2,
    }

    resp, err = unitTestRaft.RequestVote(req)
    Expect(err).Should(Succeed())
    Expect(resp.VoteGranted).Should(BeTrue())

    req = &communication.VoteRequest{
      Term: 3,
      CandidateID: 3,
      LastLogIndex: 3,
      LastLogTerm: 2,
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
    req := &communication.AppendRequest{
      Term: 1,
      LeaderID: 1,
    }
    resp, err := unitTestRaft.Append(req)
    Expect(err).Should(Succeed())
    Expect(resp.Success).Should(BeFalse())
  })

  It("Log No Match", func() {
    // Reply false if log doesn’t contain an entry at prevLogIndex
    // whose term matches prevLogTerm (§5.3)
    req := &communication.AppendRequest{
      Term: 1,
      LeaderID: 1,
      PrevLogIndex: 10,
      PrevLogTerm: 2,
    }
    resp, err := unitTestRaft.Append(req)
    Expect(err).Should(Succeed())
    Expect(resp.Success).Should(BeFalse())
  })

  It("Log No Match Term", func() {
    req := &communication.AppendRequest{
      Term: 1,
      LeaderID: 1,
      PrevLogIndex: 1,
      PrevLogTerm: 2,
    }
    resp, err := unitTestRaft.Append(req)
    Expect(err).Should(Succeed())
    Expect(resp.Success).Should(BeFalse())
  })

  It("Append", func() {
    // Append any new entries not already in the log
    req := &communication.AppendRequest{
      Term: 2,
      LeaderID: 1,
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
    req = &communication.AppendRequest{
      Term: 3,
      LeaderID: 1,
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
    req = &communication.AppendRequest{
      Term: 3,
      LeaderID: 1,
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
    req = &communication.AppendRequest{
      Term: 3,
      LeaderID: 1,
      LeaderCommit: 3,
    }
    resp, err = unitTestRaft.Append(req)
    Expect(err).Should(Succeed())
    Expect(resp.Success).Should(BeTrue())
    Expect(resp.CommitIndex).Should(BeEquivalentTo(3))

    req = &communication.AppendRequest{
      Term: 3,
      LeaderID: 1,
      LeaderCommit: 5,
      Entries: []storage.Entry{
        storage.Entry{
          Term: 3,
          Index: 7,
        },
      },
    }
    resp, err = unitTestRaft.Append(req)
    Expect(err).Should(Succeed())
    Expect(resp.Success).Should(BeTrue())
    Expect(resp.CommitIndex).Should(BeEquivalentTo(5))

    req = &communication.AppendRequest{
      Term: 3,
      LeaderID: 1,
      LeaderCommit: 99,
      Entries: []storage.Entry{
        storage.Entry{
          Term: 3,
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
    Expect(newIndex).Should(BeEquivalentTo(unitTestRaft.GetCommitIndex()))
  })

  It("No commit No Consensus", func() {
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
    Expect(newIndex).Should(BeEquivalentTo(unitTestRaft.GetCommitIndex()))
  })

  It("Commit Consensus", func() {
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
    Expect(newIndex).Should(BeEquivalentTo(lastIndex))
  })

  It("Commit Consensus 2", func() {
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
    Expect(newIndex).Should(BeEquivalentTo(lastIndex - 1))
  })

  /*
   * Voting. Handle both joint and regular consensus.
   */

  nodeSelf := discovery.Node{
    ID: 1,
  }
  node1 := discovery.Node{
    ID: 10,
  }
  node2 := discovery.Node{
    ID: 20,
  }
  node3 := discovery.Node{
    ID: 30,
  }
  node4 := discovery.Node{
    ID: 40,
  }
  node5 := discovery.Node{
    ID: 50,
  }

  It("Vote several nodes", func() {
    cur := discovery.NodeList{
      New: []discovery.Node{nodeSelf, node1, node2},
    }
    cfg := &discovery.NodeConfig{
      Current: &cur,
    }

    // Majority of nodes, plus ourselves, voted yes.
    responses := []communication.VoteResponse{
      {NodeID: 10, VoteGranted: true},
      {NodeID: 20, VoteGranted: true},
    }
    granted := unitTestRaft.countVotes(responses, cfg)
    Expect(granted).Should(BeTrue())

    responses = []communication.VoteResponse{
      {NodeID: 10, VoteGranted: true},
      {NodeID: 20, VoteGranted: true},
    }
    granted = unitTestRaft.countVotes(responses, cfg)
    Expect(granted).Should(BeTrue())

    responses = []communication.VoteResponse{
      {NodeID: 10, VoteGranted: false},
      {NodeID: 20, VoteGranted: false},
    }
    granted = unitTestRaft.countVotes(responses, cfg)
    Expect(granted).Should(BeFalse())

    responses = []communication.VoteResponse{
      {NodeID: 10, Error: errors.New("Pow")},
      {NodeID: 20, VoteGranted: false},
    }
    granted = unitTestRaft.countVotes(responses, cfg)
    Expect(granted).Should(BeFalse())
  })

  It("Vote one node", func() {
    cur := discovery.NodeList{
      New: []discovery.Node{nodeSelf},
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
      New: []discovery.Node{nodeSelf, node1, node2, node3},
    }
    cfg := &discovery.NodeConfig{
      Current: &cur,
    }

    responses := []communication.VoteResponse{
      {NodeID: 10, VoteGranted: true},
      {NodeID: 20, VoteGranted: true},
      {NodeID: 30, VoteGranted: true},
    }
    granted := unitTestRaft.countVotes(responses, cfg)
    Expect(granted).Should(BeTrue())

    responses = []communication.VoteResponse{
      {NodeID: 10, VoteGranted: true},
      {NodeID: 20, VoteGranted: false},
      {NodeID: 30, VoteGranted: true},
    }
    granted = unitTestRaft.countVotes(responses, cfg)
    Expect(granted).Should(BeTrue())

    responses = []communication.VoteResponse{
      {NodeID: 10, VoteGranted: true},
      {NodeID: 20, VoteGranted: false},
      {NodeID: 30, VoteGranted: false},
    }
    granted = unitTestRaft.countVotes(responses, cfg)
    Expect(granted).Should(BeFalse())
  })

  It("Vote joint consensus", func() {
    cur := discovery.NodeList{
      New: []discovery.Node{nodeSelf, node1, node2, node3, node4, node5},
      Old: []discovery.Node{nodeSelf, node1, node2},
    }
    cfg := &discovery.NodeConfig{
      Current: &cur,
    }

    // Consensus from both clusters
    responses := []communication.VoteResponse{
      {NodeID: 10, VoteGranted: true},
      {NodeID: 20, VoteGranted: true},
      {NodeID: 30, VoteGranted: true},
      {NodeID: 40, VoteGranted: true},
      {NodeID: 50, VoteGranted: true},
    }
    granted := unitTestRaft.countVotes(responses, cfg)
    Expect(granted).Should(BeTrue())

    // Consensus from old but not new
    responses = []communication.VoteResponse{
      {NodeID: 10, VoteGranted: true},
      {NodeID: 20, VoteGranted: true},
      {NodeID: 30, VoteGranted: false},
      {NodeID: 40, VoteGranted: false},
      {NodeID: 50, VoteGranted: false},
    }
    granted = unitTestRaft.countVotes(responses, cfg)
    Expect(granted).Should(BeFalse())
  })

  It("Vote joint consensus no leader", func() {
    // Simulate a situation where we're the leader and we're leaving
    cur := discovery.NodeList{
      Old: []discovery.Node{nodeSelf, node1, node2, node3, node4},
      New: []discovery.Node{node1, node2, node3},
    }
    cfg := &discovery.NodeConfig{
      Current: &cur,
    }

    // Consensus from both clusters
    responses := []communication.VoteResponse{
      {NodeID: 10, VoteGranted: true},
      {NodeID: 20, VoteGranted: true},
      {NodeID: 30, VoteGranted: true},
      {NodeID: 40, VoteGranted: true},
    }
    granted := unitTestRaft.countVotes(responses, cfg)
    Expect(granted).Should(BeTrue())

    // Old config had consensus but not new config
    responses = []communication.VoteResponse{
      {NodeID: 10, VoteGranted: false},
      {NodeID: 20, VoteGranted: false},
      {NodeID: 30, VoteGranted: true},
      {NodeID: 40, VoteGranted: true},
    }
    granted = unitTestRaft.countVotes(responses, cfg)
    Expect(granted).Should(BeFalse())
  })
})
