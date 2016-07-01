package communication

import (
	"fmt"

	"github.com/30x/changeagent/common"
)

/*
Raft is the interface that a Raft implementation must implement so that this
module can call it back when it gets various events over the network.
*/
type Raft interface {
	MyID() common.NodeID
	RequestVote(req VoteRequest) (VoteResponse, error)
	Append(req AppendRequest) (AppendResponse, error)
	Propose(e *common.Entry) (uint64, error)
}

/*
A VoteRequest is the message that a raft node sends when it wants to be elected
the leader.
*/
type VoteRequest struct {
	Term         uint64
	CandidateID  common.NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
	ClusterID    common.NodeID
}

/*
A VoteResponse is the response to a VoteRequest.
*/
type VoteResponse struct {
	NodeID      common.NodeID
	NodeAddress string
	Term        uint64
	VoteGranted bool
	Error       error
}

/*
An AppendRequest is the message that the leader sends when it wants to append
a new record to the raft log of another node.
*/
type AppendRequest struct {
	Term         uint64
	LeaderID     common.NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64
	Entries      []common.Entry
}

func (a *AppendRequest) String() string {
	s := fmt.Sprintf("AppendRequest{ Term: %d Leader: %d PrevIx: %d PrevTerm: %d LeaderCommit: %d [",
		a.Term, a.LeaderID, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit)
	for _, e := range a.Entries {
		s += e.String()
	}
	s += " ]}"
	return s
}

/*
An AppendResponse is the repsonse to an AppendRequest.
*/
type AppendResponse struct {
	Term        uint64
	Success     bool
	CommitIndex uint64
	Error       error
}

/*
DefaultAppendResponse is a convenient empty response.
*/
var DefaultAppendResponse = AppendResponse{}

func (a *AppendResponse) String() string {
	s := fmt.Sprintf("AppendResponse{ Term: %d Success: %v CommitIndex: %d ",
		a.Term, a.Success, a.CommitIndex)
	if a.Error != nil {
		s += fmt.Sprintf("Error: %s ", a.Error)
	}
	s += " }"
	return s
}

/*
A ProposalResponse is the response to the proposal of a new storage
entry. It is used when a non-leader node wishes to ask the leader
to propose something.
*/
type ProposalResponse struct {
	NewIndex uint64
	Error    error
}

/*
DefaultProposalResponse is a convenient empty response.
*/
var DefaultProposalResponse = ProposalResponse{}

func (a *ProposalResponse) String() string {
	s := fmt.Sprintf("ProposalResponse{ NewIndex: %d ", a.NewIndex)
	if a.Error != nil {
		s += fmt.Sprintf("Error: %s ", a.Error)
	}
	s += " }"
	return s
}

/*
Communication is the interface that other modules use in order to communicate
with other nodes in the cluster.
*/
type Communication interface {
	// SetRaft must be called to wire up the communications module before anything
	// else may be called.
	SetRaft(raft Raft)

	// Discover may be called by any node to discover the unique ID of another
	// node.
	Discover(address string) (common.NodeID, error)

	// RequestVote is called by a candidate when it wishes to be elected.
	// The response will be delivered asynchronously via a channel.
	RequestVote(address string, req VoteRequest, ch chan<- VoteResponse)

	// Append is called by the leader to add a new entry to the log of another
	// node. It blocks until it gets a response.
	Append(address string, req AppendRequest) (AppendResponse, error)

	// Propose is called by a non-leader node to ask the leader to propose a new
	// change to its followers. It blocks until it gets a response.
	Propose(address string, e *common.Entry) (ProposalResponse, error)
}
