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
	Join(req JoinRequest) (uint64, error)
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
A JoinRequest is sent when we are trying to add a new node to the cluster.
We use it to send catch-up messages to the new node so that it will have a
copy of the log before we let it formally join the cluster.
*/
type JoinRequest struct {
	// ClusterID must be consistent on all calls
	ClusterID common.NodeID
	// Last indicates that there will be no more join requests
	Last bool
	// Entries are entries that are just replicated and inserted in storage
	Entries []common.Entry
	// ConfigEntries are entries that must be parsed and applied to local configuration
	ConfigEntries []common.Entry
}

/*
Communication is the interface that other modules use in order to communicate
with other nodes in the cluster.
*/
type Communication interface {
	// Close should be called to shut down any communication and close ports.
	Close()

	// Port returns the port number that the communication server is listening
	// on, or zero if it was configured to piggyback on an existing listener.
	Port() int

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

	// Join is called by the leader to add a new node to the cluster. There
	// may be multiple calls here if there are a large number of records
	// to send to the new node to catch it up.
	Join(address string, req JoinRequest) (ProposalResponse, error)
}
