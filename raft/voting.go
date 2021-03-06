/*
 * Methods in this file handle the voting logic.
 */

package raft

import (
	"time"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/communication"
	"github.com/golang/glog"
)

func (r *Service) handleFollowerVote(state *raftState, cmd voteCommand, lastSawLeader time.Time) bool {
	glog.V(2).Infof("Node %s got vote request from %s at term %d",
		r.id, cmd.vr.CandidateID, cmd.vr.Term)
	currentTerm := r.GetCurrentTerm()

	resp := communication.VoteResponse{
		NodeID: r.id,
		Term:   currentTerm,
	}

	// 5.1: Reply false if term < currentTerm
	if cmd.vr.Term < currentTerm {
		glog.V(2).Infof("Voting no because term %d less than current term %d", cmd.vr.Term, currentTerm)
		resp.VoteGranted = false
		cmd.rc <- resp
		return false
	}

	// Section 6: Ignore votes if we never reached election timeout.
	// Note that election timeout may have been reached even if we are not
	// a candidate.
	sinceSawLeader := time.Now().Sub(lastSawLeader)
	elTimeout := r.GetRaftConfig().ElectionTimeout()
	if sinceSawLeader < elTimeout {
		glog.V(2).Info("Voting no because we saw the leader %v ago", sinceSawLeader)
		resp.VoteGranted = false
		cmd.rc <- resp
		return false
	}

	// Important to double-check state at this point as well since channels are buffered
	if r.GetState() != Follower {
		glog.V(2).Infof("Voting no because our state is %s", r.GetState())
		resp.VoteGranted = false
		cmd.rc <- resp
		return false
	}

	// 5.2, 5.2: If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	commitIndex := r.GetCommitIndex()
	if (state.votedFor == 0 || state.votedFor == cmd.vr.CandidateID) &&
		cmd.vr.LastLogIndex >= commitIndex {
		state.votedFor = cmd.vr.CandidateID
		r.writeLastVote(cmd.vr.CandidateID)
		glog.V(2).Infof("Node %s voting for candidate %s", r.id, cmd.vr.CandidateID)
		resp.VoteGranted = true
	} else {
		glog.V(2).Infof("Voting no because we voted for %s or because index %d >= %d",
			state.votedFor, cmd.vr.LastLogIndex, commitIndex)
		resp.VoteGranted = false
	}
	cmd.rc <- resp
	return resp.VoteGranted
}

func (r *Service) voteNo(state *raftState, cmd voteCommand) {
	resp := communication.VoteResponse{
		NodeID:      r.id,
		Term:        r.GetCurrentTerm(),
		VoteGranted: false,
	}
	glog.V(2).Infof("Voting no because our state is %s", r.GetState())
	cmd.rc <- resp
}

func (r *Service) sendVotes(state *raftState, index uint64, rc chan<- voteResult) {
	lastIndex, lastTerm, err := r.stor.GetLastIndex()
	if err != nil {
		glog.Infof("Error reading database to start election: %v", err)
		dbErr := voteResult{
			index: index,
			err:   err,
		}
		rc <- dbErr
		return
	}

	currentTerm := r.GetCurrentTerm()
	vr := communication.VoteRequest{
		Term:         currentTerm,
		CandidateID:  r.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	// Get the current node list from the current config, and request votes.
	cfg := r.GetNodeConfig()
	allNodes := cfg.GetUniqueNodes()
	votes := 0
	glog.V(2).Infof("Node %s sending vote request to %d nodes for term %d",
		r.id, len(allNodes), currentTerm)

	var responseChannels []chan communication.VoteResponse
	var responses []communication.VoteResponse

	// Send off all the votes. Each will run in a goroutine and send us the result in a channel
	for _, node := range allNodes {
		// Skip ourselves!
		if node.NodeID == r.id {
			votes++
			continue
		}
		rc := make(chan communication.VoteResponse)
		responseChannels = append(responseChannels, rc)
		r.comm.RequestVote(node.Address, vr, rc)
	}

	// Pick up all the response channels. This will block until we get all responses.
	for _, respChan := range responseChannels {
		vresp := <-respChan
		if vresp.NodeID != r.id {
			responses = append(responses, vresp)
		}
	}

	// Calculate whether we have enough votes. Take leader changes (joint consensus) into account.
	granted := r.countVotes(responses, &cfg)
	glog.Infof("Node %s: election request complete for term %d: Granted = %v", r.id, currentTerm, granted)

	finalResponse := voteResult{
		index:  index,
		result: granted,
	}
	rc <- finalResponse
}

func (r *Service) countVotes(responses []communication.VoteResponse, cfg *NodeList) bool {
	// Sort votes into a map so that we can process the rest.
	voteMap := make(map[common.NodeID]bool)

	// Map votes from peers
	for _, resp := range responses {
		if resp.Error != nil {
			glog.V(2).Infof("Node %s: Error: %s", resp.NodeID, resp.Error)
			voteMap[resp.NodeID] = false
		} else if resp.VoteGranted {
			glog.V(2).Infof("Node %s: Voted yes", resp.NodeID)
			voteMap[resp.NodeID] = true
		} else {
			glog.V(2).Infof("Node %s: Voted no", resp.NodeID)
			voteMap[resp.NodeID] = false
		}
	}

	// Don't forget to vote for ourself!
	voteMap[r.id] = true

	if len(cfg.Next) > 0 {
		// Joint consensus mode. Need both sets of nodes to vote yes.
		if !countNodeListVotes(voteMap, cfg.Next) {
			return false
		}
	}

	return countNodeListVotes(voteMap, cfg.Current)
}

func countNodeListVotes(voteMap map[common.NodeID]bool, nodes []Node) bool {
	votes := 0

	for _, node := range nodes {
		if voteMap[node.NodeID] {
			votes++
		}
	}

	return votes >= ((len(nodes) / 2) + 1)
}
