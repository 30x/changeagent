/*
 * Methods in this file handle the voting logic.
 */

package raft

import (
	"github.com/30x/changeagent/communication"
	"github.com/30x/changeagent/discovery"
	"github.com/golang/glog"
)

func (r *Service) handleFollowerVote(state *raftState, cmd voteCommand) bool {
	glog.V(2).Infof("Node %s got vote request from %d at term %d",
		r.id, cmd.vr.CandidateID, cmd.vr.Term)
	currentTerm := r.GetCurrentTerm()

	resp := communication.VoteResponse{
		NodeID: r.id,
		Term:   currentTerm,
	}

	// 5.1: Reply false if term < currentTerm
	if cmd.vr.Term < currentTerm {
		resp.VoteGranted = false
		cmd.rc <- resp
		return false
	}

	// Important to double-check state at this point as well since channels are buffered
	if r.GetState() != Follower {
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
		glog.V(2).Infof("Node %s voting for candidate %d", r.id, cmd.vr.CandidateID)
		resp.VoteGranted = true
	} else {
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
		// We won't always know our own address but if we do, skip
		if r.getNodeID(node) == r.id {
			votes++
			continue
		}
		rc := make(chan communication.VoteResponse)
		responseChannels = append(responseChannels, rc)
		r.comm.RequestVote(node, vr, rc)
	}

	// Pick up all the response channels. This will block until we get all responses.
	for _, respChan := range responseChannels {
		vresp := <-respChan
		// Record node id <-> address mapping in case we don't know
		r.addDiscoveredNode(vresp.NodeID, vresp.NodeAddress)
		if vresp.NodeID != r.id {
			responses = append(responses, vresp)
		}
	}

	// Calculate whether we have enough votes. Take leader changes (joint consensus) into account.
	granted := r.countVotes(responses, cfg)
	glog.Infof("Node %s: election request complete for term %d: Granted = %v", r.id, currentTerm, granted)

	finalResponse := voteResult{
		index:  index,
		result: granted,
	}
	rc <- finalResponse
}

func (r *Service) countVotes(responses []communication.VoteResponse, cfg *discovery.NodeConfig) bool {
	// Sort votes into a map so that we can process the rest.
	voteMap := make(map[string]bool)

	// Map votes from peers
	for _, resp := range responses {
		if resp.Error != nil {
			glog.V(2).Infof("Node %s: Error: %s", resp.NodeID, resp.Error)
			voteMap[resp.NodeAddress] = false
		} else if resp.VoteGranted {
			glog.V(2).Infof("Node %s: Voted yes", resp.NodeID)
			voteMap[resp.NodeAddress] = true
		} else {
			glog.V(2).Infof("Node %s: Voted no", resp.NodeID)
			voteMap[resp.NodeAddress] = false
		}
	}

	// Don't forget to vote for ourself!
	voteMap[r.getLocalAddress()] = true

	if len(cfg.Current.Old) > 0 {
		// Joint consensus mode. Need both sets of nodes to vote yes.
		if !countNodeListVotes(voteMap, cfg.Current.Old) {
			return false
		}
	}

	return countNodeListVotes(voteMap, cfg.Current.New)
}

func countNodeListVotes(voteMap map[string]bool, nodes []string) bool {
	votes := 0

	for _, node := range nodes {
		if voteMap[node] {
			votes++
		}
	}

	return votes >= ((len(nodes) / 2) + 1)
}
