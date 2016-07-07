/*
 * This file contains the main loops for leaders and followers. This loop
 * runs in a single goroutine per Raft.
 */

package raft

import (
	"errors"
	"fmt"
	"time"

	"github.com/30x/changeagent/common"
	"github.com/golang/glog"
)

type voteResult struct {
	index  uint64
	result bool
	err    error
}

type peerMatchResult struct {
	nodeID   common.NodeID
	newMatch uint64
}

type raftState struct {
	votedFor         common.NodeID
	voteIndex        uint64 // Keep track of the voting channel in case something takes a long time
	voteResults      chan voteResult
	peers            map[common.NodeID]*raftPeer
	peerMatches      map[common.NodeID]uint64
	peerMatchChanges chan peerMatchResult
}

func (r *Service) mainLoop() {
	state := &raftState{
		voteIndex:        0,
		voteResults:      make(chan voteResult, 1),
		votedFor:         r.readLastVote(),
		peers:            make(map[common.NodeID]*raftPeer),
		peerMatches:      make(map[common.NodeID]uint64),
		peerMatchChanges: make(chan peerMatchResult, 1),
	}

	var stopDone chan bool
	for {
		switch r.GetState() {
		case Follower:
			glog.Infof("Node %s entering follower mode", r.id)
			stopDone = r.followerLoop(false, state)
		case Candidate:
			glog.Infof("Node %s entering candidate mode", r.id)
			stopDone = r.followerLoop(true, state)
		case Leader, Standalone:
			glog.Infof("Node %s entering leader mode", r.id)
			stopDone = r.leaderLoop(state)
		case Stopping:
			r.cleanup()
			if stopDone != nil {
				stopDone <- true
			}
			glog.V(2).Infof("Node %s stop is complete", r.id)
			return
		case Stopped:
			return
		default:
			panic(fmt.Sprintf("Invalid state %s", r.GetState()))
		}
	}
}

func (r *Service) followerLoop(isCandidate bool, state *raftState) chan bool {
	if isCandidate {
		glog.V(2).Infof("Node %s starting an election", r.id)
		state.voteIndex++
		// Update term and vote for myself
		r.setCurrentTerm(r.GetCurrentTerm() + 1)
		state.votedFor = r.id
		r.writeLastVote(r.id)
		go r.sendVotes(state, state.voteIndex, state.voteResults)
	}

	timeout := time.NewTimer(r.randomElectionTimeout())
	lastSawLeader := time.Unix(0, 0)

	for {
		select {
		case <-timeout.C:
			glog.V(2).Infof("Node %s: election timeout", r.id)
			if !r.followerOnly {
				r.setState(Candidate)
				r.setLeader(0)
				return nil
			}

		case voteCmd := <-r.voteCommands:
			granted := r.handleFollowerVote(state, voteCmd, lastSawLeader)
			if granted {
				// After voting yes, wait for a timeout until voting again
				timeout.Reset(r.randomElectionTimeout())
			}

		case appendCmd := <-r.appendCommands:
			// 5.1: If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			glog.V(2).Infof("Processing append command from leader %s", appendCmd.ar.LeaderID)
			if appendCmd.ar.Term > r.GetCurrentTerm() {
				glog.Infof("Append request from new leader at new term %d", appendCmd.ar.Term)
				r.setCurrentTerm(appendCmd.ar.Term)
				state.votedFor = 0
				r.writeLastVote(0)
				r.setState(Follower)
				r.setLeader(appendCmd.ar.LeaderID)
			} else if r.GetLeaderID() == 0 {
				glog.Infof("Seeing new leader %s for the first time", appendCmd.ar.LeaderID)
				r.setLeader(appendCmd.ar.LeaderID)
			}
			r.handleAppend(state, appendCmd)
			lastSawLeader = time.Now()
			timeout.Reset(r.randomElectionTimeout())

		case prop := <-r.proposals:
			leader := r.getLeader()
			if leader.NodeID == 0 {
				pr := proposalResult{
					err: errors.New("Cannot accept proposal because there is no leader"),
				}
				prop.rc <- pr
			} else {
				go func() {
					glog.V(2).Infof("Forwarding proposal to leader node %s", leader.NodeID)
					pr := proposalResult{}

					fr, err := r.comm.Propose(leader.Address, &prop.entry)
					if err != nil {
						pr.err = err
					} else if fr.Error != nil {
						pr.err = fr.Error
					}
					pr.index = fr.NewIndex
					prop.rc <- pr
				}()
			}

		case vr := <-state.voteResults:
			if vr.index == state.voteIndex {
				// Avoid vote results that come back way too late
				state.votedFor = 0
				r.writeLastVote(0)
				glog.V(2).Infof("Node %s received the election result: %v", r.id, vr.result)
				if vr.result {
					r.setState(Leader)
					r.setLeader(0)
					return nil
				}

				// Voting failed. Try again after timeout.
				timeout.Reset(r.randomElectionTimeout())
				r.setState(Follower)
				r.setLeader(0)
				return nil
			}

		case si := <-r.statusInquiries:
			returnStatus(si, state, false)

		case cmd := <-r.loopCommands:
			glog.V(2).Infof("Ignoring loop command %s", cmd)

		case stopDone := <-r.stopChan:
			r.setState(Stopping)
			return stopDone
		}
	}
}

func (r *Service) leaderLoop(state *raftState) chan bool {
	// Get the list of nodes here from the current configuration.
	nodes := r.GetNodeConfig().GetUniqueNodes()
	for _, node := range nodes {
		state.peers[node.NodeID] = startPeer(node, r, state.peerMatchChanges)
		state.peerMatches[node.NodeID] = 0
	}

	// Upon election: send initial empty AppendEntries RPCs (heartbeat) to
	// each server; repeat during idle periods to prevent
	// election timeouts (§5.2)
	_, err := r.makeProposal(nil, state)
	if err != nil {
		// Not sure what else to do, so abort being the leader
		glog.Infof("Error when initially trying to become leader: %s", err)
		state.votedFor = 0
		r.writeLastVote(0)
		r.setState(Follower)
		stopPeers(state)
		return nil
	}

	for {
		select {
		case voteCmd := <-r.voteCommands:
			r.voteNo(state, voteCmd)

		case appendCmd := <-r.appendCommands:
			// 5.1: If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			if appendCmd.ar.Term > r.GetCurrentTerm() {
				glog.Infof("Append request from new leader at new term %d. No longer leader",
					appendCmd.ar.Term)
				// Potential race condition averted because only this goroutine updates term
				r.setCurrentTerm(appendCmd.ar.Term)
				state.votedFor = 0
				r.writeLastVote(0)
				r.setState(Follower)
				r.setLeader(appendCmd.ar.LeaderID)
				stopPeers(state)
				r.handleAppend(state, appendCmd)
				return nil
			}
			r.handleAppend(state, appendCmd)

		case prop := <-r.proposals:
			// First check the webhooks and reply immediately if this fails
			if prop.entry.Type >= 0 {
				err = r.invokeWebHooks(&prop.entry)
			} else {
				err = nil
			}

			var index uint64
			if err == nil {
				// If command received from client: append entry to local log,
				// respond after entry applied to state machine (§5.3)
				index, err = r.makeProposal(&prop.entry, state)
				if len(state.peers) == 0 {
					// Special handling for a stand-alone node
					r.setCommitIndex(index)
					r.applyCommittedEntries(index)
				}
			}
			pr := proposalResult{
				index: index,
				err:   err,
			}
			prop.rc <- pr

		case peerMatch := <-state.peerMatchChanges:
			// Got back a changed applied index from a peer. Decide if we have a commit and
			// process it if we do.
			state.peerMatches[peerMatch.nodeID] = peerMatch.newMatch
			newIndex := r.calculateCommitIndex(state, r.GetNodeConfig())
			if r.setCommitIndex(newIndex) {
				r.applyCommittedEntries(newIndex)
				for _, p := range state.peers {
					// Send another notification to each peer to reduce latency.
					p.poke()
				}
			}

		case si := <-r.statusInquiries:
			returnStatus(si, state, true)

		case cmd := <-r.loopCommands:
			glog.V(2).Infof("Received loop command %s", cmd)
			switch cmd {
			case UpdateConfiguration:
				r.updatePeerList(state)
			case JoinAsFollower:
				r.setState(Follower)
				return nil
			case JoinAsCandidate:
				r.setState(Candidate)
				return nil
			default:
				glog.V(2).Info("Ignoring command.")
			}

		case stopDone := <-r.stopChan:
			r.setState(Stopping)
			stopPeers(state)
			return stopDone
		}
	}
}

func stopPeers(state *raftState) {
	for _, p := range state.peers {
		p.stop()
	}
}

/*
 * Whenever configuration changes, go through the list of peers and see what we need to do.
 */
func (r *Service) updatePeerList(state *raftState) {
	cfg := r.GetNodeConfig()
	foundNodes := make(map[common.NodeID]bool)
	nodes := cfg.GetUniqueNodes()

	// Add any missing nodes
	for _, n := range nodes {
		foundNodes[n.NodeID] = true
		if state.peers[n.NodeID] == nil {
			glog.Infof("Starting communications with new node %s at %s", n.NodeID, n.Address)
			state.peers[n.NodeID] = startPeer(n, r, state.peerMatchChanges)
			state.peerMatches[n.NodeID] = 0
		}
	}

	// Delete any nodes that are no longer in the configuration
	for nid := range state.peers {
		if !foundNodes[nid] {
			glog.Infof("Stopping communications to deleted node %s", nid)
			state.peers[nid].stop()
			delete(state.peers, nid)
			delete(state.peerMatches, nid)
		}
	}
}

/*
 * Given the current position of a number of peers, calculate the commit index according
 * to the Raft spec. The result is the index that may be committed and propagated to the cluster.
 *
 * Use the following rules:
 *   If there exists an N such that N > commitIndex, a majority
 *   of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
 *
 * In addition, take into consideration joint consensus.
 * From section 6:
 *   Agreement (for elections and entry commitment) requires separate majorities
 *   from both the old and new configurations.
 */
func (r *Service) calculateCommitIndex(state *raftState, cfg *NodeList) uint64 {
	// Go through the current config and calcluate commit index by building a slice of nodes
	// and then sorting it. Keep in mind to include our index when we are part of the cluster.
	// Once we have the sorted list, just pick element N / 2 + 1 and we have our answer.
	// Don't forget to do that twice in the case of joint consensus.
	newIndex := r.getPartialCommitIndex(state, cfg.Current)

	if len(cfg.Next) > 0 {
		// Joint consensus. Pick the minimum of the two.
		oldIndex := r.getPartialCommitIndex(state, cfg.Next)
		if oldIndex < newIndex {
			newIndex = oldIndex
		}
	}

	// If we found a new index, see if we can get one that matches the term
	for ix := newIndex; ix > r.GetCommitIndex(); ix-- {
		entry, err := r.stor.GetEntry(ix)
		if err != nil {
			glog.V(2).Infof("Error reading entry from log: %v", err)
			break
		} else if entry != nil && entry.Term == r.GetCurrentTerm() {
			glog.V(2).Infof("Returning new commit index %d", ix)
			return ix
		}
	}
	return r.GetCommitIndex()
}

func (r *Service) getPartialCommitIndex(state *raftState, nodes []Node) uint64 {
	var indices []uint64
	for _, node := range nodes {
		if node.NodeID == r.MyID() {
			last, _ := r.GetLastIndex()
			indices = append(indices, last)
		} else {
			indices = append(indices, state.peerMatches[node.NodeID])
		}
	}

	if len(indices) == 0 {
		return 0
	}
	reverseSortUint64(indices)

	// Since indices are zero-based, this will return element N / 2 + 1
	p := len(indices) / 2
	return indices[p]
}

func returnStatus(ch chan<- ProtocolStatus, state *raftState, isLeader bool) {
	s := ProtocolStatus{}
	if isLeader {
		pis := make(map[common.NodeID]uint64)
		for k, v := range state.peerMatches {
			pis[k] = v
		}
		s.PeerIndices = &pis
	}
	ch <- s
}
