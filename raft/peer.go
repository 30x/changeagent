/*
 * This file represents the communication from the raft leader to all its
 * peers. We maintain one goroutine per peer, which we use to send
 * all heartbeats and appends, and to keep track of peer state.
 */

package raft

import (
	"time"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/communication"
	"github.com/golang/glog"
)

const (
	maxPeerBatchSize = 64
)

type rpcResponse struct {
	success   bool
	heartbeat bool
	newIndex  uint64
	err       error
}

type raftPeer struct {
	node       Node
	r          *Service
	proposals  chan uint64
	pokes      chan bool
	changeChan chan<- peerMatchResult
	stopChan   chan bool
}

func startPeer(n Node, r *Service, changes chan<- peerMatchResult) *raftPeer {
	p := &raftPeer{
		node:       n,
		r:          r,
		proposals:  make(chan uint64, 1000),
		pokes:      make(chan bool, 1),
		changeChan: changes,
		stopChan:   make(chan bool, 1),
	}
	go p.peerLoop()
	return p
}

func (p *raftPeer) stop() {
	p.stopChan <- true
}

func (p *raftPeer) propose(ix uint64) {
	p.proposals <- ix
}

// Wake up the loop and send another heartbeat with a new commit index
func (p *raftPeer) poke() {
	p.pokes <- true
}

func (p *raftPeer) peerLoop() {
	// Next index that we know that we need to send to this peer. Start from the end
	// of the database. This may be decremented if the peer is behind.
	nextIndex, _ := p.r.GetLastIndex()
	// The index, that we expect the peer to be caught up to eventually. Starts
	// at nextIndex, but will be incremented when more data needs to be replicated.
	desiredIndex := nextIndex

	// Send HTTP requests asynchronously and notify this channel when they complete
	responseChan := make(chan rpcResponse, 1)
	// Only send one message at a time
	rpcRunning := false

	// Repeat heartbeats during idle periods to prevent
	// election timeouts (§5.2)
	hbTimeout := time.NewTimer(HeartbeatTimeout)

	// Provide a way to delay on failure
	failureDelay := false

	for {
		glog.V(2).Infof("Peer %s: next = %d desired = %d", p.node, nextIndex, desiredIndex)

		if !rpcRunning && !failureDelay && (desiredIndex > nextIndex) {
			nextDesired := desiredIndex
			if (desiredIndex - nextIndex) > maxPeerBatchSize {
				nextDesired = nextIndex + maxPeerBatchSize
			}
			err := p.sendUpdates(nextDesired, nextIndex, responseChan)
			if err != nil {
				glog.V(2).Infof("Error sending updates to peer %s: %s", p.node, err)
			}
			rpcRunning = true
			hbTimeout.Reset(HeartbeatTimeout)
		}

		select {
		case <-hbTimeout.C:
			// Heartbeat timer expired. Send a heartbeat so that a new leader is not elected.
			// Also cancel the built-in failure delay.
			failureDelay = false
			if !rpcRunning && (nextIndex == desiredIndex) {
				p.sendHeartbeat(desiredIndex, responseChan)
				rpcRunning = true
			}
			hbTimeout.Reset(HeartbeatTimeout)

		case <-p.pokes:
			// The main loop asked us to send an early heartbeat because the commit index changed
			if !rpcRunning && (nextIndex == desiredIndex) && !failureDelay {
				p.sendHeartbeat(desiredIndex, responseChan)
				rpcRunning = true
				hbTimeout.Reset(HeartbeatTimeout)
			}

		case newIndex := <-p.proposals:
			// A new proposal was pushed to us from the main loop
			desiredIndex = newIndex

		case response := <-responseChan:
			// We got back an RPC response
			rpcRunning = false
			if response.err == nil {
				nextIndex = p.handleRPCResult(response)
			} else {
				glog.V(2).Infof("Error from peer: %s", response.err)
				failureDelay = true
			}

		case <-p.stopChan:
			glog.V(2).Infof("Peer at %s stopping", p.node)
			return
		}
	}
}

func (p *raftPeer) handleRPCResult(resp rpcResponse) uint64 {
	glog.V(2).Infof("Got RPC result. Success = %v index = %d", resp.success, resp.newIndex)
	if resp.success && !resp.heartbeat {
		// If successful: update nextIndex and matchIndex for
		// follower (§5.3)
		newIndex := resp.newIndex
		glog.V(2).Infof("Peer at %s now up to date with index %d", p.node, resp.newIndex)

		// Send a notification back to the main loop so it can update its own indices.
		change := peerMatchResult{
			nodeID:   p.node.NodeID,
			newMatch: newIndex,
		}
		p.changeChan <- change
		return newIndex

	} else if !resp.success {
		// If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)
		newNext := resp.newIndex
		if newNext > 0 {
			newNext--
			glog.V(2).Infof("Decrementing next index to %d", newNext)
		}
		return newNext

	} else {
		return resp.newIndex
	}
}

/*
 * Send a heartbeat to the peer, which is a set of updates with no entries.
 */
func (p *raftPeer) sendHeartbeat(index uint64, rc chan rpcResponse) {
	lastIndex, lastTerm := p.r.GetLastIndex()
	ar := communication.AppendRequest{
		Term:         p.r.GetCurrentTerm(),
		LeaderID:     p.r.id,
		PrevLogIndex: lastIndex,
		PrevLogTerm:  lastTerm,
		LeaderCommit: p.r.GetCommitIndex(),
	}

	glog.V(2).Infof("Sending heartbeat for index %d to peer at %s", index, p.node)

	go func() {
		success, err := p.r.sendAppend(p.node.Address, ar)
		rpcResp := rpcResponse{
			success:   success,
			newIndex:  index,
			heartbeat: true,
			err:       err,
		}
		rc <- rpcResp
	}()
}

/*
 * Send a set of updates to the peer, starting at "next" and going until "desired."
 * The response will be placed on the "rpcResponse" channel.
 */
func (p *raftPeer) sendUpdates(desired uint64, next uint64, rc chan rpcResponse) error {
	// If last log index ≥ nextIndex for a follower: send AppendEntries RPC
	// with log entries starting at nextIndex
	var start uint64
	if next == 0 {
		start = 0
	} else {
		start = next - 1
	}

	glog.V(2).Infof("sendUpdates: start = %d desired = %d", start, desired)

	// Get all the entries from (start - 1).
	allEntries, err := p.r.stor.GetEntries(start, uint(desired-start),
		func(e *common.Entry) bool { return true })
	if err != nil {
		glog.V(2).Infof("Error getting entries for peer at %s: %v", p.node, err)
		return err
	}
	glog.V(2).Infof("Got %d updates", len(allEntries))

	var lastIndex, lastTerm uint64
	var sendEntries []common.Entry

	// Unless we are starting from 0, the first entry in the list just tells us what the
	// lastIndex and lastTerm should be. The rest are the entries that we actually
	// want to send.
	if next == 0 {
		lastIndex = 0
		lastTerm = 0
		sendEntries = allEntries
	} else {
		lastIndex = allEntries[0].Index
		lastTerm = allEntries[0].Term
		sendEntries = allEntries[1:]
	}

	glog.V(2).Infof("Sending %d entries between %d and %d to %s",
		len(sendEntries), next, desired, p.node)

	ar := communication.AppendRequest{
		Term:         p.r.GetCurrentTerm(),
		LeaderID:     p.r.id,
		PrevLogIndex: lastIndex,
		PrevLogTerm:  lastTerm,
		LeaderCommit: p.r.GetCommitIndex(),
		Entries:      sendEntries,
	}

	// Do the send in a new goroutine because it will block. The main loop will continue
	// and won't do anything else until the response comes back.
	go func() {
		success, err := p.r.sendAppend(p.node.Address, ar)
		newIndex := next
		if success {
			newIndex = desired
		}
		rpcResp := rpcResponse{
			success:  success,
			newIndex: newIndex,
			err:      err,
		}
		rc <- rpcResp
	}()

	return nil
}
