package raft

import (
	"fmt"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/communication"
	"github.com/golang/glog"
)

const (
	// NodeProposalTimeout is how long to wait for config change to work
	NodeProposalTimeout = ElectionTimeout * 2
)

/*
InitializeCluster sets the node up to be able to add nodes to a cluster.
It should be called once and only once on the first node in a cluster.
After it has been called, it is possible to call AddNode to add more nodes.

The "address" parameter is the network address in host:port format that other
nodes should use to contact this node. It should not be a "localhost" address
unless the whole cluster runs on localhost. The address will be sent to the
other nodes in the cluster which is why it needs to be an address that they
can reach.
*/
func (r *Service) InitializeCluster(addr string) error {
	if r.GetClusterID() != 0 {
		return fmt.Errorf("Node already part of cluster %s and may not start a new one", r.GetClusterID())
	}

	nodeID, err := r.comm.Discover(addr)
	if err != nil {
		return fmt.Errorf("Cannot call back to our own address at %s: %s", addr, err)
	}
	if nodeID != r.id {
		return fmt.Errorf("Node at %s returns ID %s and not the proper answer of %s",
			addr, nodeID, r.id)
	}

	id := randomInt64()
	err = r.stor.SetUintMetadata(ClusterIDKey, uint64(id))
	if err != nil {
		return err
	}
	r.setClusterID(common.NodeID(id))
	r.setState(Leader)

	cfg := &NodeList{
		Current: []Node{{Address: addr, NodeID: r.id}},
	}
	r.setNodeConfig(cfg)

	r.loopCommands <- JoinAsCandidate

	glog.Infof("Node %s is now the leader of its own cluster %s", r.id, r.GetClusterID())
	return nil
}

/*
AddNode starts the process to add a new node to the cluster. It does this by
creating a new membership list, and then proposing it to the cluster.
*/
func (r *Service) AddNode(addr string) error {
	if r.GetClusterID() == 0 {
		return fmt.Errorf("Cluster mode must be initialized first")
	}
	cm := r.GetMembershipChangeMode()
	if cm != Stable {
		return fmt.Errorf("Prior membership change not complete. Mode = %s", cm)
	}

	glog.V(2).Infof("Discovering node at %s", addr)
	nodeID, err := r.comm.Discover(addr)
	if err != nil {
		return fmt.Errorf("Error discovering new node at %s: %s", addr, err)
	}

	if r.GetNodeConfig().GetNode(nodeID) != nil {
		return fmt.Errorf("Node %s is already part of cluster %s", nodeID, r.GetClusterID())
	}

	glog.V(2).Infof("Catching node %s up with existing data", nodeID)
	err = r.catchUpNode(addr)
	if err != nil {
		return fmt.Errorf("Error catching up node %s: %s", nodeID, err)
	}

	glog.V(2).Infof("Proposing node %s as a new member", nodeID)

	cfg := r.GetNodeConfig()
	newCfg := &NodeList{
		Current: cfg.Current,
		Next:    cfg.Current,
	}
	newNode := Node{
		Address: addr,
		NodeID:  nodeID,
	}
	newCfg.Next = append(newCfg.Next, newNode)

	proposedEntry := common.Entry{
		Type: MembershipChange,
		Data: newCfg.encode(),
	}

	glog.V(2).Infof("Proposing joint configuration: %s", newCfg)
	ix, err := r.Propose(&proposedEntry)
	if err != nil {
		return err
	}

	appliedIx := r.appliedTracker.TimedWait(ix, NodeProposalTimeout)
	if appliedIx < ix {
		// TODO re-propose the original membership so we don't get stuck
		return fmt.Errorf("Cannot apply the membership change to a quorum")
	}

	finalCfg := &NodeList{
		Current: newCfg.Next,
	}

	proposedEntry = common.Entry{
		Type: MembershipChange,
		Data: finalCfg.encode(),
	}

	glog.V(2).Infof("Proposing final configuration: %s", newCfg)
	ix, err = r.Propose(&proposedEntry)
	if err != nil {
		return err
	}

	appliedIx = r.appliedTracker.TimedWait(ix, NodeProposalTimeout)
	if appliedIx < ix {
		// TODO re-propose the original membership so we don't get stuck
		return fmt.Errorf("Cannot apply the membership change to a quorum")
	}

	return nil
}

func (r *Service) catchUpNode(addr string) error {
	var lastIx uint64
	joinCount := 0

	for {
		entries, err := r.stor.GetEntries(lastIx, maxPeerBatchSize,
			func(e *common.Entry) bool {
				return true
			})
		if err != nil {
			return err
		}
		glog.V(2).Infof("Got back %d entries to join from %d", len(entries), lastIx)

		if len(entries) > 0 {
			joinReq := communication.JoinRequest{
				ClusterID: r.GetClusterID(),
				Entries:   entries,
			}

			glog.V(2).Infof("Sending %d entries to %s to join cluster", len(entries), addr)

			joinResp, err := r.comm.Join(addr, joinReq)
			if err != nil {
				return err
			}
			if joinResp.Error != nil {
				return joinResp.Error
			}
			joinCount += len(entries)
			lastIx = entries[len(entries)-1].Index
		} else {
			break
		}
	}

	joinReq := communication.JoinRequest{
		ClusterID: r.GetClusterID(),
		Last:      true,
	}

	joinResp, err := r.comm.Join(addr, joinReq)
	if err != nil {
		return err
	}
	if joinResp.Error != nil {
		return joinResp.Error
	}
	glog.V(2).Infof("Node at %s now caught up to %d", addr, joinResp.NewIndex)

	glog.Infof("Caught up node at %s with %d records", addr, joinCount)
	return nil
}

func (r *Service) applyMembershipChange(e *common.Entry) {
	newCfg, err := decodeNodeList(e.Data)
	if err != nil {
		glog.Errorf("Received an invalid membership list: %s", err)
		return
	}

	glog.Infof("Applying new node configuration %s", newCfg)

	r.setNodeConfig(newCfg)
	r.loopCommands <- UpdateConfiguration
}
