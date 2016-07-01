package raft

import (
	"fmt"

	"github.com/30x/changeagent/communication"
	"github.com/golang/protobuf/proto"
)

//go:generate protoc --go_out=. nodelist.proto

/*
A Node represents a single node in the cluster. It has a unique ID as well
as a network address.
*/
type Node struct {
	NodeID  communication.NodeID
	Address string
}

func (n Node) String() string {
	return fmt.Sprintf("Node{%s: %s}", n.NodeID, n.Address)
}

/*
A NodeList is simply a list of nodes. For the purposes of joint consensus, it
a list of "current" nodes (which are currently running) and an optional list
of "next" nodes, which are subject to joint consensus.
*/
type NodeList struct {
	Current []Node
	Next    []Node
}

func decodeNodeList(buf []byte) (*NodeList, error) {
	var nl NodeListPb
	err := proto.Unmarshal(buf, &nl)
	if err != nil {
		return nil, err
	}

	l := NodeList{}
	for _, pn := range nl.GetCurrent() {
		n := Node{
			NodeID:  communication.NodeID(pn.GetNodeId()),
			Address: pn.GetAddress(),
		}
		l.Current = append(l.Current, n)
	}
	for _, pn := range nl.GetNext() {
		n := Node{
			NodeID:  communication.NodeID(pn.GetNodeId()),
			Address: pn.GetAddress(),
		}
		l.Next = append(l.Next, n)
	}
	return &l, nil
}

func (nl *NodeList) getUniqueNodes() []Node {
	var l []Node
	ids := make(map[communication.NodeID]bool)
	for _, n := range nl.Current {
		if !ids[n.NodeID] {
			l = append(l, n)
			ids[n.NodeID] = true
		}
	}
	for _, n := range nl.Next {
		if !ids[n.NodeID] {
			l = append(l, n)
			ids[n.NodeID] = true
		}
	}
	return l
}

func (nl *NodeList) getNode(id communication.NodeID) *Node {
	for _, n := range nl.Current {
		if n.NodeID == id {
			return &n
		}
	}
	for _, n := range nl.Next {
		if n.NodeID == id {
			return &n
		}
	}
	return nil
}

func (nl *NodeList) encode() []byte {
	np := NodeListPb{}
	for _, n := range nl.Current {
		pn := NodePb{
			NodeId:  proto.Uint64(uint64(n.NodeID)),
			Address: proto.String(n.Address),
		}
		np.Current = append(np.Current, &pn)
	}
	for _, n := range nl.Next {
		pn := NodePb{
			NodeId:  proto.Uint64(uint64(n.NodeID)),
			Address: proto.String(n.Address),
		}
		np.Next = append(np.Next, &pn)
	}
	buf, err := proto.Marshal(&np)
	if err != nil {
		panic(err.Error())
	}
	return buf
}

func (nl *NodeList) String() string {
	s := "NodeList{Current:"
	for i, n := range nl.Current {
		if i > 0 {
			s += ", "
		}
		s += n.Address
	}
	s += ", Next:"
	for i, n := range nl.Next {
		if i > 0 {
			s += ", "
		}
		s += n.Address
	}
	s += "}"
	return s
}
