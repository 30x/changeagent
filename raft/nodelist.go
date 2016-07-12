package raft

import (
	"fmt"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/protobufs"
	"github.com/golang/protobuf/proto"
)

/*
A Node represents a single node in the cluster. It has a unique ID as well
as a network address.
*/
type Node struct {
	NodeID  common.NodeID
	Address string
}

func (n Node) String() string {
	return fmt.Sprintf("Node{%s: %s}", n.NodeID, n.Address)
}

/*
MarshalJSON creates the JSON for this object because otherwise the built-in
encoder does not encode the NodeID properly to string.
*/
func (n Node) MarshalJSON() ([]byte, error) {
	s := fmt.Sprintf("{\"id\":\"%s\", \"address\":\"%s\"}",
		n.NodeID, n.Address)
	return []byte(s), nil
}

/*
A NodeList is simply a list of nodes. For the purposes of joint consensus, it
a list of "current" nodes (which are currently running) and an optional list
of "next" nodes, which are subject to joint consensus.
*/
type NodeList struct {
	Current []Node `json:"current"`
	Next    []Node `json:"next"`
}

func decodeNodeList(buf []byte) (*NodeList, error) {
	var nl protobufs.NodeListPb
	err := proto.Unmarshal(buf, &nl)
	if err != nil {
		return nil, err
	}

	l := NodeList{}
	for _, pn := range nl.GetCurrent() {
		n := Node{
			NodeID:  common.NodeID(pn.GetNodeId()),
			Address: pn.GetAddress(),
		}
		l.Current = append(l.Current, n)
	}
	for _, pn := range nl.GetNext() {
		n := Node{
			NodeID:  common.NodeID(pn.GetNodeId()),
			Address: pn.GetAddress(),
		}
		l.Next = append(l.Next, n)
	}
	return &l, nil
}

/*
GetUniqueNodes returns only the unique nodes. This is helpful when in joint
consensus mode.
*/
func (nl *NodeList) GetUniqueNodes() []Node {
	var l []Node
	ids := make(map[common.NodeID]bool)
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

/*
GetNode returns information about a single node in the list, or nil if the
node does not exist.
*/
func (nl *NodeList) GetNode(id common.NodeID) *Node {
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
	np := protobufs.NodeListPb{}
	for _, n := range nl.Current {
		pn := protobufs.NodePb{
			NodeId:  proto.Uint64(uint64(n.NodeID)),
			Address: proto.String(n.Address),
		}
		np.Current = append(np.Current, &pn)
	}
	for _, n := range nl.Next {
		pn := protobufs.NodePb{
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
