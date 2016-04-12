package discovery

import (
  "bytes"
  "fmt"
  "github.com/golang/protobuf/proto"
)

/*
 * This module contains a set of discovery services that make it easier to
 * learn which servers are configured as part of a cluster, and make it easier
 * to know when that configuration changes.
 */

const (
  StateNew = iota
  StateCatchup = iota
  StateMember = iota
  StateDeleting = iota

  NewNode = iota
  DeletedNode = iota
  UpdatedNode = iota
)

/*
 * A single node in the system. Nodes have unique IDs, and an address which is currently an HTTP URI,
 * followed by a state that helps us track whether they are done initializing.
 */
type Node struct {
  ID uint64
  Address string
  State int
}

/*
 * A change represents a new, deleted, or updated node.
 */
type Change struct {
  Action int
  Node *Node
}

/*
 * A NodeList is a list of nodes that represents the current config state. It has a "new" and "old"
 * list of Nodes. Under normal operation, only the "new" list is available. During a configuration
 * change, both lists are set and we are in a mode of joint consensus.
 */
type NodeList struct {
  New []Node
  Old []Node
}

/*
 * The current configuration of nodes in the system. This is what we persist in the raft
 * implementation. It includes both the current and previous node lists. We use this in case
 * we need to backtrack in the history and restore an old set of node configuration.
 */
type NodeConfig struct {
  Current *NodeList
  Previous *NodeList
}

type Discovery interface {
  /*
   * Get the current configuration from this service, which will only contain a new, "current"
   * config.
   */
  GetCurrentConfig() *NodeConfig

  /*
   * Get a list of nodes from this discovery service. Safe to be called from
   * many threads.
   */
  GetNodes() []Node

  /*
   * Shortcut to get just the "address" field from a single node identified
   * by its node ID. Also safe to be called from many threads.
   */
  GetAddress(id uint64) string

  /*
   * Return a channel that will be notified via a Change object every time
   * the list of nodes is changed.
   */
  Watch() <-chan Change

  /*
   * Stop any resources created by the service.
   */
  Close()
}

/*
 * Turn a NodeConfig into a marhshalled set of bytes for storage and distribution.
 */
func EncodeConfig(config *NodeConfig) ([]byte, error) {
  cfgPb := NodeConfigPb{}
  if config.Current != nil {
    cfgPb.Current = marshalNodeList(config.Current)
  }
  if config.Previous != nil {
    cfgPb.Previous = marshalNodeList(config.Previous)
  }

  return proto.Marshal(&cfgPb)
}

/*
 * Do the opposite of EncodeConfig.
 */
func DecodeConfig(msg []byte) (*NodeConfig, error) {
  var cfg NodeConfigPb
  err := proto.Unmarshal(msg, &cfg)
  if err != nil { return nil, err }

  nc := NodeConfig{}
  if cfg.Previous != nil {
    nc.Previous = unmarshalNodeList(cfg.Previous)
  }
  if cfg.Current != nil {
    nc.Current = unmarshalNodeList(cfg.Current)
  }
  return &nc, nil
}

/*
 * Return a deduped list of all the nodes in the current config.
 * This will return the latest node list if we are in joint consensus mode.
 */
func (c *NodeConfig) GetUniqueNodes() []Node {
  nl := make(map[uint64]Node)
  if c.Current != nil {
    for _, n := range(c.Current.Old) {
      nl[n.ID] = n
    }
    for _, n := range(c.Current.New) {
      nl[n.ID] = n
    }
  }

  var ret []Node
  for _, n := range(nl) {
    ret = append(ret, n)
  }
  return ret
}

func marshalNodeList(nl *NodeList) *NodeListPb {
  listPb := NodeListPb{}
  if len(nl.New) > 0 {
    listPb.New = marshalNodes(nl.New)
  }
  if len(nl.Old) > 0 {
    listPb.Old = marshalNodes(nl.Old)
  }
  return &listPb
}

func unmarshalNodeList(nl *NodeListPb) *NodeList {
  ret := NodeList{}
  if nl.New != nil {
    ret.New = unmarshalNodes(nl.New)
  }
  if nl.Old != nil {
    ret.Old = unmarshalNodes(nl.Old)
  }
  return &ret
}

func marshalNodes(nodes []Node) []*NodePb {
  var nl []*NodePb
  for _, n := range(nodes) {
    curNode := marshalNode(n)
    nl = append(nl, curNode)
  }
  return nl
}

func marshalNode(n Node) *NodePb {
  state := int32(n.State)
  curNode := NodePb{
    Id: &n.ID,
    Address: &n.Address,
    State: &state,
  }
  return &curNode
}

func unmarshalNodes(nl []*NodePb) []Node {
  var nodes []Node
  for _, n := range(nl) {
    nn := Node{
      ID: n.GetId(),
      Address: n.GetAddress(),
      State: int(n.GetState()),
    }
    nodes = append(nodes, nn)
  }
  return nodes
}

func (n Node) String() string {
  return fmt.Sprintf("{ Id: %d State: %d Address: %s }", n.ID, n.State, n.Address)
}

func (n Node) Equal(o Node) bool {
  if n.ID != o.ID { return false }
  if n.State != o.State { return false }
  if n.Address != o.Address { return false }
  return true
}

func (n *NodeList) String() string {
  buf := &bytes.Buffer{}
  fmt.Fprintf(buf, "New: [")
  for _, cn := range(n.New) {
    fmt.Fprintf(buf, cn.String())
  }
  fmt.Fprintf(buf, "], Old: [")
  for _, cn := range(n.Old) {
    fmt.Fprintf(buf, cn.String())
  }
  fmt.Fprintf(buf, "]")
  return buf.String()
}

func (n *NodeList) Equal(o *NodeList) bool {
  if len(n.New) != len(o.New) { return false }
  if len(n.Old) != len(o.Old) { return false }
  for i := range (n.New) {
    if !n.New[i].Equal(o.New[i]) { return false }
  }
  for i := range(n.Old) {
    if !n.Old[i].Equal(o.Old[i]) { return false }
  }
  return true
}

func (n *NodeConfig) String() string {
  buf := &bytes.Buffer{}
  if n.Current != nil {
    fmt.Fprintf(buf, "Current: %s ", n.Current.String())
  }
  if n.Previous != nil {
    fmt.Fprintf(buf, "Previous: %s", n.Previous.String())
  }
  return buf.String()
}

func (n *NodeConfig) Equal(o *NodeConfig) bool {
  if n.Current == nil {
    if o.Current != nil { return false }
  } else {
    if o.Current == nil { return false }
    if !n.Current.Equal(o.Current) { return false }
  }

  if n.Previous == nil {
    if o.Previous != nil { return false }
  } else {
    if o.Previous == nil { return false }
    if !n.Previous.Equal(o.Previous) { return false }
  }
  return true
}
