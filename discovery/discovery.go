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

  // Indicate that the node list has changed
  NodesChanged = iota
  // Indicate that the addresses of certain nodes have changed but not the nodes
  AddressesChanged = iota
)

/*
 * A single node in the system. Nodes have unique IDs, and an address which is currently a host:port
 * string, followed by a state that helps us track whether they are done initializing.
 */
type Node struct {
  ID uint64
  Address string
  State int
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
   * Compare the current configuration to the supplied configuration and return the same
   * "change type" that we'd return to the "watch" channel. We use this in case we
   * missed some notifications.
   */
  CompareCurrentConfig(oldCfg *NodeConfig) int

  /*
   * Return a channel that will be notified whenever the configuration changes. If the
   * list of nodes changed, then it will receive the value "NodesChanged." Otherwise it
   * will receive the value "AddressesChanged"
   */
  Watch() <-chan int

  /*
   * Stop any resources created by the service.
   */
  Close()

  /*
   * For testing only: Add a new node to the config, or update an existing node.
   */
  SetNode(n Node)

  /*
   * For testing only: Delete a node.
   */
  DeleteNode(id uint64)
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

/*
 * Find the address of a node in the specific config, or return an empty string.
 * Check both old and new configurations so that we get the newest address first.
 */
func (c *NodeConfig) GetAddress(id uint64) string {
  ret := ""
  if c.Current != nil {
    for i := range(c.Current.Old) {
      if c.Current.Old[i].ID == id {
        ret = c.Current.Old[i].Address
      }
    }
    for i := range(c.Current.New) {
      if c.Current.New[i].ID == id {
        ret = c.Current.New[i].Address
      }
    }
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

func (c *NodeConfig) String() string {
  buf := &bytes.Buffer{}
  if c.Current != nil {
    fmt.Fprintf(buf, "Current: %s ", c.Current.String())
  }
  if c.Previous != nil {
    fmt.Fprintf(buf, "Previous: %s", c.Previous.String())
  }
  return buf.String()
}

func (c *NodeConfig) Equal(o *NodeConfig) bool {
  if c.Current == nil {
    if o.Current != nil { return false }
  } else {
    if o.Current == nil { return false }
    if !c.Current.Equal(o.Current) { return false }
  }

  if c.Previous == nil {
    if o.Previous != nil { return false }
  } else {
    if o.Previous == nil { return false }
    if !c.Previous.Equal(o.Previous) { return false }
  }
  return true
}
