package discovery

import (
	"bytes"
	"fmt"

	"github.com/golang/protobuf/proto"
)

//go:generate protoc --go_out=. discovery.proto

/*
 * This module contains a set of discovery services that make it easier to
 * learn which servers are configured as part of a cluster, and make it easier
 * to know when that configuration changes.
 */

/*
A NodeList is a list of nodes that represents the current config state. It has a "new" and "old"
list of Nodes. Under normal operation, only the "new" list is available. During a configuration
change, both lists are set and we are in a mode of joint consensus.
*/
type NodeList struct {
	New []string `json:"new,omitempty"`
	Old []string `json:"old,omitempty"`
}

/*
NodeConfig is the current configuration of nodes in the system. This is what we persist in the raft
implementation. It includes both the current and previous node lists. We use this in case
we need to backtrack in the history and restore an old set of node configuration.
*/
type NodeConfig struct {
	Current  *NodeList `json:"current,omitempty"`
	Previous *NodeList `json:"previous,omitempty"`
}

/*
Discovery is the interface that other modules use to get an intial node list
from the discovery mechanism.
*/
type Discovery interface {
	/*
	 * Get the current configuration from this service, which will only contain a new, "current"
	 * config.
	 */
	GetCurrentConfig() *NodeConfig

	/*
	 * If this method returns true, then the implementation only supports a single
	 * stand-alone node. In that case, configuration changes will never be delivered.
	 * This will only be true if the service has a single node and it will never
	 * have anything other than a single node.
	 */
	IsStandalone() bool

	/*
	 * Return a channel that will be notified whenever the configuration changes.
	 */
	Watch() <-chan bool

	/*
	 * Stop any resources created by the service.
	 */
	Close()

	/*
	 * For testing only: Add a new node to the config, or update an existing node.
	 */
	AddNode(a string)

	/*
	 * For testing only: Delete a node.
	 */
	DeleteNode(a string)
}

/*
EncodeConfig turns a NodeConfig into a marhshalled set of bytes for storage and distribution.
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
DecodeConfig turns the bytes from EncodeConfig into a NodeConfig object.
*/
func DecodeConfig(msg []byte) (*NodeConfig, error) {
	var cfg NodeConfigPb
	err := proto.Unmarshal(msg, &cfg)
	if err != nil {
		return nil, err
	}

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
GetUniqueNodes returns a deduped list of all the nodes in the current config.
This will return the latest node list if we are in joint consensus mode.
*/
func (c *NodeConfig) GetUniqueNodes() []string {
	nl := make(map[string]string)
	if c.Current != nil {
		for _, n := range c.Current.Old {
			nl[n] = n
		}
		for _, n := range c.Current.New {
			nl[n] = n
		}
	}

	var ret []string
	for _, n := range nl {
		ret = append(ret, n)
	}
	return ret
}

func marshalNodeList(nl *NodeList) *NodeListPb {
	listPb := NodeListPb{}
	if len(nl.New) > 0 {
		listPb.New = nl.New
	}
	if len(nl.Old) > 0 {
		listPb.Old = nl.Old
	}
	return &listPb
}

func unmarshalNodeList(nl *NodeListPb) *NodeList {
	ret := NodeList{}
	if nl.New != nil {
		ret.New = nl.GetNew()
	}
	if nl.Old != nil {
		ret.Old = nl.GetOld()
	}
	return &ret
}

func (n *NodeList) String() string {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "New: [")
	for _, cn := range n.New {
		fmt.Fprintf(buf, cn)
	}
	fmt.Fprintf(buf, "], Old: [")
	for _, cn := range n.Old {
		fmt.Fprintf(buf, cn)
	}
	fmt.Fprintf(buf, "]")
	return buf.String()
}

/*
Equal returns true if the specified node lists have the same
sets of keys and values.
*/
func (n *NodeList) Equal(o *NodeList) bool {
	if len(n.New) != len(o.New) {
		return false
	}
	if len(n.Old) != len(o.Old) {
		return false
	}
	for i := range n.New {
		if n.New[i] != o.New[i] {
			return false
		}
	}
	for i := range n.Old {
		if n.Old[i] != o.Old[i] {
			return false
		}
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

/*
Equal returns true if the specified NodeConfig objects have the same
sets of keys and values.
*/
func (c *NodeConfig) Equal(o *NodeConfig) bool {
	if c.Current == nil {
		if o.Current != nil {
			return false
		}
	} else {
		if o.Current == nil {
			return false
		}
		if !c.Current.Equal(o.Current) {
			return false
		}
	}

	if c.Previous == nil {
		if o.Previous != nil {
			return false
		}
	} else {
		if o.Previous == nil {
			return false
		}
		if !c.Previous.Equal(o.Previous) {
			return false
		}
	}
	return true
}
