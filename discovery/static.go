package discovery

import (
  "errors"
)

type StaticDiscovery struct {
  nodes []Node
}

func CreateStaticDiscovery(addrs []string) *StaticDiscovery {
  ret := StaticDiscovery{}
  var id uint64 = 1
  for _, na := range(addrs) {
    nn := Node{
      Id: id,
      Address: na,
      State: StateMember,
    }
    id++
    ret.nodes = append(ret.nodes, nn)
  }
  return &ret
}

func (s *StaticDiscovery) GetNodes() []Node {
  return s.nodes
}

func (s *StaticDiscovery) GetAddress(id uint64) string {
  for _, n := range(s.nodes) {
    if n.Id == id {
      return n.Address
    }
  }
  return ""
}

func (s *StaticDiscovery) AddNode(node *Node) error {
  return errors.New("Not implemented")
}

func (s *StaticDiscovery) RemoveNode(id uint64) error {
  return errors.New("Not implemented")
}

func (s *StaticDiscovery) GetChanges() <-chan Change {
  return make(chan Change)
}

func (s *StaticDiscovery) Close() {
}
