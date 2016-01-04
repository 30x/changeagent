package discovery

import (
  "bufio"
  "errors"
  "fmt"
  "io"
  "os"
  "regexp"
  "strconv"
)

var fileLine *regexp.Regexp = regexp.MustCompile("^([0-9]+)\\s(.+)")

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

func ReadDiscoveryFile(fileName string) (*StaticDiscovery, error) {
  f, err := os.Open(fileName)
  if err != nil { return nil, err }
  defer f.Close()

  rdr := bufio.NewReader(f)
  disco := &StaticDiscovery{}

  for {
    line, prefix, err := rdr.ReadLine()
    if err == io.EOF {
      break
    }
    if err != nil {
      return nil, err
    }
    if prefix {
      return nil, errors.New("Line too long")
    }
    if string(line) == "" {
      continue
    }

    matches := fileLine.FindStringSubmatch(string(line))
    if matches == nil {
      return nil, fmt.Errorf("Invalid input line: \"%s\"", string(line))
    }

    id, err := strconv.ParseUint(matches[1], 10, 64)
    if err != nil {
      return nil, fmt.Errorf("Invalid node ID: %s", matches[1])
    }

    nn := Node{
      Id: id,
      Address: matches[2],
    }
    disco.nodes = append(disco.nodes, nn)
  }

  return disco, nil
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
