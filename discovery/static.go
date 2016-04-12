package discovery

import (
  "bufio"
  "errors"
  "fmt"
  "io"
  "os"
  "regexp"
  "strconv"
  "time"
)

var fileLine = regexp.MustCompile("^([0-9]+)\\s(.+)")

func CreateStaticDiscovery(addrs []string) *Service {
  var id uint64 = 1
  var nodes []Node

  for _, na := range(addrs) {
    nn := Node{
      ID: id,
      Address: na,
      State: StateMember,
    }
    id++
    nodes = append(nodes, nn)
  }

  ret := createImpl(nodes, nil)

  return ret
}

func ReadDiscoveryFile(fileName string, updateInterval time.Duration) (*Service, error) {
  nodes, err := readFile(fileName)
  if err != nil { return nil, err }
  ret := createImpl(nodes, nil)
  if updateInterval > 0 {
    go ret.fileReadLoop(fileName, updateInterval)
  }
  return ret, nil
}

func readFile(fileName string) ([]Node, error) {
  f, err := os.Open(fileName)
  if err != nil { return nil, err }
  defer f.Close()

  rdr := bufio.NewReader(f)
  var nodes []Node

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
      ID: id,
      Address: matches[2],
    }
    nodes = append(nodes, nn)
  }
  return nodes, nil
}

func (d *Service) fileReadLoop(fileName string, interval time.Duration) {
  for {
    time.Sleep(interval)
    newNodes, err := readFile(fileName)
    if err == nil {
      d.updateNodes(newNodes)
    }
  }
}
