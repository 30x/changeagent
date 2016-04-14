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
  "github.com/golang/glog"
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

  rdr := fileReader{
    fileName: fileName,
    interval: updateInterval,
    stopChan: make(chan bool, 1),
  }

  ret := createImpl(nodes, &rdr)
  rdr.d = ret

  rdr.start()
  return ret, nil
}

type fileReader struct {
  d *Service
  fileName string
  interval time.Duration
  stopChan chan bool
}

func (r *fileReader) start() {
  if r.interval == 0 {
    return
  }
  go r.readLoop()
}

func (r *fileReader) stop() {
  r.stopChan <- true
}

func (r *fileReader) readLoop() {
  ticker := time.NewTicker(r.interval)
  sentError := false

  for {
    select {
    case <-ticker.C:
      newNodes, err := readFile(r.fileName)
      if err == nil {
        r.d.updateNodes(newNodes)
      } else if !sentError {
        glog.Errorf("Error reading discovery file for changes: %v", err)
        sentError = true
      }

    case <-r.stopChan:
      ticker.Stop()
      return
    }
  }
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

