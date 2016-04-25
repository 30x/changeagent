package discovery

import (
  "bufio"
  "io"
  "os"
  "strings"
  "time"
  "github.com/golang/glog"
)

func CreateStaticDiscovery(nodes []string) *Service {
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

func readFile(fileName string) ([]string, error) {
  f, err := os.Open(fileName)
  if err != nil { return nil, err }
  defer f.Close()

  rdr := bufio.NewReader(f)
  var nodes []string

  for {
    line, err := rdr.ReadString('\n')
    if err == io.EOF {
      break
    }
    if err != nil {
      return nil, err
    }

    addr := strings.TrimSpace(line)
    if addr == "" {
      continue
    }

    nodes = append(nodes, addr)
  }
  return nodes, nil
}

