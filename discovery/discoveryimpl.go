package discovery

import (
  "sync"
)

type updateCmd struct {
  newWatcher *(chan Change)
  newNodes []Node
}

type DiscoveryService struct {
  latch *sync.Mutex
  nodes []Node
  spi discoverySpi
  updateChan chan updateCmd
  stopChan chan chan bool
}

type discoverySpi interface {
  stop()
}

/*
 * TODO add public methods to encode and decode the node list.
 * The Raft impl will use this to store it in the database.
 */

/*
 * SPIs must call this to create the thing that they return from their "create" methods.
 */
func createImpl(nodes []Node, spi discoverySpi) *DiscoveryService {
  disco := &DiscoveryService{
    latch: &sync.Mutex{},
    nodes: nodes,
    spi: spi,
    stopChan: make(chan chan bool, 1),
    updateChan: make(chan updateCmd, 1),
  }
  go disco.discoveryLoop()
  return disco
}

func (d *DiscoveryService) GetNodes() []Node {
  d.latch.Lock()
  defer d.latch.Unlock()
  return d.nodes
}

func (d *DiscoveryService) setNodes(newNodes []Node) {
  d.latch.Lock()
  d.nodes = newNodes
  d.latch.Unlock()
}

func (d *DiscoveryService) GetAddress(id uint64) string {
  d.latch.Lock()
  defer d.latch.Unlock()

  for _, n := range(d.nodes) {
    if n.Id == id {
      return n.Address
    }
  }
  return ""
}

func (d *DiscoveryService) Watch() <-chan Change {
  watchChan := make(chan Change, 1)
  cmd := updateCmd{
    newWatcher: &watchChan,
  }
  d.updateChan <- cmd
  return watchChan
}

func (d *DiscoveryService) updateNodes(newNodes []Node) {
  cmd := updateCmd{
    newNodes: newNodes,
  }
  d.updateChan <- cmd
}

func (d *DiscoveryService) Close() {
  if d.spi != nil {
    d.spi.stop()
  }

  stopped := make(chan bool, 1)
  d.stopChan <- stopped
  <- stopped
}

func (d *DiscoveryService) discoveryLoop() {
  running := true
  var watchers [](chan Change)

  for running {
    select {
    case u := <- d.updateChan:
      if u.newWatcher != nil {
        watchers = append(watchers, *(u.newWatcher))
      } else {
        changes := compareChanges(d.GetNodes(), u.newNodes)
        for _, w := range(watchers) {
          for _, c := range(changes) {
            w <- c
          }
        }
        d.setNodes(u.newNodes)
      }

    case stopper := <- d.stopChan:
      running = false
      stopper <- true
    }
  }
}
