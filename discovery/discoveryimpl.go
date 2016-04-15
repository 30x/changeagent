package discovery

import (
  "sync"
  "github.com/golang/glog"
)

type Service struct {
  latch *sync.Mutex
  nodes []Node
  spi discoverySpi
  watcherChan chan chan int
  changeChan chan []Node
  stopChan chan chan bool
}

type discoverySpi interface {
  stop()
}

/*
 * SPIs must call this to create the thing that they return from their "create" methods.
 */
func createImpl(nodes []Node, spi discoverySpi) *Service {
  disco := &Service{
    latch: &sync.Mutex{},
    nodes: nodes,
    spi: spi,
    stopChan: make(chan chan bool, 1),
    watcherChan: make(chan chan int),
    changeChan: make(chan []Node, 1),
  }
  go disco.discoveryLoop()
  return disco
}

func (d *Service) GetCurrentConfig() *NodeConfig {
  nodes := d.getNodes()
  cur := NodeList{New: nodes}
  return &NodeConfig{Current: &cur}
}

func (d *Service) CompareCurrentConfig(oldCfg *NodeConfig) int {
  return getChangeType(d.getNodes(), oldCfg.Current.New)
}

func (d *Service) getNodes() []Node {
  d.latch.Lock()
  defer d.latch.Unlock()
  return d.nodes
}

func (d *Service) setNodes(newNodes []Node) {
  d.latch.Lock()
  d.nodes = newNodes
  d.latch.Unlock()
}

func (d *Service) SetNode(newNode Node) {
  d.latch.Lock()
  defer d.latch.Unlock()

  var newNodes []Node
  added := false
  for _, n := range(d.nodes) {
    if n.ID == newNode.ID {
      newNodes = append(newNodes, newNode)
      added = true
    } else {
      newNodes = append(newNodes, n)
    }
  }
  if !added {
    newNodes = append(newNodes, newNode)
  }
  d.updateNodes(newNodes)
}

func (d *Service) DeleteNode(id uint64) {
  d.latch.Lock()
  defer d.latch.Unlock()

  var newNodes []Node
  for i := range(d.nodes) {
    if d.nodes[i].ID != id {
      newNodes = append(newNodes, d.nodes[i])
    }
  }
  d.updateNodes(newNodes)
}

func (d *Service) GetAddress(id uint64) string {
  d.latch.Lock()
  defer d.latch.Unlock()

  for _, n := range(d.nodes) {
    if n.ID == id {
      return n.Address
    }
  }
  return ""
}

func (d *Service) Watch() <-chan int {
  watchChan := make(chan int, 1)
  d.watcherChan <- watchChan
  return watchChan
}

func (d *Service) updateNodes(newNodes []Node) {
  d.changeChan <- newNodes
}

func (d *Service) Close() {
  if d.spi != nil {
    d.spi.stop()
  }

  stopped := make(chan bool, 1)
  d.stopChan <- stopped
  <- stopped
}

func (d *Service) discoveryLoop() {
  running := true
  var watchers [](chan int)

  for running {
    select {
    case w := <- d.watcherChan:
       glog.V(2).Info("Adding a new watcher")
       watchers = append(watchers, w)

    case c := <- d.changeChan:
      changeType := getChangeType(d.getNodes(), c)
      glog.V(2).Infof("Got a new change of type %d", changeType)
      if (glog.V(3)) {
        glog.Infof("Old node list: %s", d.getNodes())
        glog.Infof("New node list: %s", c)
      }
      if changeType != 0 {
        d.setNodes(c)
        for _, w := range (watchers) {
          w <- changeType
        }
      }

    case stopper := <- d.stopChan:
      glog.V(2).Info("Stopping")
      running = false
      stopper <- true
    }
  }
}
