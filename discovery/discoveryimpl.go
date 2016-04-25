package discovery

import (
  "sync"
  "github.com/golang/glog"
)

type Service struct {
  latch *sync.Mutex
  nodes []string
  spi discoverySpi
  watcherChan chan chan bool
  changeChan chan []string
  stopChan chan chan bool
}

type discoverySpi interface {
  stop()
}

/*
 * SPIs must call this to create the thing that they return from their "create" methods.
 */
func createImpl(nodes []string, spi discoverySpi) *Service {
  disco := &Service{
    latch: &sync.Mutex{},
    nodes: nodes,
    spi: spi,
    stopChan: make(chan chan bool, 1),
    watcherChan: make(chan chan bool),
    changeChan: make(chan []string, 1),
  }
  go disco.discoveryLoop()
  return disco
}

func (d *Service) GetCurrentConfig() *NodeConfig {
  nodes := d.getNodes()
  cur := NodeList{New: nodes}
  return &NodeConfig{Current: &cur}
}

func (d *Service) getNodes() []string {
  d.latch.Lock()
  defer d.latch.Unlock()
  return d.nodes
}

func (d *Service) setNodes(newNodes []string) {
  d.latch.Lock()
  d.nodes = newNodes
  d.latch.Unlock()
}

func (d *Service) AddNode(newNode string) {
  d.latch.Lock()
  defer d.latch.Unlock()

  found := false
  for _, n := range(d.nodes) {
    if n == newNode {
      found = true
    }
  }
  if !found {
    newNodes := append(d.nodes, newNode)
    d.updateNodes(newNodes)
  }
}

func (d *Service) DeleteNode(oldNode string) {
  d.latch.Lock()
  defer d.latch.Unlock()

  var newNodes []string
  for i := range(d.nodes) {
    if d.nodes[i] != oldNode {
      newNodes = append(newNodes, d.nodes[i])
    }
  }
  d.updateNodes(newNodes)
}

func (d *Service) Watch() <-chan bool {
  watchChan := make(chan bool, 1)
  d.watcherChan <- watchChan
  return watchChan
}

func (d *Service) updateNodes(newNodes []string) {
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
  var watchers [](chan bool)

  for running {
    select {
    case w := <- d.watcherChan:
       glog.V(2).Info("Adding a new watcher")
       watchers = append(watchers, w)

    case c := <- d.changeChan:
      if (glog.V(3)) {
        glog.Infof("Old node list: %s", d.getNodes())
        glog.Infof("New node list: %s", c)
      }

      d.setNodes(c)
      for _, w := range (watchers) {
        w <- true
      }

    case stopper := <- d.stopChan:
      glog.V(2).Info("Stopping")
      running = false
      stopper <- true
    }
  }
}
