package discovery

import (
  "fmt"
  "time"
  "github.com/golang/protobuf/proto"
  "github.com/samuel/go-zookeeper/zk"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

const (
  ConnectTimeout = 15 * time.Second
)

type ZookeeperDiscovery struct {
  zk *zk.Conn                  // Connection to ZK
  path string                  // Directory where we're doing discovery
  nodes map[uint64]*Node       // Last list of nodes
  acl []zk.ACL                 // ACL for all new ZK entries
  eventChan <-chan zk.Event    // chan where ZK delivers watcher updates
  stopChan chan bool           // chan where we send "stop" notification
  changeChan chan Change       // Chan where we deliver changes to clients
  commandChan chan bool        // Chan where we send commands in response to ZK watches
}

func StartZookeeperDiscovery(servers []string, path string, user string, pw string) (*ZookeeperDiscovery, error) {
  log.Infof("Connecting to zookeeper at %v", servers)
  zoo, eventChan, err := zk.Connect(servers, ConnectTimeout)
  if err != nil {
    return nil, err
  }
  err = zoo.AddAuth("digest", []byte(fmt.Sprintf("%s:%s", user, pw)))
  if err != nil {
    return nil, err
  }

  ret := &ZookeeperDiscovery{
    zk: zoo,
    path: path,
    acl: zk.DigestACL(zk.PermAll, user, pw),
    eventChan: eventChan,
    stopChan: make(chan bool),
    nodes: make(map[uint64]*Node),
    changeChan: make(chan Change, 10),
    commandChan: make(chan bool),
  }

  pathExists, _, err := zoo.Exists(path)
  if err != nil {
    return nil, err
  }

  if !pathExists {
    _, err = zoo.Create(path, nil, 0, ret.acl)
    if err != nil {
      return nil, err
    }
  }

  newNodes, err := ret.updateChildren()
  if err != nil {
    return nil, err
  }
  ret.nodes = newNodes

  go ret.zkEventLoop()
  go ret.commandLoop()

  return ret, nil
}

func (z *ZookeeperDiscovery) Close() {
  z.stopChan <- true
}

func (z *ZookeeperDiscovery) GetChanges() <-chan Change {
  return z.changeChan
}

func (z *ZookeeperDiscovery) GetNodes() []Node {
  var nodes []Node
  for _, v := range(z.nodes) {
    nodes = append(nodes, *v)
  }
  return nodes
}

func (z *ZookeeperDiscovery) AddNode(node *Node) error {
  ns := int32(node.State)
  pb := NodePb{
    Id: &node.Id,
    Address: &node.Address,
    State: &ns,
  }
  buf, _ := proto.Marshal(&pb)

  nodePath := fmt.Sprintf("%s/%d", z.path, node.Id)
  _, err := z.zk.Create(nodePath, buf, 0, z.acl)
  return err
}

func (z *ZookeeperDiscovery) RemoveNode(id uint64) error {
  nodePath := fmt.Sprintf("%s/%d", z.path, id)
  err := z.zk.Delete(nodePath, -1)
  return err
}

func (z *ZookeeperDiscovery) zkEventLoop() {
  for {
    select {
    case se := <-z.eventChan:
      if se.Type == zk.EventNodeChildrenChanged {
        // We only start one watcher so this always mean we need to re-check children
        log.Info("Node children changed")
        z.commandChan <- true
      } else {
        log.Infof("Unknown server event %v", se.Type)
      }
    case <-z.stopChan:
      // Nothing to do!
    }
  }

  // z.zk.Close()
}

func (z *ZookeeperDiscovery) commandLoop() {
  for {
    <- z.commandChan
    newNodes, err := z.updateChildren()
    if err == nil {
      oldNodes := z.nodes
      z.nodes = newNodes
      z.sendDifferences(oldNodes, newNodes)
    } else {
      log.Infof("Error getting state from zookeeper: %v", err)
    }
  }
}

func (z *ZookeeperDiscovery) updateChildren() (map[uint64]*Node, error) {
  ret := make(map[uint64]*Node)
  childList, _, _, err := z.zk.ChildrenW(z.path)
  log.Infof("New children: %v", childList)
  if err != nil {
    return nil, fmt.Errorf("Error getting %s: %s", z.path, err.Error())
  }
  for _, c := range childList {
    path := fmt.Sprintf("%s/%s", z.path, c)
    buf, _, err := z.zk.Get(path)
    if err != nil {
      return nil, fmt.Errorf("Error getting %s: %s", path, err.Error())
    }

    var pb NodePb
    err = proto.Unmarshal(buf, &pb)
    if err != nil {
      return nil, fmt.Errorf("Error unmarshaling %s: %s", path, err.Error())
    }
    node := Node{
      Id: pb.GetId(),
      Address: pb.GetAddress(),
      State: int(pb.GetState()),
    }
    ret[pb.GetId()] = &node
  }
  log.Infof("Got new list of nodes: %v", ret)
  return ret, nil
}

func (z *ZookeeperDiscovery) sendDifferences(oldNodes map[uint64]*Node, newNodes map[uint64]*Node) {
  oldKeys := make(map[uint64]bool)
  for k := range(oldNodes) {
    oldKeys[k] = true
  }

  for k, n := range(newNodes) {
    delete(oldKeys, k)
    if oldNodes[k] == nil {
      log.Infof("New key %d", k)
      e := Change{
        Action: NewNode,
        Node: *n,
      }
      z.changeChan <- e
    }
  }

  for k := range(oldKeys) {
    log.Infof("Deleted key %d", k)
    e := Change{
      Action: DeletedNode,
    }
    z.changeChan <- e
  }
}
