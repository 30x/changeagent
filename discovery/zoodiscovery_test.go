package discovery

import (
  "os"
  "testing"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

func TestZKConnect(t *testing.T) {
  zk := connect(t)
  if zk == nil {
    t.Skip("TEST_ZK_SERVER not set: Not testing with zookeeper")
    return
  }

  zk.Close()
}

func TestAddRemove(t *testing.T) {
  zk := connect(t)
  if zk == nil {
    t.SkipNow()
  }

  n1 := Node{
    Id: 1,
    Address: "foo",
  }
  err := zk.AddNode(&n1)
  if err != nil {
    t.Fatalf("Fatal error: %v", err)
  }

  err = zk.RemoveNode(1)
  if err != nil {
    t.Fatalf("Fatal error: %v", err)
  }

  zk.Close()
}

func TestGetNodes(t *testing.T) {
  zk := connect(t)
  if zk == nil {
    t.SkipNow()
  }

  n1 := Node{
    Id: 2,
    Address: "foo",
  }
  err := zk.AddNode(&n1)
  if err != nil {
    t.Fatalf("Fatal error: %v", err)
  }
  defer zk.RemoveNode(2)

  zk2 := connect(t)
  nodes := zk2.GetNodes()
  if len(nodes) != 1 {
    t.Fatal("Expected one node")
  }
  if nodes[0].Id != 2 {
    t.Fatal("Expected node ID 2")
  }

  zk2.Close()
  zk.Close()
}

func TestWatchAdd(t *testing.T) {
  zk := connect(t)
  if zk == nil {
    t.SkipNow()
  }
  defer zk.Close()

  log.Infof("Adding node 3")
  n1 := Node{
    Id: 3,
    Address: "foo",
  }
  err := zk.AddNode(&n1)
  if err != nil {
    t.Fatalf("Fatal error: %v", err)
  }
  defer func() {
    log.Infof("Removing node 3")
    zk.RemoveNode(3)
  }()

  zk2 := connect(t)
  defer zk2.Close()
  nodes := zk2.GetNodes()
  if len(nodes) != 1 {
    t.Fatal("Expected one node")
  }
  if nodes[0].Id != 3 {
    t.Fatal("Expected node ID 3")
  }

  log.Infof("Adding node 4")
  n2 := Node{
    Id: 4,
    Address: "foo",
  }
  err = zk.AddNode(&n2)
  if err != nil {
    t.Fatalf("Fatal error: %v", err)
  }
  defer func() {
    log.Infof("Removing node 4")
    zk.RemoveNode(4)
  }()

  <- zk.GetChanges()
  log.Infof("Got event from change channel")

  nodes = zk2.GetNodes()
  if len(nodes) != 2 {
    t.Fatal("Expected two nodes")
  }
  if nodes[0].Id != 3 {
    t.Fatal("Expected node ID 3")
  }
  if nodes[1].Id != 4 {
    t.Fatal("Expected node ID 4")
  }

  log.Infof("All done")
}

func connect(t *testing.T) Discovery {
  se := os.Getenv("TEST_ZK_SERVER")
  if se == "" {
    return nil
  }
  cs := []string{se}

  zk, err := StartZookeeperDiscovery(cs, "/testdiscovery", "foo", "bar")
  if err != nil {
    t.Fatalf("Fatal error: %v", err)
  }
  return zk
}
