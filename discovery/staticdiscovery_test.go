package discovery

import (
  "testing"
  "time"
  "os"
)

func TestDiscoveryFile(t *testing.T) {
  d, err := ReadDiscoveryFile("./testfiles/testdisco", 0)
  if err != nil { t.Fatalf("Error reading file: %v", err) }

  if len(d.GetNodes()) != 2 {
    t.Fatal("Expected only two nodes to be discovered")
  }
  if d.GetNodes()[0].Id != 1 {
    t.Fatalf("Expected node Id 1 instead of %d", d.GetNodes()[0].Id)
  }
  if d.GetNodes()[0].Address != "localhost:1234" {
    t.Fatal("Invalid address for first node")
  }
  if d.GetNodes()[1].Id != 2 {
    t.Fatalf("Expected node Id 2 instead of %d", d.GetNodes()[1].Id)
  }
  if d.GetNodes()[1].Address != "localhost:2345" {
    t.Fatal("Invalid address for second node")
  }
}

func TestFixedDiscovery(t *testing.T) {
  d := CreateStaticDiscovery([]string{"one", "two", "three"})

  if len(d.GetNodes()) != 3 { t.Fatal("Expected three entries") }
  if d.GetNodes()[0].Id != 1 { t.Fatal("invalid node ID") }
  if d.GetNodes()[0].Address != "one" { t.Fatal("invalid address") }
  if d.GetNodes()[1].Id != 2 { t.Fatal("invalid node ID") }
  if d.GetNodes()[1].Address != "two" { t.Fatal("invalid address") }
  if d.GetNodes()[2].Id != 3 { t.Fatal("invalid node ID") }
  if d.GetNodes()[2].Address != "three" { t.Fatal("invalid address") }
}

func TestDiscoveryChanges(t *testing.T) {
  d := CreateStaticDiscovery([]string{"one", "two", "three"})
  // Hack this to easily create a different set of nodes and test internally replacing them.
  newNodes := CreateStaticDiscovery([]string{"one", "two", "three", "four"}).GetNodes()

  syncChanges := make(chan bool, 1)
  go func() {
    changeWatcher := d.Watch()
    syncChanges <- true
    change := <- changeWatcher
    if change.Action != NewNode { t.Fatal("Got wrong action on add node") }
    syncChanges <- true
  }()

  // Manually do what an SPI would do for itself,
  // but only after other channel is ready
  <- syncChanges
  d.updateNodes(newNodes)

  timeout := time.After(2 * time.Second)
  select {
  case <- syncChanges:
    t.Log("Successfully picked up change")
  case <- timeout:
    t.Fatal("Never got change notification")
  }
}

func TestDiscoveryTwoWatchers(t *testing.T) {
  d := CreateStaticDiscovery([]string{"one", "two", "three"})
  // Hack this to easily create a different set of nodes and test internally replacing them.
  newNodes := CreateStaticDiscovery([]string{"one", "two", "three", "four"}).GetNodes()

  syncChanges := make(chan bool, 1)

  go func() {
    changeWatcher := d.Watch()
    syncChanges <- true
    change := <- changeWatcher
    if change.Action != NewNode { t.Fatal("Got wrong action on add node") }
    t.Log("Watcher 1 got a change")
    syncChanges <- true
  }()

  go func() {
    changeWatcher := d.Watch()
    syncChanges <- true
    change := <- changeWatcher
    if change.Action != NewNode { t.Fatal("Got wrong action on add node") }
    t.Log("Watcher 2 got a change")
    syncChanges <- true
  }()

  // Manually do what an SPI would do for itself,
  // but only after other channel is ready
  <- syncChanges
  <- syncChanges
  d.updateNodes(newNodes)

  timeout := time.After(2 * time.Second)
  select {
  case <- syncChanges:
    <- syncChanges
    t.Log("Successfully picked up change")
  case <- timeout:
    t.Fatal("Never got change notification")
  }
}

func TestDiscoveryFileUpdate(t *testing.T) {
  err := copyFile(t, "./testfiles/testdisco", "./testfiles/tmp")
  if err != nil { t.Fatalf("%v", err) }
  defer os.Remove("./testfiles/tmp")

  d, err := ReadDiscoveryFile("./testfiles/tmp", 250 * time.Millisecond)
  if err != nil { t.Fatalf("Error reading file: %v", err) }

  if len(d.GetNodes()) != 2 {
    t.Fatal("Expected only two nodes to be discovered")
  }
  if d.GetNodes()[0].Id != 1 {
    t.Fatalf("Expected node Id 1 instead of %d", d.GetNodes()[0].Id)
  }
  if d.GetNodes()[0].Address != "localhost:1234" {
    t.Fatal("Invalid address for first node")
  }
  if d.GetNodes()[1].Id != 2 {
    t.Fatalf("Expected node Id 2 instead of %d", d.GetNodes()[1].Id)
  }
  if d.GetNodes()[1].Address != "localhost:2345" {
    t.Fatal("Invalid address for second node")
  }

  syncChanges := make(chan bool, 1)
  go func() {
    changeWatcher := d.Watch()
    syncChanges <- true
    change := <- changeWatcher
    if change.Action != UpdatedNode { t.Fatal("Got wrong action on add node") }
    if change.Node.Address != "localhost:9999" { t.Fatal("Got wrong address on node") }
    if change.Node.Id != 2 { t.Fatal("Got wrong ID on node") }
    syncChanges <- true
  }()

  // Manually do what an SPI would do for itself,
  // but only after other channel is ready
  <- syncChanges
  err = copyFile(t, "./testfiles/testdisco2", "./testfiles/tmp")
  if err != nil { t.Fatalf("%v", err) }

  timeout := time.After(2 * time.Second)
  select {
  case <- syncChanges:
    t.Log("Successfully picked up change")
  case <- timeout:
    t.Fatal("Never got change notification")
  }
}

func copyFile(t *testing.T, src string, dst string) error {
  dstFile, err := os.OpenFile(dst, os.O_RDWR | os.O_CREATE, 0666)
  if err != nil { return err }
  defer dstFile.Close()

  srcFile, err := os.OpenFile(src, os.O_RDONLY, 0)
  if err != nil { return err }
  defer srcFile.Close()
  stat, err := srcFile.Stat()
  if err != nil { return err }

  buf := make([]byte, stat.Size())
  _, err = srcFile.Read(buf)
  if err != nil { return err }
  _, err = dstFile.Write(buf)
  if err != nil { return err }
  t.Logf("Copied %d bytes", len(buf))
  return nil
}
