package discovery

import (
  "testing"
)

func TestDiscoveryFile(t *testing.T) {
  d, err := ReadDiscoveryFile("./testfiles/testdisco")
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
