package discovery

import (
  "testing"
)

func TestEmptyChanges(t *testing.T) {
  var old []Node
  var new []Node
  changes := compareChanges(old, new)
  if len(changes) != 0 { t.Fatal("Changes found!") }
}

func TestAdd(t *testing.T) {
  var old []Node

  new := []Node{{
    ID: 1,
    Address: "foo:1234",
  }}

  changes := compareChanges(old, new)
  if len(changes) != 1 { t.Fatal("No changes found!") }
  if changes[0].Action != NewNode { t.Fatal("Invalid node type") }
  if changes[0].Node.Address != new[0].Address { t.Fatal("Invalid address") }
}

func TestAdd2(t *testing.T) {
  old := []Node{
  {
    ID: 10,
    Address: "foo:1234",
  },
  {
    ID: 30,
    Address: "baz:1234",
  }}

  new := []Node{
  {
    ID: 10,
    Address: "foo:1234",
  },
  {
    ID: 20,
    Address: "bar:1234",
  },
  {
    ID: 30,
    Address: "baz:1234",
  }}

  changes := compareChanges(old, new)
  if len(changes) != 1 { t.Fatal("No changes found!") }
  if changes[0].Action != NewNode { t.Fatal("Invalid node type") }
  if changes[0].Node.ID != 20 { t.Fatalf("Wrong node ID %d", changes[0].Node.ID) }
  if changes[0].Node.Address != "bar:1234" { t.Fatal("Invalid address") }
}

func TestRemove(t *testing.T) {
  old := []Node{{
    ID: 1,
    Address: "foo:1234",
  }}
  var new []Node

  changes := compareChanges(old, new)
  if len(changes) != 1 { t.Fatal("No changes found!") }
  if changes[0].Action != DeletedNode { t.Fatal("Invalid node type") }
  if changes[0].Node.Address != old[0].Address { t.Fatal("Invalid address") }
}

func TestRemove2(t *testing.T) {
  new := []Node{
  {
    ID: 10,
    Address: "foo:1234",
  },
  {
    ID: 30,
    Address: "baz:1234",
  }}

  old := []Node{
  {
    ID: 10,
    Address: "foo:1234",
  },
  {
    ID: 20,
    Address: "bar:1234",
  },
  {
    ID: 30,
    Address: "baz:1234",
  }}

  changes := compareChanges(old, new)
  if len(changes) != 1 { t.Fatal("No changes found!") }
  if changes[0].Action != DeletedNode { t.Fatal("Invalid node type") }
  if changes[0].Node.ID != 20 { t.Fatalf("Wrong node ID %d", changes[0].Node.ID) }
  if changes[0].Node.Address != "bar:1234" { t.Fatal("Invalid address") }
}

func TestUpdate(t *testing.T) {
  old := []Node{{
    ID: 1,
    Address: "foo:1234",
  }}
  new := []Node{{
    ID: 1,
    Address: "bar:1234",
  }}

  changes := compareChanges(old, new)
  if len(changes) != 1 { t.Fatal("No changes found!") }
  if changes[0].Action != UpdatedNode { t.Fatal("Invalid node type") }
  if changes[0].Node.Address != "bar:1234" { t.Fatal("Invalid change propagated") }
}

func TestUpdate2(t *testing.T) {
  old := []Node{
  {
    ID: 10,
    Address: "foo:1234",
  },
  {
    ID: 20,
    Address: "bar:1234",
  },
  {
    ID: 30,
    Address: "baz:1234",
  }}

  new := []Node{
  {
    ID: 10,
    Address: "foo:1234",
  },
  {
    ID: 20,
    Address: "bar:1234",
  },
  {
    ID: 30,
    Address: "frooby:1234",
  }}

  changes := compareChanges(old, new)
  if len(changes) != 1 { t.Fatal("No changes found!") }
  if changes[0].Action != UpdatedNode { t.Fatal("Invalid node type") }
  if changes[0].Node.ID != 30 { t.Fatalf("Wrong node ID %d", changes[0].Node.ID) }
  if changes[0].Node.Address != "frooby:1234" { t.Fatal("Invalid address") }
}
