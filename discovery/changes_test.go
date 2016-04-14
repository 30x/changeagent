package discovery

import (
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

var _ = Describe("Change comparisions", func() {
  It("Empty node lists", func() {
    var old []Node
    var new []Node
    Expect(getChangeType(old, new)).Should(Equal(0))
  })

  It("Add node", func() {
    var old []Node

    new := []Node{{
      ID: 1,
      Address: "foo:1234",
    }}
    Expect(getChangeType(old, new)).Should(Equal(NodesChanged))
  })

  It("Add node 2", func() {
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
    Expect(getChangeType(old, new)).Should(Equal(NodesChanged))
  })

  It("Remove node", func() {
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
    Expect(getChangeType(old, new)).Should(Equal(NodesChanged))
  })

  It("Remove node 2", func() {
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
    Expect(getChangeType(old, new)).Should(Equal(NodesChanged))
  })

  It("Update node", func() {
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
    Expect(getChangeType(old, new)).Should(Equal(AddressesChanged))
  })

  It("Swap nodes", func() {
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
        ID: 40,
        Address: "frooby:1234",
      }}
    Expect(getChangeType(old, new)).Should(Equal(NodesChanged))
  })

  It("No change", func() {
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
        Address: "baz:1234",
      }}
    Expect(getChangeType(old, new)).Should(Equal(0))
  })
})
