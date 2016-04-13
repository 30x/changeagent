package discovery

import (
  "fmt"
  "testing"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

func TestDiscovery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Discovery Suite")
}

var _ = Describe("Node Encoding", func() {
  node1 := Node{
    ID: 1,
    Address: "localhost:123",
    State: 1,
  }
  node2 := Node{
    ID: 2,
    Address: "localhost:124",
    State: 0,
  }
  node3 := Node{
    ID: 3,
    Address: "localhost:111",
    State: -1,
  }

  It("One Node", func() {
    mn1 := marshalNodes([]Node{node1})
    um1 := unmarshalNodes(mn1)
    Expect(node1.Equal(um1[0])).Should(BeTrue())
    Expect(node1.String()).Should(Equal(um1[0].String()))
  })

  It("Two Nodes", func() {
    nn := []Node{node1, node2}
    mn := marshalNodes(nn)
    um := unmarshalNodes(mn)
    fmt.Fprintf(GinkgoWriter, "Before: %v\n", nn)
    fmt.Fprintf(GinkgoWriter, "After:  %v\n", um)
    Expect(node1.Equal(um[0])).Should(BeTrue())
    Expect(node2.Equal(um[1])).Should(BeTrue())
  })

  It("Node List", func() {
    nl := &NodeList{New: []Node{node1}}
    ml := marshalNodeList(nl)
    um := unmarshalNodeList(ml)
    Expect(nl.Equal(um)).Should(BeTrue())
  })

  It("Basic Node List", func() {
    nl := NodeList{
      New: []Node{node1},
    }
    cfg := NodeConfig{
      Current: &nl,
    }

    buf, err := EncodeConfig(&cfg)
    Expect(err).Should(Succeed())

    decCfg, err := DecodeConfig(buf)
    Expect(err).Should(Succeed())

    fmt.Fprintf(GinkgoWriter, "Before: %s\n", &cfg)
    fmt.Fprintf(GinkgoWriter, "After: %s\n", decCfg)
    Expect(decCfg.Equal(&cfg)).Should(BeTrue())
  })

  It("Longer NodeList", func() {
    nl := NodeList{
      New: []Node{node1, node2, node3},
    }
    cfg := NodeConfig{
      Current: &nl,
    }

    buf, err := EncodeConfig(&cfg)
    Expect(err).Should(Succeed())

    decCfg, err := DecodeConfig(buf)
    Expect(err).Should(Succeed())

    fmt.Fprintf(GinkgoWriter, "Before: %s\n", &cfg)
    fmt.Fprintf(GinkgoWriter, "After: %s\n", decCfg)
    Expect(decCfg.Equal(&cfg)).Should(BeTrue())
  })

  It("Complex Node List", func() {
    nl := NodeList{
      New: []Node{node1, node2, node3},
      Old: []Node{node1, node3},
    }
    ol := NodeList{
      New: []Node{node2},
    }
    cfg := NodeConfig{
      Current: &nl,
      Previous: &ol,
    }

    buf, err := EncodeConfig(&cfg)
    Expect(err).Should(Succeed())

    decCfg, err := DecodeConfig(buf)
    Expect(err).Should(Succeed())

    fmt.Fprintf(GinkgoWriter, "Before: %s\n", &cfg)
    fmt.Fprintf(GinkgoWriter, "After: %s\n", decCfg)
    Expect(decCfg.Equal(&cfg)).Should(BeTrue())
  })
})