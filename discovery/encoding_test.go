package discovery

import (
	"flag"
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	DebugMode = false
)

func TestDiscovery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Discovery Suite")
}

var _ = BeforeSuite(func() {
	flag.Set("logtostderr", "true")
	if DebugMode {
		flag.Set("v", "5")
	}
	flag.Parse()
})

var _ = Describe("Node Encoding", func() {
	It("Node List", func() {
		nl := &NodeList{New: []string{"node1"}}
		ml := marshalNodeList(nl)
		um := unmarshalNodeList(ml)
		Expect(nl.Equal(um)).Should(BeTrue())
	})

	It("Basic Node List", func() {
		nl := NodeList{
			New: []string{"node1"},
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
			New: []string{"node1", "node2", "node3"},
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
			New: []string{"node1", "node2", "node3"},
			Old: []string{"node1", "node3"},
		}
		ol := NodeList{
			New: []string{"node2"},
		}
		cfg := NodeConfig{
			Current:  &nl,
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

	It("Uniquify Node List", func() {
		nl := NodeList{
			New: []string{"node1", "node2", "node3"},
			Old: []string{"node1", "node3"},
		}
		ol := NodeList{
			New: []string{"node2"},
		}
		cfg := NodeConfig{
			Current:  &nl,
			Previous: &ol,
		}

		expected := []string{"node1", "node2", "node3"}
		un := cfg.GetUniqueNodes()
		fmt.Fprintf(GinkgoWriter, "Expected: %v\n", expected)
		fmt.Fprintf(GinkgoWriter, "Unique:   %v\n", un)
		Expect(len(un)).Should(Equal(len(expected)))
		for _, n := range expected {
			Expect(un).Should(ContainElement(n))
		}
	})
})
