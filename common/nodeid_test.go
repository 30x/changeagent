package common

import (
	"testing"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCommon(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Common Suite")
}

var _ = Describe("Node ID Test", func() {
	It("Node ID Encoding", func() {
		err := quick.Check(testID, nil)
		Expect(err).Should(Succeed())
	})
	It("Invalid node ID", func() {
		Expect(ParseNodeID("!foo!")).Should(BeZero())
		Expect(ParseNodeID("")).Should(BeZero())
	})
})

func testID(id uint64) bool {
	nid := NodeID(id)
	Expect(ParseNodeID(nid.String())).Should(Equal(nid))
	return true
}
