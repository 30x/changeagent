package raft

import (
	"reflect"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Node List tests", func() {
	It("Encode empty  list", func() {
		nl := NodeList{}
		buf := nl.encode()
		rnl, err := decodeNodeList(buf)
		Expect(err).Should(Succeed())
		Expect(reflect.DeepEqual(nl, rnl)).Should(BeTrue())
	})

	It("Encode basic list", func() {
		nl := NodeList{
			Current: []Node{
				{NodeID: 1234, Address: "http://foo.com"},
			},
		}
		buf := nl.encode()
		rnl, err := decodeNodeList(buf)
		Expect(err).Should(Succeed())
		Expect(reflect.DeepEqual(nl, rnl)).Should(BeTrue())
	})

	It("Encode larger list", func() {
		nl := NodeList{
			Current: []Node{
				{NodeID: 1234, Address: "http://foo.com"},
				{NodeID: 2345, Address: "http://bar.com"},
			},
			Next: []Node{
				{NodeID: 2345, Address: "http://bar.com"},
			},
		}
		buf := nl.encode()
		rnl, err := decodeNodeList(buf)
		Expect(err).Should(Succeed())
		Expect(reflect.DeepEqual(nl, rnl)).Should(BeTrue())
	})
})
