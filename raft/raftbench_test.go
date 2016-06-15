package raft

import (
	"time"

	"github.com/30x/changeagent/storage"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Simple Benchmarks", func() {
	Measure("Appends", func(b Benchmarker) {
		waitForLeader()
		leader := getLeader()
		Expect(leader).ShouldNot(BeNil())

		lastIndex, _ := leader.GetLastIndex()
		proposal := "Benchmark entry"
		newEntry := storage.Entry{
			Timestamp: time.Now(),
			Data:      []byte(proposal),
		}

		b.Time("runtime", func() {
			newIndex, err := leader.Propose(newEntry)
			Expect(err).Should(Succeed())
			Expect(newIndex).Should(Equal(lastIndex + 1))
			waitForApply(newIndex, 3)
			lastIndex++
		})
	}, 50)
})
