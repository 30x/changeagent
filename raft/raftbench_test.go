package raft

import (
	"time"

	"github.com/30x/changeagent/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	shortPollInterval = time.Millisecond
)

var _ = Describe("Simple Benchmarks", func() {
	Measure("Appends", func(b Benchmarker) {
		assertOneLeader()
		leader := getLeader()
		Expect(leader).ShouldNot(BeNil())

		lastIndex, _ := leader.GetLastIndex()
		proposal := "Benchmark entry"
		newEntry := common.Entry{
			Timestamp: time.Now(),
			Data:      []byte(proposal),
		}

		b.Time("runtime", func() {
			newIndex, err := leader.Propose(&newEntry)
			Expect(err).Should(Succeed())
			Expect(newIndex).Should(Equal(lastIndex + 1))
			waitForApply(newIndex, clusterSize, shortPollInterval)
			lastIndex++
		})
	}, 50)
})
