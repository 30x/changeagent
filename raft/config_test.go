package raft

import (
	"reflect"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config tests", func() {
	It("Config encoding empty", func() {
		cfg := &Config{}
		buf := cfg.encode()
		dec, err := decodeRaftConfig(buf)
		Expect(err).Should(Succeed())
		Expect(reflect.DeepEqual(cfg, dec)).Should(BeTrue())
	})

	It("Config encoding", func() {
		cfg := &Config{
			MinPurgeRecords:  999,
			MinPurgeDuration: 999 * time.Millisecond,
		}
		buf := cfg.encode()
		dec, err := decodeRaftConfig(buf)
		Expect(err).Should(Succeed())
		Expect(reflect.DeepEqual(cfg, dec)).Should(BeTrue())
	})
})
