package raft

import (
	"reflect"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config tests", func() {
	It("Config encoding empty", func() {
		cfg := Config{}
		buf := cfg.encode()
		dec, err := decodeRaftConfig(buf)
		Expect(err).Should(Succeed())
		Expect(reflect.DeepEqual(cfg, dec)).Should(BeTrue())
	})

	It("Config encoding", func() {
		cfg := Config{
			MinPurgeRecords:  999,
			MinPurgeDuration: 999 * time.Millisecond,
			HeartbeatTimeout: 1 * time.Second,
			ElectionTimeout:  2 * time.Second,
		}
		buf := cfg.encode()
		dec, err := decodeRaftConfig(buf)
		Expect(err).Should(Succeed())
		Expect(reflect.DeepEqual(cfg, dec)).Should(BeTrue())
	})

	It("Config Validation", func() {
		err := GetDefaultConfig().validate()
		Expect(err).Should(Succeed())
		cfg := Config{
			MinPurgeRecords:  999,
			MinPurgeDuration: 999 * time.Millisecond,
		}
		err = cfg.validate()
		Expect(err).ShouldNot(Succeed())
	})
})
