package storage

import (
  "flag"
  "testing"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

func TestStorage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Storage Suite")
}

var _ = BeforeSuite(func() {
  flag.Set("logtostderr", "true")
  flag.Parse()
})
