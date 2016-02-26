package storage

import (
  "flag"
  "testing"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

var indexTestDb Storage

func TestStorage(t *testing.T) {
  RegisterFailHandler(Fail)
  RunSpecs(t, "Storage Suite")
}

var _ = BeforeSuite(func() {
  flag.Set("logtostderr", "true")
  flag.Parse()

  var err error
  indexTestDb, err = CreateRocksDBStorage("./indextestleveldb", 1000)
  Expect(err).Should(Succeed())
})

var _ = AfterSuite(func() {
  indexTestDb.Close()
  err := indexTestDb.Delete()
  Expect(err).Should(Succeed())
})

