package stress

import (
  "os"
  "path"
  "testing"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

var testPids map[int]os.Process
var dataDir string

func TestRaft(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Raft Suite")
}

var _ = BeforeSuite(func() {
  testPids = make(map[int]os.Process)
  dataDir = path.Join(".", "stressData")
  err := os.MkdirAll(dataDir, 0777)
  Expect(err).Should(Succeed())
})

var _ = AfterSuite(func() {
  for _, proc := range(testPids) {
    proc.Kill()
    proc.Wait()
  }

  err := os.RemoveAll(dataDir)
  Expect(err).Should(Succeed())
})
