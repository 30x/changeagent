package config

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestHooks(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Config Suite")
}

var testDir string

var _ = BeforeSuite(func() {
	var err error
	testDir, err = ioutil.TempDir("", "config")
	Expect(err).Should(Succeed())
})

var _ = AfterSuite(func() {
	os.RemoveAll(testDir)
})

var _ = Describe("Config Tests", func() {
	It("Read Write Default", func() {
		dflt := GetDefaultConfig()
		Expect(dflt.ShouldPurgeRecords()).ShouldNot(BeTrue())
		fn := path.Join(testDir, "default")
		err := dflt.StoreFile(fn)
		Expect(err).Should(Succeed())
		result := GetDefaultConfig()
		err = result.LoadFile(fn)
		Expect(err).Should(Succeed())
		compare(dflt, result)
	})

	It("Read file", func() {
		cfg := GetDefaultConfig()
		err := cfg.LoadFile("./test1.yaml")
		Expect(err).Should(Succeed())
		Expect(cfg.MinPurgeRecords()).Should(BeEquivalentTo(123))
		Expect(cfg.ShouldPurgeRecords()).Should(BeTrue())

		fn := path.Join(testDir, "test1")
		err = cfg.StoreFile(fn)
		Expect(err).Should(Succeed())
		result := GetDefaultConfig()
		err = result.LoadFile(fn)
		Expect(err).Should(Succeed())
		compare(cfg, result)
	})

	It("Read file with hooks", func() {
		cfg := GetDefaultConfig()
		err := cfg.LoadFile("./test2.yaml")
		Expect(err).Should(Succeed())
		Expect(cfg.MinPurgeRecords()).Should(BeEquivalentTo(123))
		Expect(cfg.WebHooks()[0].URI).Should(Equal("http://foo.com"))
		Expect(cfg.WebHooks()[1].URI).Should(Equal("http://bar.com"))

		fn := path.Join(testDir, "test2")
		err = cfg.StoreFile(fn)
		Expect(err).Should(Succeed())
		result := GetDefaultConfig()
		err = result.LoadFile(fn)
		Expect(err).Should(Succeed())
		compare(cfg, result)
	})
})

func compare(s1, s2 *State) {
	Expect(s1.MinPurgeRecords()).Should(Equal(s2.MinPurgeRecords()))
	Expect(s1.MinPurgeDuration()).Should(Equal(s2.MinPurgeDuration()))
	Expect(s1.HeartbeatTimeout()).Should(Equal(s2.HeartbeatTimeout()))
	Expect(s1.ElectionTimeout()).Should(Equal(s2.ElectionTimeout()))
	Expect(len(s1.WebHooks())).Should(Equal(len(s2.WebHooks())))
	for i := range s1.WebHooks() {
		Expect(s1.WebHooks()[i].URI).Should(Equal(s2.WebHooks()[i].URI))
		if len(s1.WebHooks()[i].Headers) > 0 {
			Expect(s1.WebHooks()[i].Headers).Should(BeEquivalentTo(s2.WebHooks()[i].Headers))
		}
	}
}
