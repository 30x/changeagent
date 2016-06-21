package discovery

import (
	"fmt"
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	//"golang.org/x/tools/benchmark/parse"
)

var _ = Describe("Static Discovery", func() {
	It("Discovery File", func() {
		d, err := ReadDiscoveryFile("./testfiles/testdisco", 0)
		Expect(err).Should(Succeed())
		defer d.Close()

		cfg := d.GetCurrentConfig()
		Expect(cfg.Previous).Should(BeNil())
		Expect(cfg.Current.Old).Should(BeNil())
		Expect(d.IsStandalone()).Should(BeFalse())

		n := cfg.Current.New

		Expect(len(n)).Should(Equal(2))
		Expect(n[0]).Should(Equal("localhost:1234"))
		Expect(n[1]).Should(Equal("localhost:2345"))

		nc := d.GetCurrentConfig()
		Expect(nc).Should(Equal(cfg))
		Expect(nc.Equal(cfg)).Should(BeTrue())
	})

	It("Fixed Discovery", func() {
		d := CreateStaticDiscovery([]string{"one", "two", "three"})
		defer d.Close()

		cfg := d.GetCurrentConfig()
		Expect(cfg.Previous).Should(BeNil())
		Expect(cfg.Current.Old).Should(BeNil())
		Expect(d.IsStandalone()).Should(BeFalse())

		n := cfg.Current.New
		fmt.Fprintf(GinkgoWriter, "Nodes: %v\n", n)

		Expect(len(n)).Should(Equal(3))
		Expect(n[0]).Should(Equal("one"))
		Expect(n[1]).Should(Equal("two"))
		Expect(n[2]).Should(Equal("three"))
	})

	It("Standalone Discovery", func() {
		d := CreateStandaloneDiscovery("one")
		defer d.Close()

		cfg := d.GetCurrentConfig()
		Expect(cfg.Previous).Should(BeNil())
		Expect(cfg.Current.Old).Should(BeNil())
		Expect(d.IsStandalone()).Should(BeTrue())

		n := cfg.Current.New
		fmt.Fprintf(GinkgoWriter, "Nodes: %v\n", n)

		Expect(len(n)).Should(Equal(1))
		Expect(n[0]).Should(Equal("one"))
	})

	It("Changes", func() {
		d := CreateStaticDiscovery([]string{"one", "two", "three"})
		defer d.Close()

		syncChanges := make(chan bool, 1)
		go func() {
			changeWatcher := d.Watch()
			syncChanges <- true
			change := <-changeWatcher
			Expect(change).Should(BeTrue())
			syncChanges <- true
		}()

		// Manually do what an SPI would do for itself,
		// but only after other channel is ready
		<-syncChanges
		d.AddNode("four")

		Eventually(syncChanges).Should(Receive(BeTrue()))

		n := d.GetCurrentConfig().Current.New
		Expect(len(n)).Should(Equal(4))
		Expect(d.IsStandalone()).Should(BeFalse())
		Expect(n[0]).Should(Equal("one"))
		Expect(n[1]).Should(Equal("two"))
		Expect(n[2]).Should(Equal("three"))
		Expect(n[3]).Should(Equal("four"))
	})

	It("Changes overwrite", func() {
		d := CreateStaticDiscovery([]string{"one", "two", "three"})
		defer d.Close()

		syncChanges := make(chan bool, 1)
		go func() {
			changeWatcher := d.Watch()
			syncChanges <- true
			change := <-changeWatcher
			Expect(change).Should(BeTrue())
			syncChanges <- true
		}()

		// Manually do what an SPI would do for itself,
		// but only after other channel is ready
		<-syncChanges
		d.AddNode("three")

		Eventually(syncChanges).ShouldNot(Receive(BeTrue()))

		n := d.GetCurrentConfig().Current.New
		Expect(len(n)).Should(Equal(3))
		Expect(n[0]).Should(Equal("one"))
		Expect(n[1]).Should(Equal("two"))
		Expect(n[2]).Should(Equal("three"))
	})

	It("Changes Delete", func() {
		d := CreateStaticDiscovery([]string{"one", "two", "three"})
		defer d.Close()

		syncChanges := make(chan bool, 1)
		go func() {
			changeWatcher := d.Watch()
			syncChanges <- true
			change := <-changeWatcher
			Expect(change).Should(BeTrue())
			syncChanges <- true
		}()

		// Manually do what an SPI would do for itself,
		// but only after other channel is ready
		<-syncChanges
		d.DeleteNode("two")

		Eventually(syncChanges).Should(Receive(BeTrue()))

		n := d.GetCurrentConfig().Current.New
		Expect(len(n)).Should(Equal(2))
		Expect(n[0]).Should(Equal("one"))
		Expect(n[1]).Should(Equal("three"))
	})

	It("Changes Delete No Effect", func() {
		d := CreateStaticDiscovery([]string{"one", "two", "three"})
		defer d.Close()

		syncChanges := make(chan bool, 1)
		go func() {
			changeWatcher := d.Watch()
			syncChanges <- true
			change := <-changeWatcher
			Expect(change).Should(BeTrue())
			syncChanges <- true
		}()

		// Manually do what an SPI would do for itself,
		// but only after other channel is ready
		<-syncChanges
		d.DeleteNode("four")

		Eventually(syncChanges).ShouldNot(Receive(BeTrue()))

		n := d.GetCurrentConfig().Current.New
		Expect(len(n)).Should(Equal(3))
		Expect(n[0]).Should(Equal("one"))
		Expect(n[1]).Should(Equal("two"))
		Expect(n[2]).Should(Equal("three"))
	})

	It("Changes two watchers", func() {
		d := CreateStaticDiscovery([]string{"one", "two", "three"})
		defer d.Close()

		syncChanges := make(chan bool, 1)

		go func() {
			changeWatcher := d.Watch()
			syncChanges <- true
			change := <-changeWatcher
			Expect(change).Should(BeTrue())
			syncChanges <- true
		}()

		go func() {
			changeWatcher := d.Watch()
			syncChanges <- true
			change := <-changeWatcher
			Expect(change).Should(BeTrue())
			syncChanges <- true
		}()

		// Manually do what an SPI would do for itself,
		// but only after other channel is ready
		<-syncChanges
		<-syncChanges
		d.AddNode("four")

		Eventually(syncChanges).Should(Receive(BeTrue()))
		Eventually(syncChanges).Should(Receive(BeTrue()))
	})

	It("Discovery file update", func() {
		err := copyFile("./testfiles/testdisco", "./testfiles/tmp")
		Expect(err).Should(Succeed())
		defer os.Remove("./testfiles/tmp")

		d, err := ReadDiscoveryFile("./testfiles/tmp", 10*time.Millisecond)
		Expect(err).Should(Succeed())
		defer d.Close()

		expected1 := &NodeList{
			New: []string{"localhost:1234", "localhost:2345"},
		}

		Expect(expected1.Equal(d.GetCurrentConfig().Current)).Should(BeTrue())

		syncChanges := make(chan bool, 1)
		go func() {
			expected2 := &NodeList{
				New: []string{"localhost:1234", "localhost:9999"},
			}

			changeWatcher := d.Watch()
			time.Sleep(time.Second)
			syncChanges <- true
			change := <-changeWatcher

			Expect(change).Should(BeTrue())
			Expect(expected2.Equal(d.GetCurrentConfig().Current)).Should(BeTrue())
			syncChanges <- true
		}()

		<-syncChanges
		err = copyFile("./testfiles/testdisco2", "./testfiles/tmp")

		Expect(err).Should(Succeed())
		Eventually(syncChanges).Should(Receive(BeTrue()))
	})

	It("Discovery file no change no update", func() {
		d, err := ReadDiscoveryFile("./testfiles/testdisco", 10*time.Millisecond)
		Expect(err).Should(Succeed())
		defer d.Close()
		syncChanges := make(chan bool, 1)
		go func() {
			changeWatcher := d.Watch()
			syncChanges <- true
			<-changeWatcher
			syncChanges <- true
		}()

		<-syncChanges
		Consistently(syncChanges).ShouldNot(Receive())
	})

	It("Discovery file change without changing contents no update", func() {
		err := copyFile("./testfiles/testdisco", "./testfiles/tmp")
		Expect(err).Should(Succeed())
		defer os.Remove("./testfiles/tmp")

		d, err := ReadDiscoveryFile("./testfiles/tmp", 10*time.Millisecond)
		Expect(err).Should(Succeed())
		defer d.Close()

		syncChanges := make(chan bool, 1)
		go func() {
			changeWatcher := d.Watch()
			syncChanges <- true
			<-changeWatcher
			syncChanges <- true
		}()

		<-syncChanges
		// Re-copy the file, which will change mtime but not change contents
		err = copyFile("./testfiles/testdisco", "./testfiles/tmp")
		Expect(err).Should(Succeed())
		Consistently(syncChanges).ShouldNot(Receive())
	})

})

func copyFile(src string, dst string) error {
	dstFile, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	srcFile, err := os.OpenFile(src, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	stat, err := srcFile.Stat()
	if err != nil {
		return err
	}

	buf := make([]byte, stat.Size())
	_, err = srcFile.Read(buf)
	if err != nil {
		return err
	}
	_, err = dstFile.Write(buf)
	if err != nil {
		return err
	}
	stat, _ = dstFile.Stat()
	return nil
}
