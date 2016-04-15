package discovery

import (
  "fmt"
  "time"
  "os"
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

    n := cfg.Current.New

    Expect(len(n)).Should(Equal(2))
    Expect(n[0].ID).Should(BeEquivalentTo(1))
    Expect(n[0].Address).Should(Equal("localhost:1234"))
    Expect(cfg.GetAddress(1)).Should(Equal("localhost:1234"))
    Expect(n[1].ID).Should(BeEquivalentTo(2))
    Expect(n[1].Address).Should(Equal("localhost:2345"))
    Expect(n[1].Address).Should(Equal("localhost:2345"))
    Expect(cfg.GetAddress(2)).Should(Equal("localhost:2345"))

    tn1 := Node{
      ID: 1,
      Address: "localhost:1234",
    }
    tn2 := Node{
      ID: 2,
      Address: "localhost:2345",
    }
    Expect(tn1.Equal(n[0])).Should(BeTrue())
    Expect(tn2.Equal(n[1])).Should(BeTrue())
    Expect(tn1.Equal(n[1])).Should(BeFalse())
    Expect(tn2.Equal(n[0])).Should(BeFalse())
    Expect(tn1.String()).Should(Equal(n[0].String()))
    Expect(tn2.String()).Should(Equal(n[1].String()))

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

    n := cfg.Current.New
    fmt.Fprintf(GinkgoWriter, "Nodes: %v\n", n)

    Expect(len(n)).Should(Equal(3))

    tn1 := Node{
      ID: 1,
      State: 2,
      Address: "one",
    }
    tn2 := Node{
      ID: 2,
      State: 2,
      Address: "two",
    }
    tn3 := Node{
      ID: 3,
      State: 2,
      Address: "three",
    }

    Expect(tn1.Equal(n[0])).Should(BeTrue())
    Expect(tn2.Equal(n[1])).Should(BeTrue())
    Expect(tn3.Equal(n[2])).Should(BeTrue())
  })

  It("Changes", func() {
    d := CreateStaticDiscovery([]string{"one", "two", "three"})
    defer d.Close()

    syncChanges := make(chan bool, 1)
    go func() {
      changeWatcher := d.Watch()
      syncChanges <- true
      change := <- changeWatcher
      Expect(change).Should(Equal(NodesChanged))
      syncChanges <- true
    }()

    // Manually do what an SPI would do for itself,
    // but only after other channel is ready
    <- syncChanges
    d.SetNode(Node{ID: 4, Address: "four"})

    Eventually(syncChanges).Should(Receive(BeTrue()))
  })

  It("Changes two watchers", func() {
    d := CreateStaticDiscovery([]string{"one", "two", "three"})
    defer d.Close()

    syncChanges := make(chan bool, 1)

    go func() {
      changeWatcher := d.Watch()
      syncChanges <- true
      change := <- changeWatcher
      Expect(change).Should(Equal(NodesChanged))
      syncChanges <- true
    }()

    go func() {
      changeWatcher := d.Watch()
      syncChanges <- true
      change := <- changeWatcher
      Expect(change).Should(Equal(NodesChanged))
      syncChanges <- true
    }()

    // Manually do what an SPI would do for itself,
    // but only after other channel is ready
    <- syncChanges
    <- syncChanges
    d.SetNode(Node{ID: 4, Address: "four"})

    Eventually(syncChanges).Should(Receive(BeTrue()))
    Eventually(syncChanges).Should(Receive(BeTrue()))
  })

  It("Changes Address only", func() {
    d := CreateStaticDiscovery([]string{"one", "two", "three"})
    defer d.Close()

    syncChanges := make(chan bool, 1)
    go func() {
      changeWatcher := d.Watch()
      syncChanges <- true
      change := <- changeWatcher
      Expect(change).Should(Equal(AddressesChanged))
      syncChanges <- true
    }()

    // Manually do what an SPI would do for itself,
    // but only after other channel is ready
    <- syncChanges
    d.SetNode(Node{ID: 3, Address: "four"})

    Eventually(syncChanges).Should(Receive(BeTrue()))
  })

  It("Discovery file update", func() {
    err := copyFile("./testfiles/testdisco", "./testfiles/tmp")
    Expect(err).Should(Succeed())
    defer os.Remove("./testfiles/tmp")

    d, err := ReadDiscoveryFile("./testfiles/tmp", 250 * time.Millisecond)
    Expect(err).Should(Succeed())
    defer d.Close()

    expected1 := &NodeList{
      New: []Node{
        {ID: 1, Address: "localhost:1234"},
        {ID: 2, Address: "localhost:2345"},
      },
    }

    Expect(expected1.Equal(d.GetCurrentConfig().Current)).Should(BeTrue())

    syncChanges := make(chan bool, 1)
    go func() {
      expected2 := &NodeList{
        New: []Node{
          {ID: 1, Address: "localhost:1234"},
          {ID: 2, Address: "localhost:9999"},
        },
      }

      changeWatcher := d.Watch()
      syncChanges <- true
      change := <- changeWatcher

      Expect(change).Should(Equal(AddressesChanged))
      Expect(expected2.Equal(d.GetCurrentConfig().Current)).Should(BeTrue())
      syncChanges <- true
    }()

    <- syncChanges
    err = copyFile("./testfiles/testdisco2", "./testfiles/tmp")

    Expect(err).Should(Succeed())
    Eventually(syncChanges).Should(Receive(BeTrue()))
  })
})

func copyFile(src string, dst string) error {
  dstFile, err := os.OpenFile(dst, os.O_RDWR | os.O_CREATE, 0666)
  if err != nil { return err }
  defer dstFile.Close()

  srcFile, err := os.OpenFile(src, os.O_RDONLY, 0)
  if err != nil { return err }
  defer srcFile.Close()
  stat, err := srcFile.Stat()
  if err != nil { return err }

  buf := make([]byte, stat.Size())
  _, err = srcFile.Read(buf)
  if err != nil { return err }
  _, err = dstFile.Write(buf)
  if err != nil { return err }
  return nil
}
