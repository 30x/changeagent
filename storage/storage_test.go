package storage

import (
  "fmt"
  "time"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

var _ = Describe("Storage Main Test", func() {
  It("Open and reopen", func() {
    stor, err := CreateRocksDBStorage("./openleveldb", 1000)
    Expect(err).Should(Succeed())
    stor.Close()
    stor, err = CreateRocksDBStorage("./openleveldb", 1000)
    Expect(err).Should(Succeed())
    stor.Close()
    err = stor.Delete()
    Expect(err).Should(Succeed())
  })

  It("Test metadata", func() {
    stor, err := CreateRocksDBStorage("./metadatatestleveldb", 1000)
    Expect(err).Should(Succeed())
    defer func() {
      stor.Close()
      err := stor.Delete()
      Expect(err).Should(Succeed())
    }()
    metadataTest(stor)
  })

  It("Test entries", func() {
    stor, err := CreateRocksDBStorage("./entrytestleveldb", 1000)
    Expect(err).Should(Succeed())
    defer func() {
      //stor.Dump(os.Stdout, 25)
      stor.Close()
      err := stor.Delete()
      Expect(err).Should(Succeed())
    }()
    entriesTest(stor)
  })
})

func metadataTest(stor Storage) {
  err := stor.SetMetadata(1, 123)
  Expect(err).Should(Succeed())
  val, err := stor.GetMetadata(1)
  Expect(err).Should(Succeed())
  Expect(val).Should(BeEquivalentTo(123))

  err = stor.SetMetadata(1, 234)
  Expect(err).Should(Succeed())
  val, err = stor.GetMetadata(1)
  Expect(err).Should(Succeed())
  Expect(val).Should(BeEquivalentTo(234))

  bval := []byte("Hello, Metadata World!")
  err = stor.SetRawMetadata(2, bval)
  Expect(err).Should(Succeed())
  bresult, err := stor.GetRawMetadata(2)
  Expect(err).Should(Succeed())
  Expect(bresult).Should(Equal(bval))

  val, err = stor.GetMetadata(999)
  Expect(err).Should(Succeed())
  Expect(val).Should(BeEquivalentTo(0))
}

func entriesTest(stor Storage) {
  max, term, err := stor.GetLastIndex()
  Expect(err).Should(Succeed())
  Expect(max).Should(BeEquivalentTo(0))
  Expect(term).Should(BeEquivalentTo(0))

  entries, err := stor.GetEntries(0, 2, everTrue)
  Expect(err).Should(Succeed())
  Expect(len(entries)).Should(BeEquivalentTo(0))

  entry, err := stor.GetEntry(1)
  Expect(err).Should(Succeed())
  Expect(entry).Should(BeNil())

  hello := []byte("Hello!")

  // Put in some metadata to confuse the index a bit
  err = stor.SetMetadata(1, 1)
  Expect(err).Should(Succeed())

  max, term, err = stor.GetLastIndex()
  Expect(err).Should(Succeed())
  Expect(max).Should(BeEquivalentTo(0))
  Expect(term).Should(BeEquivalentTo(0))

  entry1 := &Entry{
    Index: 1,
    Term: 1,
    Timestamp: time.Now(),
  }
  fmt.Fprintf(GinkgoWriter, "Collection 1 is %v", entry1.Collection)

  err = stor.AppendEntry(entry1)
  Expect(err).Should(Succeed())

  entry2 := &Entry{
    Index: 2,
    Term: 1,
    Timestamp: time.Now(),
    Data: hello,
  }
  err = stor.AppendEntry(entry2)
  Expect(err).Should(Succeed())

  re, err := stor.GetEntry(1)
  Expect(err).Should(Succeed())
  fmt.Fprintf(GinkgoWriter, "Decoded collection 1 is %v", re.Collection)
  compareEntries(re, entry1)

  re, err = stor.GetEntry(2)
  Expect(err).Should(Succeed())
  compareEntries(entry2, re)

  max, term, err = stor.GetLastIndex()
  Expect(err).Should(Succeed())
  Expect(max).Should(BeEquivalentTo(2))
  Expect(term).Should(BeEquivalentTo(1))

  ets, err := stor.GetEntryTerms(1)
  Expect(err).Should(Succeed())
  Expect(ets[1]).Should(BeEquivalentTo(1))
  Expect(ets[2]).Should(BeEquivalentTo(1))

  ets, err = stor.GetEntryTerms(3)
  Expect(err).Should(Succeed())
  Expect(len(ets)).Should(Equal(0))

  entries, err = stor.GetEntries(0, 3, everTrue)
  Expect(err).Should(Succeed())
  Expect(len(entries)).Should(Equal(2))
  compareEntries(entry1, &entries[0])
  compareEntries(entry2, &entries[1])

  err = stor.DeleteEntries(1)
  Expect(err).Should(Succeed())

  ets, err = stor.GetEntryTerms(1)
  Expect(err).Should(Succeed())
  Expect(len(ets)).Should(Equal(0))

  err = stor.DeleteEntries(1)
  Expect(err).Should(Succeed())
}

func compareEntries(e1 *Entry, e2 *Entry) {
  Expect(e1.Index).Should(Equal(e2.Index))
  Expect(e1.Term).Should(Equal(e2.Term))
  Expect(e1.Timestamp).Should(Equal(e2.Timestamp))
  Expect(e1.Collection).Should(Equal(e2.Collection))
  Expect(e1.Key).Should(Equal(e2.Key))
  Expect(e1.Data).Should(Equal(e2.Data))
}

func everTrue(e *Entry) bool {
  return true
}
