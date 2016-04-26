package storage

import (
  "bytes"
  "time"
  "testing/quick"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

var _ = Describe("Conversion", func() {
  It("Int Key", func() {
    s := testIntKey(1, 1234)
    Expect(s).Should(BeTrue())
    err := quick.Check(testIntKey, nil)
    Expect(err).Should(Succeed())
  })

  It("String Key", func() {
    s := testStringKey(1, "foo")
    Expect(s).Should(BeTrue())
    err := quick.Check(testStringKey, nil)
    Expect(err).Should(Succeed())
  })

  It("Entry", func() {
    s := testEntry(123, time.Now().UnixNano(), "", []byte("Hello!"))
    Expect(s).Should(BeTrue())
    s = testEntry(123, time.Now().UnixNano(), "baz", []byte("Hello!"))
    Expect(s).Should(BeTrue())

    err := quick.Check(testEntry, nil)
    Expect(err).Should(Succeed())
  })

  It("Entry Key Compare", func() {
    s := testEntryIndexCompare(1, 1)
    Expect(s).Should(BeTrue())
    s = testEntryIndexCompare(1, 2)
    Expect(s).Should(BeTrue())
    s = testEntryIndexCompare(2, 1)
    Expect(s).Should(BeTrue())

    err := quick.Check(testEntryIndexCompare, nil)
    Expect(err).Should(Succeed())
  })

  It("Metadata Key Compare", func() {
    s := testMetadataIndexCompare("foo", "foo")
    Expect(s).Should(BeTrue())
    s = testMetadataIndexCompare("foo", "bar")
    Expect(s).Should(BeTrue())
    s = testMetadataIndexCompare("foo", "")
    Expect(s).Should(BeTrue())
    s = testMetadataIndexCompare("", "foo")
    Expect(s).Should(BeTrue())

    err := quick.Check(testMetadataIndexCompare, nil)
    Expect(err).Should(Succeed())
  })

  It("Entry string", func() {
    tst := time.Now()
    e := &Entry{
      Index: 22,
      Term: 1,
      Timestamp: tst,
      Data: []byte("Foo!"),
      Tags: []string{"one", "two"},
    }
    s := e.String()
    Expect(s).Should(ContainSubstring("Index: 22"))
  })
})

func testIntKey(kt int, key uint64) bool {
  if kt < 0 || kt > (1 << 8) {
    return true
  }
  keyBytes, keyLen := uintToKey(kt, key)
  Expect(keyLen).Should(BeEquivalentTo(9))
  defer freePtr(keyBytes)

  newType, newKey, err := keyToUint(keyBytes, keyLen)
  Expect(err).Should(Succeed())
  Expect(newType).Should(Equal(kt))
  Expect(newKey).Should(Equal(key))
  return true
}

func testStringKey(kt int, key string) bool {
  if kt < 0 || kt > (1 << 8) {
    return true
  }
  keyBytes, keyLen := stringToKey(kt, key)
  Expect(keyLen > 0).Should(BeTrue())
  defer freePtr(keyBytes)

  newType, newKey, err := keyToString(keyBytes, keyLen)
  Expect(err).Should(Succeed())
  Expect(newType).Should(Equal(kt))
  Expect(newKey).Should(Equal(key))
  return true
}

func testEntry(term uint64, ts int64, key string, data []byte) bool {
  tst := time.Unix(0, ts)
  e := &Entry{
    Term: term,
    Timestamp: tst,
    Data: data,
    Tags: []string{"one", "two"},
  }
  bb, len := entryToPtr(e)
  defer freePtr(bb)

  re, err := ptrToEntry(bb, len)
  Expect(err).Should(Succeed())
  Expect(re.Term).Should(Equal(e.Term))
  Expect(re.Timestamp).Should(Equal(e.Timestamp))
  Expect(re.Tags).Should(Equal(e.Tags))
  Expect(bytes.Equal(re.Data, data)).Should(BeTrue())
  return true
}

func testEntryIndexCompare(k1, k2 uint64) bool {
  keyBytes1, keyLen1 := uintToKey(EntryKey, k1)
  defer freePtr(keyBytes1)
  keyBytes2, keyLen2 := uintToKey(EntryKey, k2)
  defer freePtr(keyBytes2)

  cmp := compareKeys(keyBytes1, keyLen1, keyBytes2, keyLen2)

  if k1 < k2 {
    return cmp < 0
  }
  if k1 > k2 {
    return cmp > 0
  }
  return cmp == 0
}

func testMetadataIndexCompare(k1, k2 string) bool {
  keyBytes1, keyLen1 := stringToKey(MetadataKey, k1)
  defer freePtr(keyBytes1)
  keyBytes2, keyLen2 := stringToKey(MetadataKey, k2)
  defer freePtr(keyBytes2)

  cmp := compareKeys(keyBytes1, keyLen1, keyBytes2, keyLen2)

  if k1 < k2 {
    return cmp < 0
  }
  if k1 > k2 {
    return cmp > 0
  }
  return cmp == 0
}
