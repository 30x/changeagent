package storage

import (
  "bytes"
  "fmt"
  "time"
  "testing/quick"
  "github.com/satori/go.uuid"
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

  It("Int", func() {
    s := testInt(1234)
    Expect(s).Should(BeTrue())
    err := quick.Check(testInt, nil)
    Expect(err).Should(Succeed())
  })

  It("String value", func() {
    s := testStringValue("foo")
    Expect(s).Should(BeTrue())
    err := quick.Check(testStringValue, nil)
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

  It("Index entry", func() {
    s := testIndexEntry("foo")
    Expect(s).Should(BeTrue())

    err := quick.Check(testIndexEntry, nil)
    Expect(err).Should(Succeed())
  })

  It("Tenant Index entry", func() {
    err := quick.Check(testTenantIndex, nil)
    Expect(err).Should(Succeed())
  })

  It("Start index", func() {
    id := uuid.NewV4()
    bb, len, err := startIndexToPtr(&id)
    Expect(err).Should(Succeed())
    defer freePtr(bb)
    Expect(len).ShouldNot(BeZero())

    newId, newType, newKey, err := ptrToIndexKey(bb, len)
    Expect(err).Should(Succeed())
    Expect(newType).Should(BeEquivalentTo(startRange))
    Expect(newKey).Should(Equal(""))
    Expect(newId.Bytes()).Should(Equal(id.Bytes()))

    keyBuf, keyLen, err := indexKeyToPtr(&id, "foo")
    Expect(err).Should(Succeed())
    defer freePtr(keyBuf)
    cmp := compareKeys(bb, len, keyBuf, keyLen)
    Expect(cmp).Should(BeNumerically("<", 0))
    cmp = compareKeys(keyBuf, keyLen, bb, len)
    Expect(cmp).Should(BeNumerically(">", 0))
    cmp = compareKeys(bb, len, bb, len)
    Expect(cmp).Should(BeZero())
  })

  It("End index", func() {
    id := uuid.NewV4()
    bb, len, err := endIndexToPtr(&id)
    Expect(err).Should(Succeed())
    defer freePtr(bb)
    Expect(len).ShouldNot(BeZero())

    newId, newType, newKey, err := ptrToIndexKey(bb, len)
    Expect(err).Should(Succeed())
    Expect(newType).Should(BeEquivalentTo(endRange))
    Expect(newKey).Should(Equal(""))
    Expect(newId.Bytes()).Should(Equal(id.Bytes()))

    keyBuf, keyLen, err := indexKeyToPtr(&id, "foo")
    Expect(err).Should(Succeed())
    defer freePtr(keyBuf)

    cmp := compareKeys(bb, len, keyBuf, keyLen)
    Expect(cmp).Should(BeNumerically(">", 0))
    cmp = compareKeys(keyBuf, keyLen, bb, len)
    Expect(cmp).Should(BeNumerically("<", 0))
    cmp = compareKeys(bb, len, bb, len)
    Expect(cmp).Should(BeZero())
  })

  It("Key Compare", func() {
    s := testKeyCompare(true, 1, true, 1)
    Expect(s).Should(BeTrue())
    s = testKeyCompare(true, 1, false, 1)
    Expect(s).Should(BeTrue())
    s = testKeyCompare(false, 1, true, 1)
    Expect(s).Should(BeTrue())
    s = testKeyCompare(false, 1, false, 2)
    Expect(s).Should(BeTrue())
    s = testKeyCompare(false, 2, false, 1)
    Expect(s).Should(BeTrue())

    err := quick.Check(testKeyCompare, nil)
    Expect(err).Should(Succeed())
  })

  It("Index compare", func() {
    s := testIndexCompare("foo", "foo")
    Expect(s).Should(BeTrue())
    s = testIndexCompare("foo", "bar")
    Expect(s).Should(BeTrue())
    s = testIndexCompare("bar", "foo")
    Expect(s).Should(BeTrue())
    s = testIndexCompare("barrrrrrr", "foo")
    Expect(s).Should(BeTrue())
    s = testIndexCompare("bar", "foooooooo")
    Expect(s).Should(BeTrue())

    err := quick.Check(testIndexCompare, nil)
    Expect(err).Should(Succeed())
  })

  It("Tenant index compare", func() {
    s := testTenantIndexCompare(123, 123, true)
    Expect(s).Should(BeTrue())
    s = testTenantIndexCompare(123, 123, false)
    Expect(s).Should(BeTrue())
    s = testTenantIndexCompare(123, 456, true)
    Expect(s).Should(BeTrue())
    s = testTenantIndexCompare(123, 456, false)
    Expect(s).Should(BeTrue())

    err := quick.Check(testTenantIndexCompare, nil)
    Expect(err).Should(Succeed())
  })

  It("UUID Value", func() {
    id := uuid.NewV4()
    bb, len := uuidToPtr(&id)
    defer freePtr(bb)

    newId, err := ptrToUuid(bb, len)
    Expect(err).Should(Succeed())
    Expect(newId.Bytes()).Should(Equal(id.Bytes()))
  })
})

func testIntKey(kt int, key uint64) bool {
  if kt < 0 || kt > (1 << 8) {
    return true
  }
  keyBytes, keyLen := uintToKey(kt, key)
  if keyLen != 9 {
    return false
  }
  defer freePtr(keyBytes)

  newType, newKey, err := keyToUint(keyBytes, keyLen)
  if err != nil { return false}
  if newType != kt { return false }
  if newKey != key { return false }
  return true
}

func testInt(val uint64) bool {
  bytes, len := uintToPtr(val)
  defer freePtr(bytes)

  result := ptrToUint(bytes, len)
  if result != val { return false }
  return true
}

func testStringValue(str string) bool {
  bytes, len := stringToPtr(str)
  defer freePtr(bytes)

  result, err := ptrToString(bytes, len)
  Expect(err).Should(Succeed())
  Expect(result).Should(Equal(str))
  return true
}

func testEntry(term uint64, ts int64, key string, data []byte) bool {
  tst := time.Unix(0, ts)
  coll := uuid.NewV4()
  e := &Entry{
    Term: term,
    Timestamp: tst,
    Collection: &coll,
    Key: key,
    Data: data,
  }
  bb, len := entryToPtr(e)
  defer freePtr(bb)

  re, err := ptrToEntry(bb, len)
  Expect(err).Should(Succeed())
  Expect(re.Term).Should(Equal(e.Term))
  Expect(re.Timestamp).Should(Equal(e.Timestamp))
  Expect(re.Collection).Should(Equal(e.Collection))
  Expect(re.Key).Should(Equal(e.Key))
  Expect(re.Data).Should(Equal(re.Data))
  return true
}

func testIndexEntry(key string) bool {
  id := uuid.NewV4()
  bb, len, err := indexKeyToPtr(&id, key)
  Expect(err).Should(Succeed())
  defer freePtr(bb)

  newId, newType, newKey, err := ptrToIndexKey(bb, len)
  Expect(err).Should(Succeed())
  Expect(newType).Should(BeEquivalentTo(0))
  Expect(newKey).Should(Equal(key))
  Expect(newId.Bytes()).Should(Equal(id.Bytes()))
  return true
}

func testTenantIndex(ix uint64) bool {
  id := uuid.NewV4()
  bb, len := tenantIndexToPtr(&id, ix)
  defer freePtr(bb)

  newId, newIx, err := ptrToTenantIndex(bb, len)
  Expect(err).Should(Succeed())
  Expect(newIx).Should(Equal(ix))
  Expect(newId.Bytes()).Should(Equal(id.Bytes()))
  return true
}

func testKeyCompare(isMetadata1 bool, k1 uint64, isMetadata2 bool, k2 uint64) bool {
  var kt1 int
  if isMetadata1 {
    kt1 = MetadataKey
  } else {
    kt1 = EntryKey
  }

  var kt2 int
  if isMetadata2 {
    kt2 = MetadataKey
  } else {
    kt2 = EntryKey
  }

  keyBytes1, keyLen1 := uintToKey(kt1, k1)
  defer freePtr(keyBytes1)
  keyBytes2, keyLen2 := uintToKey(kt2, k2)
  defer freePtr(keyBytes2)

  cmp := compareKeys(keyBytes1, keyLen1, keyBytes2, keyLen2)

  if kt1 < kt2 {
    return cmp < 0
  }
  if kt1 > kt2 {
    return cmp > 0
  }
  if k1 < k2 {
    return cmp < 0
  }
  if k1 > k2 {
    return cmp > 0
  }
  return cmp == 0
}

func testIndexCompare(key1, key2 string) bool {
  id1 := uuid.NewV4()
  id2 := uuid.NewV4()

  kb1, kl1, err := indexKeyToPtr(&id1, key1)
  Expect(err).Should(Succeed())
  defer freePtr(kb1)
  kb2, kl2, err := indexKeyToPtr(&id2, key2)
  Expect(err).Should(Succeed())
  defer freePtr(kb2)

  cmp := compareKeys(kb1, kl1, kb2, kl2)

  // UUIDs are never equal, so this should always work
  bcmp := bytes.Compare(id1.Bytes(), id2.Bytes())
  Expect(bcmp).ShouldNot(BeEquivalentTo(0))
  fmt.Fprintf(GinkgoWriter, "bcmp = %d cmp = %d\n", bcmp, cmp)
  if bcmp < 0 {
    Expect(cmp).Should(BeNumerically("<", 0))
  } else if bcmp > 0 {
    Expect(cmp).Should(BeNumerically(">", 0))
  }

  kb1a, kl1a, err := indexKeyToPtr(&id1, key2)
  Expect(err).Should(Succeed())
  defer freePtr(kb1a)

  cmp = compareKeys(kb1, kl1, kb1a, kl1a)

  fmt.Fprintf(GinkgoWriter, "key1 = %s\n", key1)
  fmt.Fprintf(GinkgoWriter, "key2 = %s\n", key2)
  if key1 < key2 {
    fmt.Fprintf(GinkgoWriter, "key1 < key2 cmp = %d\n", cmp)
    Expect(cmp).Should(BeNumerically("<", 0))
  } else if (key1 > key2) {
    fmt.Fprintf(GinkgoWriter, "key1 > key2 cmp = %d\n", cmp)
    Expect(cmp).Should(BeNumerically(">", 0))
  } else {
    fmt.Fprintf(GinkgoWriter, "key1 == key2 cmp = %d\n", cmp)
    Expect(cmp).Should(BeEquivalentTo(0))
  }

  cmp = compareKeys(kb1, kl1, kb1, kl1)
  Expect(cmp).Should(BeEquivalentTo(0))

  return true
}

func testTenantIndexCompare(ix1, ix2 uint64, sameId bool) bool {
  id1 := uuid.NewV4()
  var id2 uuid.UUID
  if sameId {
    id2 = id1
  } else {
    id2 = uuid.NewV4()
  }

  ptr1, len1 := tenantIndexToPtr(&id1, ix1)
  defer freePtr(ptr1)
  ptr2, len2 := tenantIndexToPtr(&id2, ix2)
  defer freePtr(ptr2)

  cmp := compareKeys(ptr1, len1, ptr2, len2)

  if !sameId {
    Expect(cmp).ShouldNot(BeEquivalentTo(0))
  } else if ix1 < ix2 {
    Expect(cmp).Should(BeNumerically("<", 0))
  } else if ix1 > ix2 {
    Expect(cmp).Should(BeNumerically(">", 0))
  } else {
    Expect(cmp).Should(BeEquivalentTo(0))
  }
  return true
}

/*
func tenantRangeTest(t *testing.T, tenant string, collection string, key string) bool {
  if tenant == "" || collection == "" {
    // Skip for testing
    return true
  }

  // Create an entry
  e := &Entry{
    Tenant: tenant,
    Collection: collection,
    Key: key,
  }
  keyBytes, keyLen, err := indexKeyToPtr(e)
  if err != nil {
    t.Logf("Ignoring error: %s", err)
    return true
  }
  defer freePtr(keyBytes)

  // Ensure that "start of tenant" and "end of tenant" records compare properly
  startBytes, startLen, err := startTenantToPtr(tenant)
  if err != nil { t.Fatal(err.Error()) }
  defer freePtr(startBytes)
  endBytes, endLen, err := endTenantToPtr(tenant)
  if err != nil { t.Fatal(err.Error()) }
  defer freePtr(endBytes)

  cmp := compareKeys(startBytes, startLen, keyBytes, keyLen)
  if (cmp >= 0) {
    t.Log("Start range should always be before all keys")
    return false
  }
  cmp = compareKeys(keyBytes, keyLen, endBytes, endLen)
  if (cmp >= 0) {
    t.Log("End range should always be after all keys")
    return false
  }

  return true
}

func collectionRangeTest(t *testing.T, tenant string, collection string, key string) bool {
  if tenant == "" || collection == "" {
    // Skip for testing
    return true
  }

  e := &Entry{
    Tenant: tenant,
    Collection: collection,
    Key: key,
  }
  keyBytes, keyLen, err := indexKeyToPtr(e)
  if err != nil {
    t.Logf("Ignoring error: %s", err)
    return true
  }
  defer freePtr(keyBytes)

  // Ensure that "start of tenant" and "end of tenant" records compare properly
  startBytes, startLen, err := startTenantToPtr(tenant)
  if err != nil { t.Fatal(err.Error()) }
  defer freePtr(startBytes)
  endBytes, endLen, err := endTenantToPtr(tenant)
  if err != nil { t.Fatal(err.Error()) }
  defer freePtr(endBytes)

  cmp := compareKeys(startBytes, startLen, keyBytes, keyLen)
  if cmp >= 0 {
    t.Log("Start tenant range should always be before all keys")
    return false
  }
  cmp = compareKeys(keyBytes, keyLen, endBytes, endLen)
  if cmp >= 0 {
    t.Log("End tenant range should always be after all keys")
    return false
  }
  cmp = compareKeys(startBytes, startLen, endBytes, endLen)
  if cmp >= 0 {
    t.Log("Start tenant must always be before end")
    return false
  }
  cmp = compareKeys(endBytes, endLen, startBytes, startLen)
  if cmp <= 0 {
    t.Log("End tenant must always be after start")
    return false
  }

  // Ensure that "start of collection" and "end of collection" records compare properly
  startColl, collLen, err := startCollectionToPtr(tenant, collection)
  if err != nil { t.Fatal(err.Error()) }
  defer freePtr(startColl)
  endColl, endCollLen, err := endCollectionToPtr(tenant, collection)
  if err != nil { t.Fatal(err.Error()) }
  defer freePtr(endColl)

  cmp = compareKeys(startColl, collLen, keyBytes, keyLen)
  if cmp >= 0 {
    t.Log("Start collection range should always be before all keys")
    return false
  }
  cmp = compareKeys(keyBytes, keyLen, endColl, endCollLen)
  if cmp >= 0 {
    t.Log("End collection range should always be after all keys")
    return false
  }
  cmp = compareKeys(startColl, collLen, startColl, collLen)
  if cmp != 0 {
    t.Log("Start collection keys must compare equal")
    return false
  }

  cmp = compareKeys(startBytes, startLen, startColl, collLen)
  if cmp >= 0 {
    t.Log("Start tenant should always be before start collection")
    return false
  }
  cmp = compareKeys(endColl, endCollLen, endBytes, endLen)
  if cmp >= 0 {
    t.Log("End tenant should always be after end collection")
    return false
  }
  cmp = compareKeys(endColl, endCollLen, endColl, endCollLen)
  if cmp != 0 {
    t.Log("End tenant keys should compare equal")
    return false
  }

  return true
}
*/
