package storage

import (
  "fmt"
  "math"
  "reflect"
  "sort"
  "testing/quick"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

var _ = Describe("Index test", func() {
  It("Test Index", func() {
    stor, err := CreateRocksDBStorage("./indextestleveldb", 1000)
    Expect(err).Should(Succeed())
    defer func() {
      //stor.Dump(os.Stdout, 25)
      stor.Close()
      err := stor.Delete()
      Expect(err).Should(Succeed())
    }()
    indexTest(stor)
  })
})

func indexTest(stor Storage) {
  if !testOneTenant(stor, "foo") {
    Fail("Fatal errors for first tenant")
  }

  err := quick.Check(func(tenant string) bool {
    return testOneTenant(stor, tenant)
  }, &quick.Config{
    MaxCount: 10,
  })
  Expect(err).Should(Succeed())
}

func testOneTenant(stor Storage, tenantName string) bool {
  if tenantName == "" { return true }

  err := stor.SetIndexEntry(tenantName, "coll", "foo", 1234);
  if err != nil {
    fmt.Fprintf(GinkgoWriter, "error: %v\n", err)
    return false
  }

  tenExists, err := stor.TenantExists(tenantName)
  Expect(err).Should(Succeed())
  if tenExists {
    fmt.Fprintf(GinkgoWriter, "Tenant exists and should not\n")
    return false
  }

  err = stor.CreateTenant(tenantName)
  Expect(err).Should(Succeed())

  tenExists, err = stor.TenantExists(tenantName)
  Expect(err).Should(Succeed())
  if !tenExists {
    fmt.Fprintf(GinkgoWriter, "Tenant does not exist and should\n")
    return false
  }

  var collections []string

  if !testOneCollection(stor, tenantName, "foocollection") {
    fmt.Fprintf(GinkgoWriter, "Fatal errors testing collection\n")
    return false
  }
  collections = append(collections, "foocollection")

  err = quick.Check(func(coll string) bool {
    if coll != "" {
      collections = append(collections, coll)
    }
    return testOneCollection(stor, tenantName, coll)
  }, nil)
  Expect(err).Should(Succeed())

  sort.Strings(collections)

  foundCollections, err := stor.GetTenantCollections(tenantName)

  if !reflect.DeepEqual(collections, foundCollections) {
    fmt.Fprintf(GinkgoWriter, "Collection list does not match for tenant %s\n", tenantName)
    for i := 0; i < len(collections) && i < len(foundCollections); i++ {
      if collections[i] != foundCollections[i] {
        fmt.Fprintf(GinkgoWriter, "Mismatch at position %d\n", i)
        fmt.Fprintf(GinkgoWriter, "Expected: %s\n", collections[i])
        fmt.Fprintf(GinkgoWriter, "Found:    %s\n", foundCollections[i])
      }
    }
    return false
  }

  return true
}

func testOneCollection(stor Storage, tenantName string, collectionName string) bool {
  if collectionName == "" { return true }

  collExists, err := stor.CollectionExists(tenantName, collectionName)
  Expect(err).Should(Succeed())
  if collExists {
    fmt.Fprintln(GinkgoWriter, "Collection exists and should not")
    return false
  }

  err = stor.CreateCollection(tenantName, collectionName)
  Expect(err).Should(Succeed())

  collExists, err = stor.CollectionExists(tenantName, collectionName)
  Expect(err).Should(Succeed())
  if !collExists {
    fmt.Fprintln(GinkgoWriter, "Collection does not exist and should")
    return false
  }

  expected := make(map[string]uint64)

  if !testOneKey(stor, tenantName, collectionName, "fookey", 1234, false) {
    fmt.Fprintln(GinkgoWriter, "Fatal errors testing a key")
    return false
  }
  expected["fookey"] = 1234
  if !testOneKey(stor, tenantName, collectionName, "barkey", 1234, true) {
    fmt.Fprintln(GinkgoWriter, "Fatal errors testing a key")
    return false
  }

  err = quick.Check(func(key string, val uint64, shouldDelete bool) bool {
    if !shouldDelete && key != "" {
      if expected[key] != 0 {
        fmt.Fprintf(GinkgoWriter, "Duplicate key %s\n", key)
      }
      expected[key] = val
    }
    return testOneKey(stor, tenantName, collectionName, key, val, shouldDelete)
  }, nil)
  Expect(err).Should(Succeed())

  indexVals, err := stor.GetCollectionIndices(tenantName, collectionName, math.MaxUint32)
  Expect(err).Should(Succeed())
  fmt.Fprintf(GinkgoWriter, "Inserted %d to %s\n", len(expected), collectionName)
  fmt.Fprintf(GinkgoWriter, "Collection %s has %d values\n", collectionName, len(indexVals))

  var expectedVals []uint64
  for _, v := range(expected) {
    expectedVals = append(expectedVals, v)
  }
  expectedVals = sortArray(expectedVals)
  indexVals = sortArray(indexVals)

  if !reflect.DeepEqual(expectedVals, indexVals) {
    fmt.Fprintln(GinkgoWriter, "Values do not match")
    for i := 0; i < len(expectedVals) && i < len(indexVals); i++ {
      if expectedVals[i] != indexVals[i] {
        fmt.Fprintf(GinkgoWriter, "Mismatch at position %d (%d != %d)\n", i, expectedVals[i], indexVals[i])
      }
    }
    return false
  }

  return true
}

func testOneKey(stor Storage, tenantName, collectionName, key string, val uint64, shouldDelete bool) bool {
  if key == "" || val == 0 { return true }

  index, err := stor.GetIndexEntry(tenantName, collectionName, key)
  Expect(err).Should(Succeed())
  if index != 0 {
    fmt.Fprintf(GinkgoWriter, "Key %s exists and should not\n", key)
    return false
  }

  err = stor.SetIndexEntry(tenantName, collectionName, key, val)
  Expect(err).Should(Succeed())

  index, err = stor.GetIndexEntry(tenantName, collectionName, key)
  Expect(err).Should(Succeed())
  if index != val {
    fmt.Fprintf(GinkgoWriter, "Key %s does not exist and should\n", key)
    return false
  }

  if shouldDelete {
    err = stor.DeleteIndexEntry(tenantName, collectionName, key)
    Expect(err).Should(Succeed())
    index, err := stor.GetIndexEntry(tenantName, collectionName, key)
    Expect(err).Should(Succeed())
    if index != 0 {
      fmt.Fprintf(GinkgoWriter, "Key %s exists after delete and should not\n", key)
      return false
    }
  }

  return true
}

func sortArray(a []uint64) []uint64 {
  o := createIndexArray(a)
  sort.Sort(o)
  return o.vals
}

type indexArray struct {
  vals []uint64
}

func createIndexArray(ix []uint64) *indexArray {
  return &indexArray{
    vals: ix,
  }
}

func (a *indexArray) Len() int {
  return len(a.vals)
}

func (a *indexArray) Less(i, j int) bool {
  return a.vals[i] < a.vals[j]
}

func (a *indexArray) Swap(i, j int) {
  tmp := a.vals[i]
  a.vals[i] = a.vals[j]
  a.vals[j] = tmp
}