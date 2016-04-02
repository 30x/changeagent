package storage

import (
  "fmt"
  "sort"
  "math"
  "testing/quick"
  "github.com/satori/go.uuid"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

var _ = Describe("Index test", func() {
  It("Tenant creation", func() {
    id := uuid.NewV4()
    err := indexTestDb.CreateTenant("singleTest", id)
    Expect(err).Should(Succeed())
    Expect(id).ShouldNot(BeNil())

    idStr, err := indexTestDb.GetTenantByName("singleTest")
    Expect(err).Should(Succeed())
    Expect(idStr).Should(Equal(id))

    name, err := indexTestDb.GetTenantByID(id)
    Expect(err).Should(Succeed())
    Expect(name).Should(Equal("singleTest"))

    ixes, err := indexTestDb.GetTenants()
    Expect(err).Should(Succeed())
    Expect(len(ixes)).Should(BeNumerically(">", 0))

    c := Collection{
      Id: id,
      Name: "singleTest",
    }
    Expect(ixes).Should(ContainElement(c))
  })

  It("Collection creation", func() {
    tenant := uuid.NewV4()
    err := indexTestDb.CreateTenant("collectionTest", tenant)
    Expect(err).Should(Succeed())
    Expect(tenant).ShouldNot(BeNil())

    id := uuid.NewV4()
    err = indexTestDb.CreateCollection(tenant, "singleCollection", id)
    Expect(err).Should(Succeed())
    Expect(id).ShouldNot(BeNil())

    idStr, err := indexTestDb.GetCollectionByName(tenant, "singleCollection")
    Expect(err).Should(Succeed())
    Expect(idStr).Should(Equal(id))

    name, tenantId, err := indexTestDb.GetCollectionByID(id)
    Expect(err).Should(Succeed())
    Expect(name).Should(Equal("singleCollection"))
    Expect(uuid.Equal(tenantId, tenant)).Should(BeTrue())

    ixes, err := indexTestDb.GetTenantCollections(tenant)
    Expect(err).Should(Succeed())
    Expect(len(ixes)).Should(BeNumerically(">", 0))

    c := Collection{
      Id: id,
      Name: "singleCollection",
    }
    Expect(ixes).Should(ContainElement(c))
  })

  It("Collection insert, delete, iterate", func() {
    tenant := uuid.NewV4()
    err := indexTestDb.CreateTenant("collectionInsert", tenant)
    Expect(err).Should(Succeed())
    Expect(tenant).ShouldNot(BeNil())

    id := uuid.NewV4()
    err = indexTestDb.CreateCollection(tenant, "insertCollection", id)
    Expect(err).Should(Succeed())
    Expect(id).ShouldNot(BeNil())

    err = indexTestDb.SetIndexEntry(id, "one", 1)
    Expect(err).Should(Succeed())
    ix, err := indexTestDb.GetIndexEntry(id, "one")
    Expect(err).Should(Succeed())
    Expect(ix).Should(BeEquivalentTo(1))
    err = indexTestDb.DeleteIndexEntry(id, "one")
    Expect(err).Should(Succeed())
    ix, err = indexTestDb.GetIndexEntry(id, "one")
    Expect(err).Should(Succeed())
    Expect(ix).Should(BeZero())

    err = indexTestDb.SetIndexEntry(id, "two", 2)
    Expect(err).Should(Succeed())

    ixes, err := indexTestDb.GetCollectionIndices(id, "", 100)
    Expect(err).Should(Succeed())
    expected := []uint64{2}
    Expect(expected).Should(Equal(ixes))

    err = indexTestDb.SetIndexEntry(id, "three", 3)
    Expect(err).Should(Succeed())
    err = indexTestDb.SetIndexEntry(id, "four", 4)
    Expect(err).Should(Succeed())

    ixes, err = indexTestDb.GetCollectionIndices(id, "", 100)
    Expect(err).Should(Succeed())
    expected = []uint64{4, 3, 2}
    Expect(ixes).Should(Equal(expected))

    ixes, err = indexTestDb.GetCollectionIndices(id, "", 1)
    Expect(err).Should(Succeed())
    expected = []uint64{4}
    Expect(ixes).Should(Equal(expected))

    ixes, err = indexTestDb.GetCollectionIndices(id, "", 2)
    Expect(err).Should(Succeed())
    expected = []uint64{4, 3}
    Expect(ixes).Should(Equal(expected))

    ixes, err = indexTestDb.GetCollectionIndices(id, "three", 1)
    Expect(err).Should(Succeed())
    expected = []uint64{2}
    Expect(ixes).Should(Equal(expected))

    ixes, err = indexTestDb.GetCollectionIndices(id, "two", 100)
    Expect(err).Should(Succeed())
    expected = []uint64{}
    Expect(ixes).Should(BeEmpty())
  })

  It("Single collection stress", func() {
    s := testOneTenant(indexTestDb, "foo")
    Expect(s).Should(BeTrue())
  })

  It("Collection stress", func() {
    err := quick.Check(func(tenant string) bool {
      return testOneTenant(indexTestDb, tenant)
    }, &quick.Config{
      MaxCount: 10,
    })
    Expect(err).Should(Succeed())
  })
})

func testOneTenant(stor Storage, tenantName string) bool {
  if tenantName == "" { return true }
  tenantID := uuid.NewV4()

  err := stor.CreateTenant(tenantName, tenantID)
  Expect(err).Should(Succeed())

  var collections []string

  err = quick.Check(func(coll string) bool {
    if coll != "" {
      collections = append(collections, coll)
    }
    return testOneCollection(stor, tenantID, coll)
  }, nil)
  Expect(err).Should(Succeed())

  var foundCollections []string
  tenantCollections, err := stor.GetTenantCollections(tenantID)
  Expect(err).Should(Succeed())

  for _, c := range(tenantCollections) {
    foundCollections = append(foundCollections, c.Name)
  }

  sort.Strings(collections)
  Expect(foundCollections).Should(Equal(collections))

  return true
}

func testOneCollection(stor Storage, tenantID uuid.UUID, collectionName string) bool {
  if collectionName == "" { return true }
  collectionID := uuid.NewV4()

  err := stor.CreateCollection(tenantID, collectionName, collectionID)
  Expect(err).Should(Succeed())

  expected := make(map[string]uint64)

  err = quick.Check(func(key string, val uint64, shouldDelete bool) bool {
    if !shouldDelete && key != "" {
      if expected[key] != 0 {
        fmt.Fprintf(GinkgoWriter, "Duplicate key %s\n", key)
      }
      expected[key] = val
    }
    return testOneKey(stor, collectionID, key, val, shouldDelete)
  }, nil)
  Expect(err).Should(Succeed())

  indexVals, err := stor.GetCollectionIndices(collectionID, "", math.MaxUint32)
  Expect(err).Should(Succeed())
  fmt.Fprintf(GinkgoWriter, "Inserted %d to %s\n", len(expected), collectionName)
  fmt.Fprintf(GinkgoWriter, "Collection %s has %d values\n", collectionName, len(indexVals))

  var expectedVals []uint64
  for _, v := range(expected) {
    expectedVals = append(expectedVals, v)
  }
  expectedVals = sortArray(expectedVals)
  indexVals = sortArray(indexVals)

  Expect(indexVals).Should(Equal(expectedVals))

  return true
}

func testOneKey(stor Storage, collectionID uuid.UUID, key string, val uint64, shouldDelete bool) bool {
  if key == "" || val == 0 { return true }

  index, err := stor.GetIndexEntry(collectionID, key)
  Expect(err).Should(Succeed())
  if index != 0 {
    fmt.Fprintf(GinkgoWriter, "Key %s exists and should not\n", key)
    return false
  }

  err = stor.SetIndexEntry(collectionID, key, val)
  Expect(err).Should(Succeed())

  index, err = stor.GetIndexEntry(collectionID, key)
  Expect(err).Should(Succeed())
  if index != val {
    fmt.Fprintf(GinkgoWriter, "Key %s does not exist and should\n", key)
    return false
  }

  if shouldDelete {
    err = stor.DeleteIndexEntry(collectionID, key)
    Expect(err).Should(Succeed())
    index, err := stor.GetIndexEntry(collectionID, key)
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
