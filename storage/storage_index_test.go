package storage

import (
  "flag"
  "math"
  "reflect"
  "sort"
  "testing"
  "testing/quick"
)

func TestLevelDBIndex(t *testing.T) {
  flag.Set("logtostderr", "true")
  stor, err := CreateRocksDBStorage("./indextestleveldb", 1000)
  if err != nil { t.Fatalf("Create db failed: %v", err) }
  defer func() {
    //stor.Dump(os.Stdout, 25)
    stor.Close()
    err := stor.Delete()
    if err != nil { t.Logf("Error deleting database: %v", err) }
  }()
  indexTest(t, stor)
}

func indexTest(t *testing.T, stor Storage) {
  if !testOneTenant(t, stor, "foo") {
    t.Fatal("Fatal errors for tenant")
  }

  err := quick.Check(func(tenant string) bool {
    return testOneTenant(t, stor, tenant)
  }, &quick.Config{
    MaxCount: 10,
  })
  if err != nil {
    t.Fatalf("Test failed: %v", err)
  }
}

func testOneTenant(t *testing.T, stor Storage, tenantName string) bool {
  if tenantName == "" { return true }

  err := stor.SetIndexEntry(tenantName, "coll", "foo", 1234);
  if err != nil {
    t.Logf("error: %v", err)
    return false
  }

  tenExists, err := stor.TenantExists(tenantName)
  if err != nil { t.Fatal(err.Error()) }
  if tenExists {
    t.Log("Tenant exists and should not")
    return false
  }

  err = stor.CreateTenant(tenantName)
  if err != nil { t.Fatal(err.Error()) }

  tenExists, err = stor.TenantExists(tenantName)
  if err != nil { t.Fatal(err.Error()) }
  if !tenExists {
    t.Log("Tenant does not exist and should")
    return false
  }

  var collections []string

  if !testOneCollection(t, stor, tenantName, "foocollection") {
    t.Log("Fatal errors testing collection")
    return false
  }
  collections = append(collections, "foocollection")

  err = quick.Check(func(coll string) bool {
    if coll != "" {
      collections = append(collections, coll)
    }
    return testOneCollection(t, stor, tenantName, coll)
  }, nil)
  if err != nil {
    t.Logf("Error testing collections: %s", err)
    return false
  }

  sort.Strings(collections)

  foundCollections, err := stor.GetTenantCollections(tenantName)

  if !reflect.DeepEqual(collections, foundCollections) {
    t.Logf("Collection list does not match for tenant %s", tenantName)
    for i := 0; i < len(collections) && i < len(foundCollections); i++ {
      if collections[i] != foundCollections[i] {
        t.Logf("Mismatch at position %d", i)
        t.Logf("Expected: %s", collections[i])
        t.Logf("Found:    %s", foundCollections[i])
      }
    }
    return false
  }

  return true
}

func testOneCollection(t *testing.T, stor Storage, tenantName string, collectionName string) bool {
  if collectionName == "" { return true }

  collExists, err := stor.CollectionExists(tenantName, collectionName)
  if err != nil { t.Fatal(err.Error()) }
  if collExists {
    t.Log("Collection exists and should not")
    return false
  }

  err = stor.CreateCollection(tenantName, collectionName)
  if err != nil { t.Fatal(err.Error()) }

  collExists, err = stor.CollectionExists(tenantName, collectionName)
  if err != nil { t.Fatal(err.Error()) }
  if !collExists {
    t.Log("Collection does not exist and should")
    return false
  }

  expected := make(map[string]uint64)

  if !testOneKey(t, stor, tenantName, collectionName, "fookey", 1234, false) {
    t.Log("Fatal errors testing a key")
    return false
  }
  expected["fookey"] = 1234
  if !testOneKey(t, stor, tenantName, collectionName, "barkey", 1234, true) {
    t.Log("Fatal errors testing a key")
    return false
  }

  err = quick.Check(func(key string, val uint64, shouldDelete bool) bool {
    if !shouldDelete && key != "" {
      if expected[key] != 0 {
        t.Logf("Duplicate key %s", key)
      }
      expected[key] = val
    }
    return testOneKey(t, stor, tenantName, collectionName, key, val, shouldDelete)
  }, nil)
  if err != nil {
    t.Logf("Error testing keys: %s", err)
    return false
  }

  indexVals, err := stor.GetCollectionIndices(tenantName, collectionName, math.MaxUint32)
  if err != nil {
    t.Logf("Error getting indices: %s", err)
    return false
  }
  t.Logf("Inserted %d to %s", len(expected), collectionName)
  t.Logf("Collection %s has %d values", collectionName, len(indexVals))

  var expectedVals []uint64
  for _, v := range(expected) {
    expectedVals = append(expectedVals, v)
  }
  expectedVals = sortArray(expectedVals)
  indexVals = sortArray(indexVals)

  if !reflect.DeepEqual(expectedVals, indexVals) {
    t.Log("Values do not match")
    for i := 0; i < len(expectedVals) && i < len(indexVals); i++ {
      if expectedVals[i] != indexVals[i] {
        t.Logf("Mismatch at position %d (%d != %d)", i, expectedVals[i], indexVals[i])
      }
    }
    return false
  }

  return true
}

func testOneKey(t *testing.T, stor Storage, tenantName, collectionName, key string, val uint64, shouldDelete bool) bool {
  if key == "" || val == 0 { return true }

  index, err := stor.GetIndexEntry(tenantName, collectionName, key)
  if err != nil { t.Fatal(err.Error()) }
  if index != 0 {
    t.Logf("Key %s exists and should not", key)
    return false
  }

  err = stor.SetIndexEntry(tenantName, collectionName, key, val)
  if err != nil { t.Fatal(err.Error()) }

  index, err = stor.GetIndexEntry(tenantName, collectionName, key)
  if err != nil { t.Fatal(err.Error()) }
  if index != val {
    t.Logf("Key %s does not exist and should not", key)
    return false
  }

  if shouldDelete {
    err = stor.DeleteIndexEntry(tenantName, collectionName, key)
    if err != nil { t.Fatal(err.Error()) }
    index, err := stor.GetIndexEntry(tenantName, collectionName, key)
    if err != nil { t.Fatal(err.Error()) }
    if index != 0 {
      t.Logf("Key %s exists after delete and should not", key)
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