package storage

import (
  "flag"
  "testing"
  "os"
  "testing/quick"
)

func TestLevelDBIndex(t *testing.T) {
  flag.Set("logtostderr", "true")
  stor, err := CreateLevelDBStorage("./indextestleveldb")
  if err != nil { t.Fatalf("Create db failed: %v", err) }
  defer func() {
    stor.Dump(os.Stdout, 25)
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
  }, nil)
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

  if !testOneCollection(t, stor, tenantName, "foocollection") {
    t.Log("Fatal errors testing collection")
    return false
  }

  err = quick.Check(func(coll string) bool {
    return testOneCollection(t, stor, tenantName, coll)
  }, nil)
  if err != nil {
    t.Logf("Error testing collections: %s", err)
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

  if !testOneKey(t, stor, tenantName, collectionName, "fookey") {
    t.Log("Fatal errors testing a key")
    return false
  }

  err = quick.Check(func(key string) bool {
    return testOneKey(t, stor, tenantName, collectionName, key)
  }, nil)
  if err != nil {
    t.Logf("Error testing keys: %s", err)
    return false
  }

  return true
}

func testOneKey(t *testing.T, stor Storage, tenantName, collectionName, key string) bool {
  if key == "" { return true }

  index, err := stor.GetIndexEntry(tenantName, collectionName, key)
  if err != nil { t.Fatal(err.Error()) }
  if index != 0 {
    t.Logf("Key %s exists and should not", key)
    return false
  }

  err = stor.SetIndexEntry(tenantName, collectionName, key, 1111)
  if err != nil { t.Fatal(err.Error()) }

  index, err = stor.GetIndexEntry(tenantName, collectionName, key)
  if err != nil { t.Fatal(err.Error()) }
  if index != 1111 {
    t.Logf("Key %s does not exist and shoul not", key)
    return false
  }

  return true
}