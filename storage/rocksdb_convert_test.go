package storage

import (
  "bytes"
  "time"
  "testing"
  "testing/quick"
)

func TestConvertIntKey(t *testing.T) {
  if !testIntKey(1, 1234) { t.Fatal("Basic test failed") }
  err := quick.Check(testIntKey, nil)
  if err != nil { t.Fatal(err.Error()) }
}

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

func TestConvertInt(t *testing.T) {
  if !testInt(123) { t.Fatal("Basic test failed") }
  err := quick.Check(testInt, nil)
  if err != nil { t.Fatal(err.Error()) }
}

func testInt(val uint64) bool {
  bytes, len := uintToPtr(val)
  defer freePtr(bytes)

  result := ptrToUint(bytes, len)
  if result != val { return false }
  return true
}

func TestConvertEntry(t *testing.T) {
  if !testEntry(t, 123, time.Now().UnixNano(), "", "", "", []byte("Hello!")) { t.Fatal("Failed on empty entry") }
  if !testEntry(t, 123, time.Now().UnixNano(), "foo", "bar", "baz", []byte("Hello!")) { t.Fatal("Failed on simple entry") }
  err := quick.Check(func(term uint64, ts int64, tenant string, collection string, key string, data []byte) bool {
    return testEntry(t, term, ts, tenant, collection, key, data)
  }, nil)
  if err != nil { t.Fatal(err.Error()) }
}

func testEntry(t *testing.T, term uint64, ts int64, tenant string, collection string, key string, data []byte) bool {
  tst := time.Unix(0, ts)
  e := &Entry{
    Term: term,
    Tenant: tenant,
    Timestamp: tst,
    Collection: collection,
    Key: key,
    Data: data,
  }
  bb, len := entryToPtr(e)
  defer freePtr(bb)

  re, err := ptrToEntry(bb, len)
  if err != nil { t.Error(err); return false }
  if e.Term != re.Term { t.Error("Term does not match"); return false }
  if !e.Timestamp.Equal(re.Timestamp) { t.Error("Timestamp does not match"); return false }
  if e.Tenant != re.Tenant { t.Errorf("Tenant %s does not match %s", re.Tenant, e.Tenant); return false }
  if e.Collection != re.Collection { t.Error("Collection does not match"); return false }
  if e.Key != re.Key { t.Error("Key does not match"); return false }
  if !bytes.Equal(e.Data, re.Data) { t.Error("Data does not match"); return false }
  return true
}

func TestConvertIndexEntry(t *testing.T) {
  if !testIndexEntry(t, "", "", "") { t.Fatal("Empty entry fails") }
  if !testIndexEntry(t, "foo", "bar", "baz") { t.Fatal("Simple entry fails") }
  err := quick.Check(func(tenant string, collection string, key string) bool {
    return testIndexEntry(t, tenant, collection, key)
  }, nil)
  if err != nil { t.Fatal(err.Error()) }
}

func testIndexEntry(t *testing.T, tenant string, collection string, key string) bool {
  e := &Entry{
    Tenant: tenant,
    Collection: collection,
    Key: key,
  }
  bb, len, err := indexKeyToPtr(e)
  if err != nil { t.Errorf("Error on conversion: %s", err); return false }
  defer freePtr(bb)

  re, err := ptrToIndexKey(bb, len)
  if err != nil { t.Error(err); return false }
  if e.Tenant != re.Tenant { t.Errorf("Tenant %s does not match %s", re.Tenant, e.Tenant); return false }
  if e.Collection != re.Collection { t.Error("Collection does not match"); return false }
  if e.Key != re.Key { t.Error("Key does not match"); return false }

  ten, col, ixLen, err := ptrToIndexType(bb, len)
  if err != nil { t.Error(err); return false }
  if ten != e.Tenant { t.Errorf("Tenant %s does not match %s", ten, e.Tenant); return false }
  if col != re.Collection { t.Error("Collection does not match"); return false }
  if ixLen >= maxKeyLen { t.Error("Key length is not correct") }
  return true
}

func TestConvertStartEndIndexEntry(t *testing.T) {

  startTen, startTenLen, err := startTenantToPtr("foo")
  if err != nil { t.Fatal(err.Error()) }
  defer freePtr(startTen)

  e, err := ptrToIndexKey(startTen, startTenLen)
  if err != nil { t.Fatal(err.Error()) }
  if e.Tenant != "foo" { t.Fatal("Tenant does not match") }

  ten, coll, len, err := ptrToIndexType(startTen, startTenLen)
  if err != nil { t.Fatal(err.Error()) }
  if ten != "foo" { t.Fatal("Wrong tenant name") }
  if coll != "" { t.Fatal("Had a collection name and should not") }
  if len != startRange { t.Fatal("Should be start range") }


  endTen, endTenLen, err := endTenantToPtr("foo")
  if err != nil { t.Fatal(err.Error()) }
  defer freePtr(endTen)

  e, err = ptrToIndexKey(endTen, endTenLen)
  if err != nil { t.Fatal(err.Error()) }
  if e.Tenant != "foo" { t.Fatal("Tenant does not match") }

  ten, coll, len, err = ptrToIndexType(endTen, endTenLen)
  if err != nil { t.Fatal(err.Error()) }
  if ten != "foo" { t.Fatal("Wrong tenant name") }
  if coll != "" { t.Fatal("Had a collection name and should not") }
  if len != endRange { t.Fatal("Should be end range") }


  startColl, startCollLen, err := startCollectionToPtr("foo", "bar")
  if err != nil { t.Fatal(err.Error()) }
  defer freePtr(startColl)

  e, err = ptrToIndexKey(startColl, startCollLen)
  if err != nil { t.Fatal(err.Error()) }
  if e.Tenant != "foo" { t.Fatal("Tenant does not match") }
  if e.Collection != "bar" { t.Fatal("Collection does not match") }

  ten, coll, len, err = ptrToIndexType(startColl, startCollLen)
  if err != nil { t.Fatal(err.Error()) }
  if ten != "foo" { t.Fatal("Wrong tenant name") }
  if coll != "bar" { t.Fatal("Wrong collection name") }
  if len != startRange { t.Fatal("Should be start range") }


  endColl, endCollLen, err := endCollectionToPtr("foo", "bar")
  if err != nil { t.Fatal(err.Error()) }
  defer freePtr(endColl)

  e, err = ptrToIndexKey(endColl, endCollLen)
  if err != nil { t.Fatal(err.Error()) }
  if e.Tenant != "foo" { t.Fatal("Tenant does not match") }
  if e.Collection != "bar" { t.Fatal("Collection does not match") }

  ten, coll, len, err = ptrToIndexType(endColl, endCollLen)
  if err != nil { t.Fatal(err.Error()) }
  if ten != "foo" { t.Fatal("Wrong tenant name") }
  if coll != "bar" { t.Fatal("Wrong collection name") }
  if len != endRange { t.Fatal("Should be end range") }
}

func TestConvertKeyComp(t *testing.T) {
  if !testKeyCompare(t, true, 1, true, 1) { t.Fatal("Equals comparison failed") }
  if !testKeyCompare(t, true, 1, false, 1) { t.Fatal("Type less comparison failed") }
  if !testKeyCompare(t, false, 1, true, 1) { t.Fatal("Type more comparison failed") }
  if !testKeyCompare(t, false, 1, false, 2) { t.Fatal("val less comparison failed") }
  if !testKeyCompare(t, false, 2, false, 1) { t.Fatal("val more comparison failed") }

  err := quick.Check(func(isMetadata1 bool, k1 uint64, isMetadata2 bool, k2 uint64) bool {
     return testKeyCompare(t, isMetadata1, k1, isMetadata2, k2)
  }, nil)
  if err != nil { t.Fatal(err.Error()) }
}

func testKeyCompare(t *testing.T, isMetadata1 bool, k1 uint64, isMetadata2 bool, k2 uint64) bool {
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
    t.Logf("kt1 < kt2: cmp %d", cmp)
    return cmp < 0
  }
  if kt1 > kt2 {
    t.Logf("kt1 > kt2: cmp %d", cmp)
    return cmp > 0
  }
  if k1 < k2 {
    t.Logf("k1 < k2: cmp %d", cmp)
    return cmp < 0
  }
  if k1 > k2 {
    t.Logf("k1 > k2: cmp %d", cmp)
    return cmp > 0
  }
  t.Logf("cmp %d", cmp)
  return cmp == 0
}

func TestIndexComp(t *testing.T) {
  if !testIndexCompare(t, "foo", "bar", "baz", "foo", "bar", "baz") { t.Fatal("Equals failed") }
  if !testIndexCompare(t, "aaa", "bar", "baz", "foo", "bar", "baz") { t.Fatal("Tenant < failed") }
  if !testIndexCompare(t, "aaa", "bar", "baz", "aaaa", "bar", "baz") { t.Fatal("Tenant < failed") }
  if !testIndexCompare(t, "foo", "bar", "baz", "aaa", "bar", "baz") { t.Fatal("Tenant > failed") }
  if !testIndexCompare(t, "foo", "aaaa", "baz", "foo", "bar", "baz") { t.Fatal("Collection < failed") }
  if !testIndexCompare(t, "foo", "bar", "baz", "foo", "a", "baz") { t.Fatal("Collection > failed") }
  if !testIndexCompare(t, "foo", "bar", "aaaaaa", "foo", "bar", "baz") { t.Fatal("Key < failed") }
  if !testIndexCompare(t, "foo", "bar", "baz", "foo", "bar", "a") { t.Fatal("Key > failed") }
  if !testIndexCompare(t, "", "", "aaaaaa", "", "", "baz") { t.Fatal("Key only < failed") }
  if !testIndexCompare(t, "", "", "baz", "", "", "a") { t.Fatal("Key only > failed") }

  err := quick.Check(func(tenant1 string, collection1 string, key1 string,
                          tenant2 string, collection2 string, key2 string) bool {
     return testIndexCompare(t, tenant1, collection1, key1, tenant2, collection2, key2)
  }, nil)
  if err != nil { t.Fatal(err.Error()) }

  err = quick.Check(func(collection1 string, key1 string,
                          collection2 string, key2 string) bool {
     return testIndexCompare(t, "", collection1, key1, "", collection2, key2)
  }, nil)
  if err != nil { t.Fatal(err.Error()) }

  err = quick.Check(func(key1 string, key2 string) bool {
     return testIndexCompare(t, "", "", key1, "", "", key2)
  }, nil)
  if err != nil { t.Fatal(err.Error()) }
}

func testIndexCompare(t *testing.T, tenant1 string, collection1 string, key1 string,
                      tenant2 string, collection2 string, key2 string) bool {
  e1 := &Entry{
    Tenant: tenant1,
    Collection: collection1,
    Key: key1,
  }
  keyBytes1, keyLen1, err := indexKeyToPtr(e1)
  if err != nil {
    t.Logf("Ignoring error: %s", err)
    return true
  }
  defer freePtr(keyBytes1)

  e2 := &Entry{
    Tenant: tenant2,
    Collection: collection2,
    Key: key2,
  }
  keyBytes2, keyLen2, err := indexKeyToPtr(e2)
  if err != nil {
    t.Logf("Ignoring error: %s", err)
    return true
  }
  defer freePtr(keyBytes2)

  cmp := compareKeys(keyBytes1, keyLen1, keyBytes2, keyLen2)

  if tenant1 < tenant2 {
    t.Logf("tenant1 < tenant2: %d", cmp)
    return (cmp < 0)
  }
  if tenant1 > tenant2 {
    t.Logf("tenant1 > tenant2: %d", cmp)
    return (cmp > 0)
  }

  if collection1 < collection2 {
    t.Logf("collection1 < collection1: %d", cmp)
    return (cmp < 0)
  }
  if collection1 > collection2 {
    t.Logf("collection1 > collection1: %d", cmp)
    return (cmp > 0)
  }

  if key1 < key2 {
    t.Logf("key1 < key2: %d", cmp)
    return (cmp < 0)
  }
  if key1 > key2 {
    t.Logf("key1 > key2: %d", cmp)
    return (cmp > 0)
  }

  t.Logf("Equal. %d", cmp)
  return (cmp == 0)
}

func TestTenantRange(t *testing.T) {
  if !tenantRangeTest(t, "foo", "bar", "baz") { t.Fatal("Range test failed") }

  err := quick.Check(func(tenant string, collection string, key string) bool {
     return tenantRangeTest(t, tenant, collection, key)
  }, nil)
  if err != nil { t.Fatal(err.Error()) }
}

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

func TestCollectionRange(t *testing.T) {
  if !collectionRangeTest(t, "foo", "bar", "baz") { t.Fatal("End range test failed") }
  if !collectionRangeTest(t, "foo", "bar", "baz") { t.Fatal("Start range test failed") }

  err := quick.Check(func(tenant string, collection string, key string) bool {
     return collectionRangeTest(t, tenant, collection, key)
  }, nil)
  if err != nil { t.Fatal(err.Error()) }
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
