package storage

import (
  "bytes"
  "testing"
  "testing/quick"
)

func TestConvertStringKey(t *testing.T) {
  if !testKey(1, "foobar") { t.Fatal("Basic test failed") }
  err := quick.Check(testKey, nil)
  if err != nil { t.Fatal(err.Error()) }
}

func testKey(kt int, key string) bool {
  if kt < 0 || kt > (1 << 8) {
    return true
  }
  keyBytes, keyLen := stringToKey(kt, key)
  defer freePtr(keyBytes)

  newType, newKey, err := keyToString(keyBytes, keyLen)
  if err != nil { return false}
  if newType != kt { return false }
  if newKey != key { return false }
  return true
}

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
  if !testEntry(123, []byte("Hello!")) { t.Fatal("Basic test fails") }
  err := quick.Check(testEntry, nil)
  if err != nil { t.Fatal(err.Error()) }
}

func testEntry(term uint64, data []byte) bool {
  bb, len := entryToPtr(term, data)
  defer freePtr(bb)

  resultTerm, resultData, err := ptrToEntry(bb, len)
  if err != nil { return false }
  if resultTerm != term { return false }
  if !bytes.Equal(resultData, data) { return false }
  return true
}
