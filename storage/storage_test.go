package storage

import (
  "bytes"
  "flag"
  "testing"
  "time"
  "os"
)

func TestLevelDBMetadata(t *testing.T) {
  flag.Set("logtostderr", "true")
  stor, err := CreateRocksDBStorage("./metadatatestleveldb", 1000)
  if err != nil { t.Fatalf("Create db failed: %v", err) }
  defer func() {
    stor.Close()
    err := stor.Delete()
    if err != nil { t.Logf("Error deleting database: %v", err) }
  }()
  metadataTest(t, stor)
}

func metadataTest(t* testing.T, stor Storage) {
   err := stor.SetMetadata(1, 123)
   if err != nil { t.Fatalf("Error on set: %v", err) }
   val, err := stor.GetMetadata(1)
   if err != nil { t.Fatalf("Error on get: %v", err) }
   if val != 123 { t.Fatalf("Received %d instead of %d", val, 123) }

   err = stor.SetMetadata(1, 234)
   if err != nil { t.Fatalf("Error on set: %v", err) }
   val, err = stor.GetMetadata(1)
   if err != nil { t.Fatalf("Error on get: %v", err) }
   if val != 234 { t.Fatalf("Received %d instead of %d", val, 234) }

   bval := []byte("Hello, Metadata World!")
   err = stor.SetRawMetadata(2, bval)
   if err != nil { t.Fatalf("Error on set: %v", err) }
   bresult, err := stor.GetRawMetadata(2)
   if err != nil { t.Fatalf("Error on get: %v", err) }
   if !bytes.Equal(bresult, bval) { t.Fatal("Bytes metadata does not match") }

   val, err = stor.GetMetadata(999)
   if err != nil { t.Fatalf("Error on get: %v", err) }
   if val != 0 { t.Fatalf("Received %d instead of %d", val, 0) }
}

func TestLevelDBEntries(t *testing.T) {
  flag.Set("logtostderr", "true")
  stor, err := CreateRocksDBStorage("./entrytestleveldb", 1000)
  if err != nil { t.Fatalf("Create db failed: %v", err) }
  defer func() {
    stor.Dump(os.Stdout, 25)
    stor.Close()
    err := stor.Delete()
    if err != nil { t.Logf("Error deleting database: %v", err) }
  }()
  entriesTest(t, stor)
}

func entriesTest(t *testing.T, stor Storage) {
  max, term, err := stor.GetLastIndex()
  if err != nil { t.Fatalf("error on get max: %v", err) }
  if max != 0 { t.Fatalf("Expected 0 max index and got %d", max) }
  if term != 0 { t.Fatalf("Expected 0 max term and got %d", term) }

  entries, err := stor.GetEntries(1, 3)
  if err != nil { t.Fatalf("error on getEntries: %v", err) }
  if len(entries) != 0 { t.Fatalf("Expected no entries and got %d", len(entries)) }

  entry, err := stor.GetEntry(1)
  if err != nil { t.Fatalf("error on get: %v", err) }
  if entry != nil { t.Fatal("expected nil entry") }

  hello := []byte("Hello!")

  // Put in some metadata to confuse the index a bit
  err = stor.SetMetadata(1, 1)
  if err != nil { t.Fatalf("error on set: %v", err) }

  max, term, err = stor.GetLastIndex()
  if err != nil { t.Fatalf("error on get max: %v", err) }
  if max != 0 { t.Fatalf("Expected 0 max index and got %d", max) }
  if term != 0 { t.Fatalf("Expected 0 max term and got %d", term) }

  entry1 := &Entry{
    Index: 1,
    Term: 1,
    Timestamp: time.Now(),
  }
  err = stor.AppendEntry(entry1)
  if err != nil { t.Fatalf("error on append: %v", err) }

  entry2 := &Entry{
    Index: 2,
    Term: 1,
    Timestamp: time.Now(),
    Data: hello,
  }
  err = stor.AppendEntry(entry2)
  if err != nil { t.Fatalf("error on append: %v", err) }

  re, err := stor.GetEntry(1)
  if err != nil { t.Fatalf("error on get: %v", err) }
  if !entriesMatch(t, entry1, re) { t.Fatal("No match") }

  re, err = stor.GetEntry(2)
  if err != nil { t.Fatalf("error on get: %v", err) }
  if !entriesMatch(t, entry2, re) { t.Fatal("No match") }

  max, term, err = stor.GetLastIndex()
  if err != nil { t.Fatalf("error on get max: %v", err) }
  if max != 2 { t.Fatalf("Expected 2 max index and got %d", max) }
  if term != 1 { t.Fatalf("Expected 1 max term and got %d", term) }

  ets, err := stor.GetEntryTerms(1)
  if err != nil { t.Fatalf("error on get terms: %v", err) }
  if ets[1] != 1 { t.Fatalf("Expected 1 and not %d at 1", ets[1]) }
  if ets[2] != 1 { t.Fatalf("Expected 1 and not %d at 2", ets[2]) }

  ets, err = stor.GetEntryTerms(3)
  if err != nil { t.Fatalf("error on get terms: %v", err) }
  if len(ets) > 0 { t.Fatalf("Expected nothing") }

  entries, err = stor.GetEntries(1, 2)
  if err != nil { t.Fatalf("error on getEntries: %v", err) }
  if len(entries) != 2 { t.Fatalf("Expected 2 entries and got %d", len(entries)) }
  if !entriesMatch(t, entry1, &entries[0]) { t.Fatal("No match") }
  if !entriesMatch(t, entry2, &entries[1]) { t.Fatal("No match") }

  err = stor.DeleteEntries(1)
  if err != nil { t.Fatalf("error on delete: %v", err) }

  ets, err = stor.GetEntryTerms(1)
  if err != nil { t.Fatalf("error on get terms: %v", err) }
  if len(ets) > 0 { t.Fatalf("Expected nothing") }

  err = stor.DeleteEntries(1)
  if err != nil { t.Fatalf("error on delete: %v", err) }
}

func entriesMatch(t *testing.T, e1 *Entry, e2 *Entry) bool {
  if e1.Index != e2.Index { t.Log("Index does not match"); return false }
  if e1.Term != e2.Term { t.Log("Term does not match"); return false  }
  if !e1.Timestamp.Equal(e2.Timestamp) { t.Logf("Timestamp %v does not match %v", e1.Timestamp, e2.Timestamp); return false  }
  if e1.Tenant != e2.Tenant { t.Log("Tenant does not match"); return false  }
  if e1.Collection != e2.Collection { t.Log("Collection does not match"); return false  }
  if e1.Key != e2.Key { t.Log("Key does not match"); return false  }
  if !bytes.Equal(e1.Data, e2.Data) { t.Log("Data does not match"); return false  }
  return true
}
