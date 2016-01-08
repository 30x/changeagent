package storage

import (
  "bytes"
  "testing"
)

func TestLevelDBMetadata(t *testing.T) {
  stor, err := CreateLevelDBStorage("./metadatatestleveldb")
  if err != nil { t.Fatalf("Create db failed: %v", err) }
  defer func() {
    err := stor.Delete()
    if err != nil { t.Logf("Error deleting database: %v", err) }
  }()
  defer stor.Close()
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
  stor, err := CreateLevelDBStorage("./entrytestleveldb")
  if err != nil { t.Fatalf("Create db failed: %v", err) }
  defer func() {
    err := stor.Delete()
    if err != nil { t.Logf("Error deleting database: %v", err) }
  }()
  defer stor.Close()
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

   term, data, err := stor.GetEntry(1)
   if err != nil { t.Fatalf("error on get: %v", err) }
   if term != 0 { t.Fatalf("Expected term 0 and got %d", term) }
   if data != nil { t.Fatal("expected nil data") }

   hello := []byte("Hello!")

   err = stor.AppendEntry(1, 1, nil)
   if err != nil { t.Fatalf("error on append: %v", err) }
   err = stor.AppendEntry(2, 1, hello)
   if err != nil { t.Fatalf("error on append: %v", err) }

   term, data, err = stor.GetEntry(1)
   if err != nil { t.Fatalf("error on get: %v", err) }
   if term != 1 { t.Fatalf("Expected term 1 and got %d", term) }
   if data != nil { t.Fatalf("Expected nil data and got %v", data) }

   term, data, err = stor.GetEntry(2)
   if err != nil { t.Fatalf("error on get: %v", err) }
   if term != 1 { t.Fatalf("Expected term 1 and got %d", term) }
   if bytes.Compare(hello, data) != 0 { t.Fatal("Expected bytes to match") }

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
   if entries[0].Index != 1 { t.Fatalf("Expected index 1 and got %d", term) }
   if entries[0].Term != 1 { t.Fatalf("Expected term 1 and got %d", term) }
   if entries[0].Data != nil { t.Fatalf("Expected nil data") }
   if entries[1].Index != 2 { t.Fatalf("Expected index 2 and got %d", term) }
   if entries[1].Term != 1 { t.Fatalf("Expected term 1 and got %d", term) }
   if bytes.Compare(hello, entries[1].Data) != 0 { t.Fatal("Expected bytes to match") }

   err = stor.DeleteEntries(1)
   if err != nil { t.Fatalf("error on delete: %v", err) }

   ets, err = stor.GetEntryTerms(1)
   if err != nil { t.Fatalf("error on get terms: %v", err) }
   if len(ets) > 0 { t.Fatalf("Expected nothing") }

   err = stor.DeleteEntries(1)
   if err != nil { t.Fatalf("error on delete: %v", err) }
}
