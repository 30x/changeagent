package storage

/*
#include <stdlib.h>
#include "rocksdb_native.h"
#cgo CFLAGS: -g -O0 -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lrocksdb
*/
import "C"

import (
  "fmt"
  "io"
  "math"
  "unsafe"
  "sync"
  "github.com/golang/glog"
  "github.com/satori/go.uuid"
)

const (
  TenantsCollectionId = "B7404AD3-1268-4823-B7D4-E9C66F592E37"
)

var defaultWriteOptions *C.rocksdb_writeoptions_t = C.rocksdb_writeoptions_create();
var defaultReadOptions *C.rocksdb_readoptions_t = C.rocksdb_readoptions_create();
var rocksInitOnce sync.Once
var tenantsCollection uuid.UUID = uuid.FromStringOrNil(TenantsCollectionId)

type LevelDBStorage struct {
  baseFile string
  dbHandle *C.GoRocksDb
  db *C.rocksdb_t
  metadata *C.rocksdb_column_family_handle_t
  indices *C.rocksdb_column_family_handle_t
  tenantIndices *C.rocksdb_column_family_handle_t
  entries *C.rocksdb_column_family_handle_t
}

func CreateRocksDBStorage(baseFile string, cacheSize uint) (*LevelDBStorage, error) {
  stor := &LevelDBStorage{
    baseFile: baseFile,
  }

  rocksInitOnce.Do(func() {
    C.go_rocksdb_init()
  })

  var dbh *C.GoRocksDb;
  e := C.go_rocksdb_open(
    C.CString(baseFile),
    C.size_t(cacheSize),
    &dbh);

  if e != nil {
    defer freeString(e)
    err := stringToError(e)
    glog.Errorf("Error opening RocksDB file %s: %v", baseFile, err)
    return nil, err
  }

  glog.Infof("Opened RocksDB file in %s", baseFile)

  stor.dbHandle = dbh;
  stor.db = dbh.db
  stor.metadata = dbh.metadata
  stor.entries = dbh.entries
  stor.tenantIndices = dbh.tenantIndices
  stor.indices = dbh.indices

  success := false
  defer func() {
    if !success {
      stor.Close()
    }
  }()

  // Lay down the records for a special collection that will hold names and IDs of each tenant
  collName, err := stor.readCollectionStart(&tenantsCollection)
  if err != nil { return nil, err }
  if collName == "" {
    glog.V(2).Infof("Creating tenants collection %s", TenantsCollectionId)
    err = stor.writeCollectionDelimiters(&tenantsCollection, "_tenants")
    if err != nil { return nil, err }
  }

  success = true
  return stor, nil
}

func (s *LevelDBStorage) GetDataPath() string {
  return s.baseFile
}

func (s *LevelDBStorage) Close() {
  C.go_rocksdb_close(s.dbHandle)
  freePtr(unsafe.Pointer(s.dbHandle))
}

func (s *LevelDBStorage) Delete() error {
  var e *C.char
  opts := C.rocksdb_options_create()
  defer C.rocksdb_options_destroy(opts)

  dbCName := C.CString(s.baseFile)
  defer freeString(dbCName)
  C.rocksdb_destroy_db(opts, dbCName, &e)
  if e == nil {
    glog.Infof("Destroyed LevelDB database in %s", s.baseFile)
    return nil
  }
  defer freeString(e)
  err := stringToError(e)
  if err != nil {
    glog.Infof("Error destroying LevelDB database: %s", err)
  }
  return err
}

func (s *LevelDBStorage) Dump(out io.Writer, max int) {
  mit := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.metadata)
  defer C.rocksdb_iter_destroy(mit)

  fmt.Fprintln(out, "* Metadata *")
  C.rocksdb_iter_seek_to_first(mit)

  for i := 0; i < max && C.rocksdb_iter_valid(mit) != 0; i++ {
    var keyLen C.size_t
    var valLen C.size_t

    keyPtr := unsafe.Pointer(C.rocksdb_iter_key(mit, &keyLen))
    C.rocksdb_iter_value(mit, &valLen)

    _, mkey, _ := keyToUint(keyPtr, keyLen)
    fmt.Fprintf(out, "Metadata   (%d) %d bytes\n", mkey, valLen)
    C.rocksdb_iter_next(mit)
  }

  iit := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.indices)
  defer C.rocksdb_iter_destroy(iit)

  fmt.Fprintln(out, "* Indices *")
  C.rocksdb_iter_seek_to_first(iit)

  for i := 0; i < max && C.rocksdb_iter_valid(iit) != 0; i++ {
    //var keyLen C.size_t
    var valLen C.size_t

    //keyPtr := unsafe.Pointer(C.rocksdb_iter_key(iit, &keyLen))
    C.rocksdb_iter_value(iit, &valLen)

    /*
    tenName, colName, ixLen, _ := ptrToIndexType(keyPtr, keyLen)

    if colName == "" {
      switch ixLen {
      case startRange:
        fmt.Fprintf(out, "Tenant (%s) start\n", tenName)
      case endRange:
        fmt.Fprintf(out, "Tenant (%s) end\n", tenName)
      default:
        fmt.Fprintf(out, "Tenant (%s) %d bytes\n", tenName, valLen)
      }
    } else {
      switch ixLen {
      case startRange:
        fmt.Fprintf(out, "Collection (%s) start\n", colName)
      case endRange:
        fmt.Fprintf(out, "Collection (%s) end\n", colName)
      default:
        fmt.Fprintf(out, "Collection (%s) %d bytes\n", colName, valLen)
      }
    }
    */

    C.rocksdb_iter_next(iit)
  }

  eit := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
  defer C.rocksdb_iter_destroy(eit)

  fmt.Fprintln(out, "* Entries *")
  C.rocksdb_iter_seek_to_first(eit)

  for i := 0; i < max && C.rocksdb_iter_valid(eit) != 0; i++ {
    var keyLen C.size_t
    var valLen C.size_t

    keyPtr := unsafe.Pointer(C.rocksdb_iter_key(eit, &keyLen))
    valPtr := C.rocksdb_iter_value(eit, &valLen)

    _, ekey, _ := keyToUint(keyPtr, keyLen)
    entry, _ := ptrToEntry(unsafe.Pointer(valPtr), valLen)
    fmt.Fprintf(out,          "Entry       (%d) %s\n", ekey, entry)

    C.rocksdb_iter_next(eit)
  }
}

func (s *LevelDBStorage) GetMetadata(key uint) (uint64, error) {
  keyBuf, keyLen := uintToKey(MetadataKey, uint64(key))
  defer freePtr(keyBuf)

  val, valLen, err := s.readEntry(s.metadata, keyBuf, keyLen)
  if err != nil { return 0, err }
  if val == nil { return 0, nil }

  defer freePtr(val)
  return ptrToUint(val, valLen), nil
}

func (s *LevelDBStorage) GetRawMetadata(key uint) ([]byte, error) {
  keyBuf, keyLen := uintToKey(MetadataKey, uint64(key))
  defer freePtr(keyBuf)

  val, valLen, err := s.readEntry(s.metadata, keyBuf, keyLen)
  if err != nil { return nil, err }
  if val == nil { return nil, nil }

  defer freePtr(val)
  return ptrToBytes(val, valLen), nil
}

func (s *LevelDBStorage) SetMetadata(key uint, val uint64) error {
  keyBuf, keyLen := uintToKey(MetadataKey, uint64(key))
  defer freePtr(keyBuf)
  valBuf, valLen := uintToPtr(val)
  defer freePtr(valBuf)

  return s.putEntry(s.metadata, keyBuf, keyLen, valBuf, valLen)
}

func (s *LevelDBStorage) SetRawMetadata(key uint, val []byte) error {
  keyBuf, keyLen := uintToKey(MetadataKey, uint64(key))
  defer freePtr(keyBuf)
  valBuf, valLen := bytesToPtr(val)
  defer freePtr(valBuf)

  return s.putEntry(s.metadata, keyBuf, keyLen, valBuf, valLen)
}

// Methods for the Raft index

func (s *LevelDBStorage) AppendEntry(entry *Entry) error {
  keyPtr, keyLen := uintToKey(EntryKey, entry.Index)
  defer freePtr(keyPtr)

  valPtr, valLen := entryToPtr(entry)
  defer freePtr(valPtr)

  glog.V(2).Infof("Appending entry: %s", entry)
  return s.putEntry(s.entries, keyPtr, keyLen, valPtr, valLen)
}

  // Get term and data for entry. Return term 0 if not found.
func (s *LevelDBStorage) GetEntry(index uint64) (*Entry, error) {
  keyPtr, keyLen := uintToKey(EntryKey, index)
  defer freePtr(keyPtr)

  valPtr, valLen, err := s.readEntry(s.entries, keyPtr, keyLen)
  if err != nil { return nil, err }
  if valPtr == nil { return nil, nil }

  defer freePtr(valPtr)
  return ptrToEntry(valPtr, valLen)
}

func (s *LevelDBStorage) GetEntries(since uint64, max uint, filter func(*Entry) bool) ([]Entry, error) {
  it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
  defer C.rocksdb_iter_destroy(it)

  var entries []Entry
  var count uint = 0

  firstKeyPtr, firstKeyLen := uintToKey(EntryKey, since + 1)
  defer freePtr(firstKeyPtr)

  C.go_rocksdb_iter_seek(it, firstKeyPtr, firstKeyLen)

  for (count < max) && (C.rocksdb_iter_valid(it) != 0) {
    _, keyType, entry, err := readIterPosition(it)
    if err != nil { return nil, err }

    if (keyType != EntryKey) {
      break
    }

    if filter(entry) {
      entries = append(entries, *entry)
      count++
    }

    C.rocksdb_iter_next(it)
  }

  return entries, nil
}

func (s *LevelDBStorage) GetLastIndex() (uint64, uint64, error) {
  it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
  defer C.rocksdb_iter_destroy(it)

  C.rocksdb_iter_seek_to_last(it)

  if C.rocksdb_iter_valid(it) == 0 {
    return 0, 0, nil
  }

  index, keyType, entry, err := readIterPosition(it)
  if err != nil { return 0, 0, err }

  if keyType != EntryKey { return 0, 0, nil }
  return index, entry.Term, nil
}

/*
 * Read index, term, and data from current iterator position and free pointers
 * to data returned by LevelDB. Assumes that the iterator is valid at this
 * position!
 */
func readIterPosition(it *C.rocksdb_iterator_t) (uint64, int, *Entry, error) {
  var keyLen C.size_t
  keyPtr := C.rocksdb_iter_key(it, &keyLen)

  keyType, key, err := keyToUint(unsafe.Pointer(keyPtr), keyLen)
  if err != nil { return 0, 0, nil, err }

  if keyType != EntryKey {
    // This function is just for reading index entries. Short-circuit if we see something else.
    return key, keyType, nil, nil
  }

  var valLen C.size_t
  valPtr := C.rocksdb_iter_value(it, &valLen)

  entry, err := ptrToEntry(unsafe.Pointer(valPtr), valLen)
  if err != nil { return 0, 0, nil, err }
  return key, keyType, entry, nil
}

// Return index and term of everything from index to the end
func (s *LevelDBStorage) GetEntryTerms(first uint64) (map[uint64]uint64, error) {
  it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
  defer C.rocksdb_iter_destroy(it)

  terms := make(map[uint64]uint64)

  firstKeyPtr, firstKeyLen := uintToKey(EntryKey, first)
  defer freePtr(firstKeyPtr)

  C.go_rocksdb_iter_seek(it, firstKeyPtr, firstKeyLen)

  for C.rocksdb_iter_valid(it) != 0 {
    index, keyType, entry, err := readIterPosition(it)
    if err != nil { return nil, err }
    if keyType != EntryKey {
      return terms, nil
    }

    terms[index] = entry.Term

    C.rocksdb_iter_next(it)
  }
  return terms, nil
}

// Delete everything that is greater than or equal to the index
func (s *LevelDBStorage) DeleteEntries(first uint64) error {
  it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
  defer C.rocksdb_iter_destroy(it)

  firstKeyPtr, firstKeyLen := uintToKey(EntryKey, first)
  defer freePtr(firstKeyPtr)

  C.go_rocksdb_iter_seek(it, firstKeyPtr, firstKeyLen)

  for C.rocksdb_iter_valid(it) != 0 {
    var keyLen C.size_t
    keyPtr := C.rocksdb_iter_key(it, &keyLen)

    keyType, key, err := keyToUint(unsafe.Pointer(keyPtr), keyLen)
    if err != nil { return err }
    if keyType != EntryKey {
      return nil
    }

    delPtr, delLen := uintToKey(EntryKey, key)
    defer freePtr(delPtr)

    var e *C.char
    C.go_rocksdb_delete(s.db, defaultWriteOptions, s.entries, delPtr, delLen, &e)
    if e != nil {
      defer freeString(e)
      return stringToError(e)
    }

    C.rocksdb_iter_next(it)
  }
  return nil
}

// Methods for secondary indices

func (s *LevelDBStorage) CreateTenant(tenantName string) (*uuid.UUID, error) {
  id := uuid.NewV4()

  // This record delimits the collection for when we are iterating on it
  // It also lets us read the name later given the ID
  err := s.writeCollectionDelimiters(&id, tenantName)
  if err != nil { return nil, err }

  // Put an entry in the special tenants collection that contains the new tenant ID
  valPtr, valLen := uuidToPtr(&id)
  defer freePtr(valPtr)
  err = s.writeCollectionEntry(&tenantsCollection, tenantName, valPtr, valLen)
  if err != nil { return nil, err }

  return &id, nil
}

func (s *LevelDBStorage) GetTenantByName(tenantName string) (*uuid.UUID, error) {
  // Read the entry from the special "tenants" collection
  ptr, len, err := s.readCollectionEntry(&tenantsCollection, tenantName)
  if err != nil { return nil, err }
  if ptr == nil { return nil, nil }
  defer freePtr(ptr)
  id, err := ptrToUuid(ptr, len)
  if err != nil { return nil, err }

  return id, nil
}

func (s *LevelDBStorage) GetTenantByID(tenantID *uuid.UUID) (string, error) {
  // Read the special start of tenant record which happens to include the name
  name, err := s.readCollectionStart(tenantID)
  if err != nil { return "", err }
  return name, nil
}

func (s *LevelDBStorage) GetTenants() ([]Collection, error) {
  // Start from the start and read until the end!
  startPtr, startLen, err := startIndexToPtr(&tenantsCollection)
  if err != nil { return nil, err }
  defer freePtr(startPtr)
  endPtr, endLen, err := endIndexToPtr(&tenantsCollection)
  if err != nil { return nil, err }
  defer freePtr(endPtr)

  var ret []Collection

  err = s.readCollectionIndex(startPtr, startLen, endPtr, endLen, math.MaxUint32,
    func(keyPtr unsafe.Pointer, keyLen C.size_t, valPtr unsafe.Pointer, valLen C.size_t) error {
      _, _, name, err := ptrToIndexKey(keyPtr, keyLen)
      if err != nil { return err }

      id, err := ptrToUuid(valPtr, valLen)
      if err != nil { return err }

      coll := Collection{
        Id: id,
        Name: name,
      }
      ret = append(ret, coll)
      return nil
    })

  return ret, err
}

func (s *LevelDBStorage) ensureTenant(tenantID *uuid.UUID) error {
  tenantName, err := s.readCollectionStart(tenantID)
  if err != nil { return err }
  if tenantName == "" {
    return fmt.Errorf("Unknown tenant \"%s\"", tenantID)
  }
  return nil
}

func (s *LevelDBStorage) CreateCollection(tenantID *uuid.UUID, collectionName string) (*uuid.UUID, error) {
  err := s.ensureTenant(tenantID)
  if err != nil { return nil, err }

  id := uuid.NewV4()

  // This record delimits the collection for when we are iterating on it
  // It also lets us read the name later given the ID
  err = s.writeCollectionDelimiters(&id, collectionName)
  if err != nil { return nil, err }

  // Put an entry in the tenants' own collection that contains the new tenant ID
  valPtr, valLen := uuidToPtr(&id)
  defer freePtr(valPtr)
  err = s.writeCollectionEntry(tenantID, collectionName, valPtr, valLen)
  if err != nil { return nil, err }

  return &id, nil
}

func (s *LevelDBStorage) GetCollectionByName(tenantID *uuid.UUID, collectionName string) (*uuid.UUID, error) {
  // Read the entry from the tenants' collection
  ptr, len, err := s.readCollectionEntry(tenantID, collectionName)
  if err != nil { return nil, err }
  if ptr == nil { return nil, nil }
  defer freePtr(ptr)

  id, err := ptrToUuid(ptr, len)
  if err != nil { return nil, err }

  return id, nil
}

func (s *LevelDBStorage) GetCollectionByID(collectionID *uuid.UUID) (string, error) {
  // Read the special start of tenant record which happens to include the name
  name, err := s.readCollectionStart(collectionID)
  if err != nil { return "", err }
  return name, nil
}

func (s *LevelDBStorage) GetTenantCollections(tenantID *uuid.UUID) ([]Collection, error) {
  err := s.ensureTenant(tenantID)
  if err != nil { return nil, err }

  // Start from the start and read until the end!
  startPtr, startLen, err := startIndexToPtr(tenantID)
  if err != nil { return nil, err }
  defer freePtr(startPtr)
  endPtr, endLen, err := endIndexToPtr(tenantID)
  if err != nil { return nil, err }
  defer freePtr(endPtr)

  var ret []Collection

  err = s.readCollectionIndex(startPtr, startLen, endPtr, endLen, math.MaxUint32,
    func(keyPtr unsafe.Pointer, keyLen C.size_t, valPtr unsafe.Pointer, valLen C.size_t) error {
      _, _, name, err := ptrToIndexKey(keyPtr, keyLen)
      if err != nil { return err }

      id, err := ptrToUuid(valPtr, valLen)
      if err != nil { return err }

      coll := Collection{
        Id: id,
        Name: name,
      }
      ret = append(ret, coll)
      return nil
    })

  return ret, err
}

func (s *LevelDBStorage) ensureCollection(collectionID *uuid.UUID) error {
  tenantName, err := s.readCollectionStart(collectionID)
  if err != nil { return err }
  if tenantName == "" {
    return fmt.Errorf("Unknown collection \"%s\"", collectionID)
  }
  return nil
}

func (s *LevelDBStorage) SetIndexEntry(collectionID *uuid.UUID, key string, index uint64) error {
  err := s.ensureCollection(collectionID)
  if err != nil { return err }

  valPtr, valLen := uintToPtr(index)
  defer freePtr(valPtr)

  return s.writeCollectionEntry(collectionID, key, valPtr, valLen)
}

func (s *LevelDBStorage) DeleteIndexEntry(collectionID *uuid.UUID, key string) error {
  err := s.ensureCollection(collectionID)
  if err != nil { return err }

  return s.deleteCollectionEntry(collectionID, key)
}

func (s *LevelDBStorage) GetIndexEntry(collectionID *uuid.UUID, key string) (uint64, error) {
  err := s.ensureCollection(collectionID)
  if err != nil { return 0, err }

  ptr, len, err := s.readCollectionEntry(collectionID, key)
  if err != nil { return 0, err }
  if ptr == nil { return 0, nil }

  defer freePtr(ptr)

  ix := ptrToUint(ptr, len)
  return ix, nil
}

func (s *LevelDBStorage) GetCollectionIndices(collectionID *uuid.UUID, lastKey string, max uint) ([]uint64, error) {
  err := s.ensureCollection(collectionID)
  if err != nil { return nil, err }

  var startPtr unsafe.Pointer
  var startLen C.size_t
  if lastKey == "" {
    startPtr, startLen, err = startIndexToPtr(collectionID)
  } else {
    startPtr, startLen, err = indexKeyToPtr(collectionID, lastKey)
  }
  if err != nil { return nil, err }
  defer freePtr(startPtr)

  endPtr, endLen, err := endIndexToPtr(collectionID)
  if err != nil { return nil, err }
  defer freePtr(endPtr)

  var ret []uint64

  err = s.readCollectionIndex(startPtr, startLen, endPtr, endLen, max,
    func(keyPtr unsafe.Pointer, keyLen C.size_t, valPtr unsafe.Pointer, valLen C.size_t) error {
      index := ptrToUint(valPtr, valLen)
      ret = append(ret, index)
      return nil
    })

  return ret, err
}

func (s *LevelDBStorage) CreateTenantEntry(entry *Entry) error {
  keyPtr, keyLen := tenantIndexToPtr(entry.Tenant, entry.Index)
  defer freePtr(keyPtr)

  valPtr, valLen := entryToPtr(entry)
  defer freePtr(valPtr)

  glog.V(2).Infof("Appending entry: %s", entry)
  return s.putEntry(s.tenantIndices, keyPtr, keyLen, valPtr, valLen)
}

func (s *LevelDBStorage) GetTenantEntry(tenant *uuid.UUID, ix uint64) (*Entry, error) {
  keyPtr, keyLen := tenantIndexToPtr(tenant, ix)
  defer freePtr(keyPtr)

  valPtr, valLen, err := s.readEntry(s.tenantIndices, keyPtr, keyLen)
  if err != nil { return nil, err }
  if valPtr == nil { return nil, nil }

  defer freePtr(valPtr)
  return ptrToEntry(valPtr, valLen)
}

func (s *LevelDBStorage) DeleteTenantEntry(tenant *uuid.UUID, ix uint64) error {
  ixPtr, ixLen := tenantIndexToPtr(tenant, ix)
  defer freePtr(ixPtr)

  var e *C.char

  C.go_rocksdb_delete(
    s.db, defaultWriteOptions, s.tenantIndices,
    ixPtr, ixLen, &e)
  if e == nil {
    return nil
  }
  defer freeString(e)
  return stringToError(e)
}

func (s *LevelDBStorage) GetTenantEntries(
    tenant *uuid.UUID, last uint64,
    max uint, filter func(*Entry) bool) ([]Entry, error) {
  it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.tenantIndices)
  defer C.rocksdb_iter_destroy(it)

  var entries []Entry
  var count uint = 0

  firstKeyPtr, firstKeyLen := tenantIndexToPtr(tenant, last + 1)
  defer freePtr(firstKeyPtr)

  C.go_rocksdb_iter_seek(it, firstKeyPtr, firstKeyLen)

  for (count < max) && (C.rocksdb_iter_valid(it) != 0) {
    var keyLen C.size_t
    keyPtr := unsafe.Pointer(C.rocksdb_iter_key(it, &keyLen))

    ixId, _, err := ptrToTenantIndex(keyPtr, keyLen)
    if err != nil { return nil, err }

    if !uuid.Equal(*tenant, *ixId) {
      break
    }

    var valLen C.size_t
    valPtr := unsafe.Pointer(C.rocksdb_iter_value(it, &valLen))

    entry, err := ptrToEntry(valPtr, valLen)
    if err != nil { return nil, err }

    if filter(entry) {
      entries = append(entries, *entry)
      count++
    }

    C.rocksdb_iter_next(it)
  }
  return entries, nil
}

func (s *LevelDBStorage) putEntry(
  cf *C.rocksdb_column_family_handle_t,
  keyPtr unsafe.Pointer, keyLen C.size_t,
  valPtr unsafe.Pointer, valLen C.size_t) error {

  var e *C.char
  C.go_rocksdb_put(
    s.db, defaultWriteOptions, cf,
    keyPtr, keyLen, valPtr, valLen,
    &e)
  if e == nil {
    return nil
  }
  defer freeString(e)
  return stringToError(e)
}

func (s *LevelDBStorage) readEntry(
  cf *C.rocksdb_column_family_handle_t,
  keyPtr unsafe.Pointer, keyLen C.size_t) (unsafe.Pointer, C.size_t, error) {

  var valLen C.size_t
  var e *C.char

  val := C.go_rocksdb_get(
    s.db, defaultReadOptions, cf,
    keyPtr, keyLen,
    &valLen, &e)

  if val == nil {
    if e == nil {
      return nil, 0, nil
    } else {
      defer freeString(e)
      return nil, 0, stringToError(e)
    }
  } else {
    return unsafe.Pointer(val), valLen, nil
  }
}