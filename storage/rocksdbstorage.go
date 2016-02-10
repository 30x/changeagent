package storage

/*
#include <stdlib.h>
#include "rocksdb_native.h"
#cgo CFLAGS: -O3 -Wall -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lrocksdb
*/
import "C"

import (
  "errors"
  "fmt"
  "io"
  "unsafe"
  "github.com/golang/glog"
)

var defaultWriteOptions *C.rocksdb_writeoptions_t = C.rocksdb_writeoptions_create();
var defaultReadOptions *C.rocksdb_readoptions_t = C.rocksdb_readoptions_create();

type LevelDBStorage struct {
  baseFile string
  db *C.rocksdb_t
  cache *C.rocksdb_cache_t
}

func CreateRocksDBStorage(baseFile string, cacheSize uint) (*LevelDBStorage, error) {
  stor := &LevelDBStorage{
    baseFile: baseFile,
  }

  cache := C.rocksdb_cache_create_lru(C.size_t(cacheSize))

  opts := C.rocksdb_options_create()
  defer C.rocksdb_options_destroy(opts)
  C.rocksdb_options_set_create_if_missing(opts, 1)
  C.rocksdb_options_set_comparator(opts, C.go_create_comparator())

  blockOpts := C.rocksdb_block_based_options_create()
  C.rocksdb_block_based_options_set_block_cache(blockOpts, cache)

  C.rocksdb_options_set_block_based_table_factory(opts, blockOpts)

  db, err := stor.openDb(opts)
  if err != nil { return nil, err }
  stor.db = db
  stor.cache = cache
  glog.Infof("Opened RocksDB file in %s", stor.baseFile)



  return stor, nil
}

func (s *LevelDBStorage) GetDataPath() string {
  return s.baseFile
}

func (s *LevelDBStorage) openDb(opts *C.rocksdb_options_t) (*C.rocksdb_t, error) {
  var e *C.char
  dbCName := C.CString(s.baseFile)
  defer freeString(dbCName)
  db := C.rocksdb_open(opts, dbCName, &e)

  if db == nil {
    if e == nil {
      return nil, errors.New("Error opening DB")
    } else {
      defer freeString(e)
      return nil, stringToError(e)
    }
  }
  return db, nil
}

func (s *LevelDBStorage) Close() {
  C.rocksdb_cache_destroy(s.cache)
  C.rocksdb_close(s.db)
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
  it := C.rocksdb_create_iterator(s.db, defaultReadOptions)
  defer C.rocksdb_iter_destroy(it)

  fmt.Fprintln(out, "Start Dump...")
  C.rocksdb_iter_seek_to_first(it)

  for i := 0; i < max && C.rocksdb_iter_valid(it) != 0; i++ {
    var e *C.char
    var keyLen C.size_t
    var valLen C.size_t

    C.rocksdb_iter_get_error(it, &e)
    if e != nil {
      defer freeString(e)
      err := stringToError(e)
      fmt.Fprintf(out, "Iterator error: %s", err)
      return
    }

    keyPtr := unsafe.Pointer(C.rocksdb_iter_key(it, &keyLen))
    key := ptrToBytes(keyPtr, keyLen)
    valPtr := unsafe.Pointer(C.rocksdb_iter_value(it, &valLen))

    _, kt := parseKeyPrefix(key[0])
    switch kt {
    case MetadataKey:
      _, mkey, _ := keyToUint(keyPtr, keyLen)
      fmt.Fprintf(out,          "Metadata   (%d) %d bytes\n", mkey, valLen)
      break
    case IndexKey:
      tenName, colName, ixLen, _ := ptrToIndexType(valPtr, valLen)
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
      break
    case EntryKey:
      _, ekey, _ := keyToUint(keyPtr, keyLen)
      entry, _ := ptrToEntry(valPtr, valLen)
      fmt.Fprintf(out,          "Entry       (%d) %s\n", ekey, entry)
      break
    }

    C.rocksdb_iter_next(it)
  }

  fmt.Fprintln(out, "Done.")
}

func (s *LevelDBStorage) GetMetadata(key uint) (uint64, error) {
  keyBuf, keyLen := uintToKey(MetadataKey, uint64(key))
  defer freePtr(keyBuf)

  val, valLen, err := s.readEntry(keyBuf, keyLen)
  if err != nil { return 0, err }
  if val == nil { return 0, nil }

  defer freePtr(val)
  return ptrToUint(val, valLen), nil
}

func (s *LevelDBStorage) GetRawMetadata(key uint) ([]byte, error) {
  keyBuf, keyLen := uintToKey(MetadataKey, uint64(key))
  defer freePtr(keyBuf)

  val, valLen, err := s.readEntry(keyBuf, keyLen)
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

  return s.putEntry(keyBuf, keyLen, valBuf, valLen)
}

func (s *LevelDBStorage) SetRawMetadata(key uint, val []byte) error {
  keyBuf, keyLen := uintToKey(MetadataKey, uint64(key))
  defer freePtr(keyBuf)
  valBuf, valLen := bytesToPtr(val)
  defer freePtr(valBuf)

  return s.putEntry(keyBuf, keyLen, valBuf, valLen)
}

// Methods for the Raft index

func (s *LevelDBStorage) AppendEntry(entry *Entry) error {
  keyPtr, keyLen := uintToKey(EntryKey, entry.Index)
  defer freePtr(keyPtr)

  valPtr, valLen := entryToPtr(entry)
  defer freePtr(valPtr)

  glog.V(2).Infof("Appending entry: %s", entry)
  return s.putEntry(keyPtr, keyLen, valPtr, valLen)
}

  // Get term and data for entry. Return term 0 if not found.
func (s *LevelDBStorage) GetEntry(index uint64) (*Entry, error) {
  keyPtr, keyLen := uintToKey(EntryKey, index)
  defer freePtr(keyPtr)

  valPtr, valLen, err := s.readEntry(keyPtr, keyLen)
  if err != nil { return nil, err }
  if valPtr == nil { return nil, nil }

  defer freePtr(valPtr)
  return ptrToEntry(valPtr, valLen)
}

func (s *LevelDBStorage) GetEntries(first uint64, last uint64) ([]Entry, error) {
  it := C.rocksdb_create_iterator(s.db, defaultReadOptions)
  defer C.rocksdb_iter_destroy(it)

  var entries []Entry

  firstKeyPtr, firstKeyLen := uintToKey(EntryKey, first)
  defer freePtr(firstKeyPtr)

  C.go_rocksdb_iter_seek(it, firstKeyPtr, firstKeyLen)

  for C.rocksdb_iter_valid(it) != 0 {
    index, keyType, entry, err := readIterPosition(it)
    if err != nil { return nil, err }
    if (keyType != EntryKey) || (index > last) {
      return entries, nil
    }

    entries = append(entries, *entry)

    C.rocksdb_iter_next(it)
  }
  var entryList []uint64
  for _, e := range(entries) {
    entryList = append(entryList, e.Index)
  }
  return entries, nil
}

func (s *LevelDBStorage) GetLastIndex() (uint64, uint64, error) {
  it := C.rocksdb_create_iterator(s.db, defaultReadOptions)
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
  it := C.rocksdb_create_iterator(s.db, defaultReadOptions)
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
  it := C.rocksdb_create_iterator(s.db, defaultReadOptions)
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
    C.go_rocksdb_delete(s.db, defaultWriteOptions, delPtr, delLen, &e)
    if e != nil {
      defer freeString(e)
      return stringToError(e)
    }

    C.rocksdb_iter_next(it)
  }
  return nil
}

  // Methods for secondary indices
func (s *LevelDBStorage) CreateTenant(tenantName string) error {
  valBuf, valLen := uintToPtr(0)
  defer freePtr(valBuf)

  // TODO this could be a batch

  keyBuf, keyLen, err := startTenantToPtr(tenantName)
  if err != nil { return err }
  err = s.putEntry(keyBuf, keyLen, valBuf, valLen)
  freePtr(keyBuf)
  if err != nil { return err }

  endBuf, endLen, err := endTenantToPtr(tenantName)
  if err != nil { return err }
  err = s.putEntry(endBuf, endLen, valBuf, valLen)
  freePtr(endBuf)
  if err != nil { return err }

  return nil
}

func (s *LevelDBStorage) TenantExists(tenantName string) (bool, error) {
  keyBuf, keyLen, err := startTenantToPtr(tenantName)
  if err != nil { return false, err }
  defer freePtr(keyBuf)

  valPtr, _, err := s.readEntry(keyBuf, keyLen)
  if err != nil { return false, err }
  if valPtr == nil { return false, nil }
  freePtr(valPtr)
  return true, nil
}

func (s *LevelDBStorage) CreateCollection(tenantName string, collectionName string) error {
  valBuf, valLen := uintToPtr(0)
  defer freePtr(valBuf)

  keyBuf, keyLen, err := startCollectionToPtr(tenantName, collectionName)
  if err != nil { return err }
  err = s.putEntry(keyBuf, keyLen, valBuf, valLen)
  freePtr(keyBuf)
  if err != nil { return err }

  endBuf, endLen, err := endCollectionToPtr(tenantName, collectionName)
  if err != nil { return err }
  err = s.putEntry(endBuf, endLen, valBuf, valLen)
  freePtr(endBuf)
  if err != nil { return err }

  return nil
}

func (s *LevelDBStorage) CollectionExists(tenantName, collectionName string) (bool, error) {
  keyBuf, keyLen, err := startCollectionToPtr(tenantName, collectionName)
  if err != nil { return false, err }
  defer freePtr(keyBuf)

  valPtr, _, err := s.readEntry(keyBuf, keyLen)
  if err != nil { return false, err }
  if valPtr == nil { return false, nil }
  freePtr(valPtr)
  return true, nil
}

func (s *LevelDBStorage) SetIndexEntry(tenantName, collectionName, key string, index uint64) error {
  if index == 0 {
    return errors.New("Invalid index value")
  }

  entry := &Entry{
    Tenant: tenantName,
    Collection: collectionName,
    Key: key,
  }
  keyPtr, keyLen, err := indexKeyToPtr(entry)
  if err != nil { return err }
  defer freePtr(keyPtr)

  valPtr, valLen := uintToPtr(index)
  defer freePtr(valPtr)

  return s.putEntry(keyPtr, keyLen, valPtr, valLen)
}

func (s *LevelDBStorage) DeleteIndexEntry(tenantName, collectionName, key string) error {
  var e *C.char

  entry := &Entry{
    Tenant: tenantName,
    Collection: collectionName,
    Key: key,
  }
  keyPtr, keyLen, err := indexKeyToPtr(entry)
  if err != nil { return err }
  defer freePtr(keyPtr)

  C.go_rocksdb_delete(
    s.db, defaultWriteOptions,
    keyPtr, keyLen, &e)
  if e == nil {
    return nil
  }
  defer freeString(e)
  return stringToError(e)
}

func (s *LevelDBStorage) GetIndexEntry(tenantName, collectionName, key string) (uint64, error) {
  entry := &Entry{
    Tenant: tenantName,
    Collection: collectionName,
    Key: key,
  }
  keyPtr, keyLen, err := indexKeyToPtr(entry)
  if err != nil { return 0, err }

  valPtr, valLen, err := s.readEntry(keyPtr, keyLen)
  freePtr(keyPtr)
  if err != nil { return 0, err }
  if valPtr == nil { return 0, nil }

  index := ptrToUint(valPtr, valLen)
  freePtr(valPtr)
  return index, nil
}

func (s *LevelDBStorage) GetCollectionIndices(tenantName, collectionName string, max uint) ([]uint64, error) {
  startPtr, startLen, err := startCollectionToPtr(tenantName, collectionName)
  if err != nil { return nil, err }
  defer freePtr(startPtr)
  endPtr, endLen, err := endCollectionToPtr(tenantName, collectionName)
  if err != nil { return nil, err }
  defer freePtr(endPtr)

  it := C.rocksdb_create_iterator(s.db, defaultReadOptions)
  defer C.rocksdb_iter_destroy(it)

  C.go_rocksdb_iter_seek(it, startPtr, startLen)

  var ret []uint64

  for count := uint(0); (count < max) && (C.rocksdb_iter_valid(it) != 0); count++ {
    var keyLen C.size_t
    keyPtr := unsafe.Pointer(C.rocksdb_iter_key(it, &keyLen))
    if compareKeys(keyPtr, keyLen, startPtr, startLen) == 0 {
      // Always skip the start-of-range key
      C.rocksdb_iter_next(it)
      continue
    }
    if compareKeys(keyPtr, keyLen, endPtr, endLen) == 0 {
      // The end-of-range key indicates, well, the end
      break
    }

    var valLen C.size_t
    valPtr := C.rocksdb_iter_value(it, &valLen)
    index := ptrToUint(unsafe.Pointer(valPtr), valLen)
    ret = append(ret, index)

    C.rocksdb_iter_next(it)
  }

  return ret, nil
}

func (s *LevelDBStorage) GetTenantCollections(tenantName string) ([]string, error) {
  startPtr, startLen, err := startTenantToPtr(tenantName)
  if err != nil { return nil, err }
  defer freePtr(startPtr)
  endPtr, endLen, err := endTenantToPtr(tenantName)
  if err != nil { return nil, err }
  defer freePtr(endPtr)

  it := C.rocksdb_create_iterator(s.db, defaultReadOptions)
  defer C.rocksdb_iter_destroy(it)

  C.go_rocksdb_iter_seek(it, startPtr, startLen)

  var ret []string

  for C.rocksdb_iter_valid(it) != 0 {
    var keyLen C.size_t
    keyPtr := unsafe.Pointer(C.rocksdb_iter_key(it, &keyLen))
    if compareKeys(keyPtr, keyLen, startPtr, startLen) == 0 {
      // Always skip the start-of-range key
      C.rocksdb_iter_next(it)
      continue
    }
    if compareKeys(keyPtr, keyLen, endPtr, endLen) == 0 {
      // The end-of-range key indicates, well, the end
      break
    }

    _, collName, len, err := ptrToIndexType(keyPtr, keyLen)
    if err != nil { return nil, err }
    if collName == "" {
      // Shouldn't get here because we just checked for end of tenant
      return nil, errors.New("Received bogus tenant record")
    }

    if len == startRange {
      // Found a start-of-collection record
      ret = append(ret, collName)
    }

    C.rocksdb_iter_next(it)
  }

  return ret, nil
}

func (s *LevelDBStorage) putEntry(
  keyPtr unsafe.Pointer, keyLen C.size_t,
  valPtr unsafe.Pointer, valLen C.size_t) error {

  var e *C.char
  C.go_rocksdb_put(
    s.db, defaultWriteOptions,
    keyPtr, keyLen, valPtr, valLen,
    &e)
  if e == nil {
    return nil
  }
  defer freeString(e)
  return stringToError(e)
}

func (s *LevelDBStorage) readEntry(
  keyPtr unsafe.Pointer, keyLen C.size_t) (unsafe.Pointer, C.size_t, error) {

  var valLen C.size_t
  var e *C.char

  val := C.go_rocksdb_get(
    s.db, defaultReadOptions,
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