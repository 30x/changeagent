package storage

/*
#include <stdlib.h>
#include "leveldb_native.h"
#cgo CFLAGS: -O0 -g -Wall -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lleveldb
*/
import "C"

import (
  "errors"
  "fmt"
  "io"
  "unsafe"
  "encoding/hex"
  "github.com/golang/glog"
)

var defaultWriteOptions *C.leveldb_writeoptions_t = C.leveldb_writeoptions_create();
var defaultReadOptions *C.leveldb_readoptions_t = C.leveldb_readoptions_create();

type LevelDBStorage struct {
  baseFile string
  db *C.leveldb_t
}

func CreateLevelDBStorage(baseFile string) (*LevelDBStorage, error) {
  stor := &LevelDBStorage{
    baseFile: baseFile,
  }

  opts := C.leveldb_options_create()
  defer C.leveldb_options_destroy(opts)
  C.leveldb_options_set_create_if_missing(opts, 1)
  C.leveldb_options_set_comparator(opts, C.go_create_comparator())

  db, err := stor.openDb(opts)
  if err != nil { return nil, err }
  stor.db = db
  glog.Infof("Opened LevelDB file in %s using LevelDB %d.%d",
    stor.baseFile,
    C.leveldb_major_version(), C.leveldb_minor_version())

  return stor, nil
}

func (s *LevelDBStorage) GetDataPath() string {
  return s.baseFile
}

func (s *LevelDBStorage) openDb(opts *C.leveldb_options_t) (*C.leveldb_t, error) {
  var e *C.char
  dbCName := C.CString(s.baseFile)
  defer freeString(dbCName)
  db := C.leveldb_open(opts, dbCName, &e)

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
  C.leveldb_close(s.db)
}

func (s *LevelDBStorage) Delete() error {
  var e *C.char
  opts := C.leveldb_options_create()
  defer C.leveldb_options_destroy(opts)

  dbCName := C.CString(s.baseFile)
  defer freeString(dbCName)
  C.leveldb_destroy_db(opts, dbCName, &e)
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
  it := C.leveldb_create_iterator(s.db, defaultReadOptions)
  defer C.leveldb_iter_destroy(it)

  fmt.Fprintln(out, "Start Dump...")
  C.leveldb_iter_seek_to_first(it)

  for i := 0; i < max && C.leveldb_iter_valid(it) != 0; i++ {
    var e *C.char
    var keyLen C.size_t
    var valLen C.size_t

    C.leveldb_iter_get_error(it, &e)
    if e != nil {
      defer freeString(e)
      err := stringToError(e)
      fmt.Fprintf(out, "Iterator error: %s", err)
      return
    }

    keyPtr := C.leveldb_iter_key(it, &keyLen)
    key := ptrToBytes(unsafe.Pointer(keyPtr), keyLen)
    valPtr := C.leveldb_iter_value(it, &valLen)
    val := ptrToBytes(unsafe.Pointer(valPtr), valLen)

    fmt.Fprintf(out, "Key: %s (%d) Value: %s (%d)\n",
      hex.EncodeToString(key), keyLen,
      hex.EncodeToString(val), valLen)
    C.leveldb_iter_next(it)
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
  it := C.leveldb_create_iterator(s.db, defaultReadOptions)
  defer C.leveldb_iter_destroy(it)

  var entries []Entry

  firstKeyPtr, firstKeyLen := uintToKey(EntryKey, first)
  defer freePtr(firstKeyPtr)

  C.go_leveldb_iter_seek(it, firstKeyPtr, firstKeyLen)

  for C.leveldb_iter_valid(it) != 0 {
    index, keyType, entry, err := readIterPosition(it)
    if err != nil { return nil, err }
    if (keyType != EntryKey) || (index > last) {
      return entries, nil
    }

    entries = append(entries, *entry)

    C.leveldb_iter_next(it)
  }
  var entryList []uint64
  for _, e := range(entries) {
    entryList = append(entryList, e.Index)
  }
  return entries, nil
}

func (s *LevelDBStorage) GetLastIndex() (uint64, uint64, error) {
  it := C.leveldb_create_iterator(s.db, defaultReadOptions)
  defer C.leveldb_iter_destroy(it)

  C.leveldb_iter_seek_to_last(it)

  if C.leveldb_iter_valid(it) == 0 {
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
func readIterPosition(it *C.leveldb_iterator_t) (uint64, int, *Entry, error) {
  var keyLen C.size_t
  keyPtr := C.leveldb_iter_key(it, &keyLen)

  keyType, key, err := keyToUint(unsafe.Pointer(keyPtr), keyLen)
  if err != nil { return 0, 0, nil, err }

  if keyType != EntryKey {
    // This function is just for reading index entries. Short-circuit if we see something else.
    return key, keyType, nil, nil
  }

  var valLen C.size_t
  valPtr := C.leveldb_iter_value(it, &valLen)

  entry, err := ptrToEntry(unsafe.Pointer(valPtr), valLen)
  if err != nil { return 0, 0, nil, err }
  return key, keyType, entry, nil
}

// Return index and term of everything from index to the end
func (s *LevelDBStorage) GetEntryTerms(first uint64) (map[uint64]uint64, error) {
  it := C.leveldb_create_iterator(s.db, defaultReadOptions)
  defer C.leveldb_iter_destroy(it)

  terms := make(map[uint64]uint64)

  firstKeyPtr, firstKeyLen := uintToKey(EntryKey, first)
  defer freePtr(firstKeyPtr)

  C.go_leveldb_iter_seek(it, firstKeyPtr, firstKeyLen)

  for C.leveldb_iter_valid(it) != 0 {
    index, keyType, entry, err := readIterPosition(it)
    if err != nil { return nil, err }
    if keyType != EntryKey {
      return terms, nil
    }

    terms[index] = entry.Term

    C.leveldb_iter_next(it)
  }
  return terms, nil
}

// Delete everything that is greater than or equal to the index
func (s *LevelDBStorage) DeleteEntries(first uint64) error {
  it := C.leveldb_create_iterator(s.db, defaultReadOptions)
  defer C.leveldb_iter_destroy(it)

  firstKeyPtr, firstKeyLen := uintToKey(EntryKey, first)
  defer freePtr(firstKeyPtr)

  C.go_leveldb_iter_seek(it, firstKeyPtr, firstKeyLen)

  for C.leveldb_iter_valid(it) != 0 {
    var keyLen C.size_t
    keyPtr := C.leveldb_iter_key(it, &keyLen)

    keyType, key, err := keyToUint(unsafe.Pointer(keyPtr), keyLen)
    if err != nil { return err }
    if keyType != EntryKey {
      return nil
    }

    delPtr, delLen := uintToKey(EntryKey, key)
    defer freePtr(delPtr)

    var e *C.char
    C.go_leveldb_delete(s.db, defaultWriteOptions, delPtr, delLen, &e)
    if e != nil {
      defer freeString(e)
      return stringToError(e)
    }

    C.leveldb_iter_next(it)
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

  C.go_leveldb_delete(
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

func (s *LevelDBStorage) putEntry(
  keyPtr unsafe.Pointer, keyLen C.size_t,
  valPtr unsafe.Pointer, valLen C.size_t) error {

  var e *C.char
  C.go_leveldb_put(
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

  val := C.go_leveldb_get(
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