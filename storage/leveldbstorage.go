package storage

/*
#include <stdio.h>
#include <stdlib.h>
#include <leveldb/c.h>
#cgo LDFLAGS: -lleveldb

static char* go_leveldb_get(
    leveldb_t* db,
    const leveldb_readoptions_t* options,
    const void* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  return leveldb_get(db, options, (const char*)key, keylen, vallen, errptr);
}

static void go_leveldb_put(
    leveldb_t* db,
    const leveldb_writeoptions_t* options,
    const void* key, size_t keylen,
    const void* val, size_t vallen,
    char** errptr) {
  leveldb_put(db, options, (const char*)key, keylen,
              (const char*)val, vallen, errptr);
}

static void go_leveldb_delete(
    leveldb_t* db,
    const leveldb_writeoptions_t* options,
    const void* key, size_t keylen,
    char** errptr) {
  leveldb_delete(db, options, (const char*)key, keylen, errptr);
}

static void go_leveldb_iter_seek(leveldb_iterator_t* it,
    const void* k, size_t klen) {
  leveldb_iter_seek(it, (const char*)k, klen);
}

int go_compare_bytes(
  void* state,
  const char* a, size_t alen,
  const char* b, size_t blen) {
  if ((alen != 9) || (blen != 9)) { return 0; }
  if (a[0] == b[0]) {
    unsigned long long* av = (unsigned long long*)(&a[1]);
    unsigned long long* bv = (unsigned long long*)(&b[1]);
    if (*av > *bv) {
      return 1;
    } else if (*av < *bv) {
      return -1;
    } else {
      return 0;
    }
  } else if (a[0] > b[0]) {
    return 1;
  } else {
    return -1;
  }
}

static const char* go_comparator_name(void* v) {
  return "ByteComparator";
}

static leveldb_comparator_t* go_create_comparator() {
  return leveldb_comparator_create(
    NULL, NULL, go_compare_bytes, go_comparator_name);
}
*/
import "C"

import (
  "errors"
  "unsafe"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

const (
  MetadataKey = 1
  EntryKey = 2
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
  log.Infof("Opened LevelDB file in %s using LevelDB %d.%d",
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
    log.Infof("Destroyed LevelDB database in %s", s.baseFile)
    return nil
  }
  defer freeString(e)
  err := stringToError(e)
  if err != nil {
    log.Infof("Error destroying LevelDB database: %s", err)
  }
  return err
}

func (s *LevelDBStorage) GetMetadata(key uint) (uint64, error) {
  var valLen C.size_t
  var e *C.char

  keyBuf, keyLen := uintToKey(MetadataKey, uint64(key))
  defer C.free(keyBuf)

  val := C.go_leveldb_get(
    s.db, defaultReadOptions,
    keyBuf, keyLen,
    &valLen, &e)

  if val == nil {
    if e == nil {
      return 0, nil
    } else {
      defer freeString(e)
      return 0, stringToError(e)
    }
  } else {
    defer freeString(val)
    val := ptrToUint(unsafe.Pointer(val), valLen)
    return val, nil
  }
}

func (s *LevelDBStorage) SetMetadata(key uint, val uint64) error {
  var e *C.char

  keyBuf, keyLen := uintToKey(MetadataKey, uint64(key))
  defer C.free(keyBuf)
  valBuf, valLen := uintToPtr(val)
  defer C.free(valBuf)

  C.go_leveldb_put(
    s.db, defaultWriteOptions,
    keyBuf, keyLen,
    valBuf, valLen,
    &e)
  if e == nil {
    return nil
  }
  defer freeString(e)
  return stringToError(e)
}

// Methods for the Raft index

func (s *LevelDBStorage) AppendEntry(index uint64, term uint64, data []byte) error {
  var e *C.char
  keyPtr, keyLen := uintToKey(EntryKey, index)
  defer C.free(keyPtr)
  valPtr, valLen := entryToPtr(term, data)
  defer C.free(valPtr)

  C.go_leveldb_put(
    s.db, defaultWriteOptions,
    keyPtr, keyLen,
    valPtr, valLen,
    &e)
  if e == nil {
    return nil
  }
  defer freeString(e)
  return stringToError(e)
}

  // Get term and data for entry. Return term 0 if not found.
func (s *LevelDBStorage) GetEntry(index uint64) (uint64, []byte, error) {
  var e *C.char
  var valLen C.size_t
  keyPtr, keyLen := uintToKey(EntryKey, index)
  defer C.free(keyPtr)

  valPtr := C.go_leveldb_get(
    s.db, defaultReadOptions,
    keyPtr, keyLen,
    &valLen, &e)

  if valPtr == nil {
    if e == nil {
      return 0, nil, nil
    } else {
      defer freeString(e)
      return 0, nil, stringToError(e)
    }
  } else {
    defer freeString(valPtr)
    return ptrToEntry(unsafe.Pointer(valPtr), valLen)
  }
}

func (s *LevelDBStorage) GetEntries(first uint64, last uint64) ([]Entry, error) {
  it := C.leveldb_create_iterator(s.db, defaultReadOptions)
  defer C.leveldb_iter_destroy(it)

  var entries []Entry

  firstKeyPtr, firstKeyLen := uintToKey(EntryKey, first)
  defer C.free(firstKeyPtr)

  C.go_leveldb_iter_seek(it, firstKeyPtr, firstKeyLen)

  for C.leveldb_iter_valid(it) != 0 {
    index, keyType, term, data, err := readIterPosition(it)
    if err != nil { return nil, err }
    if (keyType != EntryKey) || (index > last) {
      return entries, nil
    }

    ne := Entry{
      Index: index,
      Term: term,
      Data: data,
    }
    entries = append(entries, ne)

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

  index, keyType, term, _, err := readIterPosition(it)
  if err != nil { return 0, 0, err }

  if keyType != EntryKey { return 0, 0, nil }
  return index, term, nil
}

/*
 * Read index, term, and data from current iterator position and free pointers
 * to data returned by LevelDB. Assumes that the iterator is valid at this
 * position!
 */
func readIterPosition(it *C.leveldb_iterator_t) (uint64, int, uint64, []byte, error) {
  var keyLen C.size_t
  keyPtr := C.leveldb_iter_key(it, &keyLen)

  keyType, key, err := keyToUint(unsafe.Pointer(keyPtr), keyLen)
  if err != nil { return 0, 0, 0, nil, err }

  var valLen C.size_t
  valPtr := C.leveldb_iter_value(it, &valLen)

  term, data, err := ptrToEntry(unsafe.Pointer(valPtr), valLen)
  if err != nil { return 0, 0, 0, nil, err }

  return key, keyType, term, data, nil
}

// Return index and term of everything from index to the end
func (s *LevelDBStorage) GetEntryTerms(first uint64) (map[uint64]uint64, error) {
  it := C.leveldb_create_iterator(s.db, defaultReadOptions)
  defer C.leveldb_iter_destroy(it)

  terms := make(map[uint64]uint64)

  firstKeyPtr, firstKeyLen := uintToKey(EntryKey, first)
  defer C.free(firstKeyPtr)

  C.go_leveldb_iter_seek(it, firstKeyPtr, firstKeyLen)

  for C.leveldb_iter_valid(it) != 0 {
    index, keyType, term, _, err := readIterPosition(it)
    if err != nil { return nil, err }
    if keyType != EntryKey {
      return terms, nil
    }

    terms[index] = term

    C.leveldb_iter_next(it)
  }
  return terms, nil
}

// Delete everything that is greater than or equal to the index
func (s *LevelDBStorage) DeleteEntries(first uint64) error {
  it := C.leveldb_create_iterator(s.db, defaultReadOptions)
  defer C.leveldb_iter_destroy(it)

  firstKeyPtr, firstKeyLen := uintToKey(EntryKey, first)
  defer C.free(firstKeyPtr)

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
    defer C.free(delPtr)

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
