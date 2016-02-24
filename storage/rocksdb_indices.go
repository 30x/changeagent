package storage

/*
#include <stdlib.h>
#include "rocksdb_native.h"
*/
import "C"

import (
  "unsafe"
  "github.com/satori/go.uuid"
)

/*
 * These records must be written so that we can iterate over the elements of a collection.
 */
func (s *LevelDBStorage) writeCollectionDelimiters(id *uuid.UUID, name string) error {
  valBuf, valLen := stringToPtr(name)
  defer freePtr(valBuf)

  startBuf, startLen, err := startIndexToPtr(id)
  if err != nil { return err }
  defer freePtr(startBuf)
  err = s.putEntry(s.indices, startBuf, startLen, valBuf, valLen)
  if err != nil { return err }

  endBuf, endLen, err := endIndexToPtr(id)
  if err != nil { return err }
  defer freePtr(endBuf)
  err = s.putEntry(s.indices, endBuf, endLen, valBuf, valLen)
  if err != nil { return err }

  return nil
}

/*
 * Return the name of a collection, or an empty string if it is not defined
 */
func (s *LevelDBStorage) readCollectionStart(id *uuid.UUID) (string, error) {
  startBuf, startLen, err := startIndexToPtr(id)
  if err != nil { return "", err }
  defer freePtr(startBuf)

  ptr, len, err := s.readEntry(s.indices, startBuf, startLen)
  if err != nil { return "", err }
  if ptr == nil { return "", nil }

  defer freePtr(ptr)
  ret, err := ptrToString(ptr, len)
  if err != nil { return "", err }
  return ret, nil
}

/*
 * Write arbitrary data to a collection index.
 */
func (s *LevelDBStorage) writeCollectionEntry(id *uuid.UUID, key string, ptr unsafe.Pointer, len C.size_t) error {
  ixPtr, ixLen, err := indexKeyToPtr(id, key)
  if err != nil { return err }
  defer freePtr(ixPtr)
  err = s.putEntry(s.indices, ixPtr, ixLen, ptr, len)
  return err
}

/*
 * Read arbitrary data from a collection index.
 */
func (s *LevelDBStorage) readCollectionEntry(id *uuid.UUID, key string) (unsafe.Pointer, C.size_t, error) {
  ixPtr, ixLen, err := indexKeyToPtr(id, key)
  if err != nil { return nil, 0, err }
  defer freePtr(ixPtr)
  ptr, len, err := s.readEntry(s.indices, ixPtr, ixLen)
  if err != nil { return nil, 0, err }
  return ptr, len, nil
}

/*
 * Delete from an index.
 */
func (s *LevelDBStorage) deleteCollectionEntry(id *uuid.UUID, key string) error {
  ixPtr, ixLen, err := indexKeyToPtr(id, key)
  if err != nil { return err }
  defer freePtr(ixPtr)

  var e *C.char

  C.go_rocksdb_delete(
    s.db, defaultWriteOptions, s.indices,
    ixPtr, ixLen, &e)
  if e == nil {
    return nil
  }
  defer freeString(e)
  return stringToError(e)
}

/*
 * Read entries from the index, using a provided function to allow each to be converted to the
 * correct format by the caller.
 */
func (s *LevelDBStorage) readCollectionIndex(startPtr unsafe.Pointer, startLen C.size_t,
                                             endPtr unsafe.Pointer, endLen C.size_t, max uint,
                                             handle func(unsafe.Pointer, C.size_t, unsafe.Pointer, C.size_t) error) error {

  it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.indices)
  defer C.rocksdb_iter_destroy(it)

  C.go_rocksdb_iter_seek(it, startPtr, startLen)

  var count uint = 0
  for (count < max) && (C.rocksdb_iter_valid(it) != 0) {
    var keyLen C.size_t
    keyPtr := unsafe.Pointer(C.rocksdb_iter_key(it, &keyLen))
    if compareKeys(keyPtr, keyLen, startPtr, startLen) == 0 {
      // Always skip the start-of-range key
      C.rocksdb_iter_next(it)
      continue
    }
    if compareKeys(keyPtr, keyLen, endPtr, endLen) == 0 {
      // The end-of-range key indicates, its time to stop seeking
      break
    }

    var valLen C.size_t
    valPtr := unsafe.Pointer(C.rocksdb_iter_value(it, &valLen))

    err := handle(keyPtr, keyLen, valPtr, valLen)
    if err != nil { return err }

    C.rocksdb_iter_next(it)
    count++
  }

  return nil
}