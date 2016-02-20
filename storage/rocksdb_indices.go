package storage

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
