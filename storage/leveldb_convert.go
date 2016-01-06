package storage

/*
#include <stdio.h>
#include <stdlib.h>
*/
import "C"

import (
  "bytes"
  "errors"
  "unsafe"
  "encoding/binary"
)

var byteOrder binary.ByteOrder = binary.LittleEndian

func freeString(c *C.char) {
  C.free(unsafe.Pointer(c))
}

func freePtr(ptr unsafe.Pointer) {
  C.free(ptr)
}

func stringToError(c *C.char) error {
  es := C.GoString(c)
  return errors.New(es)
}

func stringToPtr(s string) (unsafe.Pointer, C.size_t) {
  bs := []byte(s)
  return bytesToPtr(bs)
}

func stringToKey(keyType int, s string) (unsafe.Pointer, C.size_t) {
  bs := []byte(s)
  typeB := []byte{ byte(keyType) }
  bb := append(typeB, bs...)
  return bytesToPtr(bb)
}

func uintToKey(keyType int, v uint64) (unsafe.Pointer, C.size_t) {
  buf := &bytes.Buffer{}
  binary.Write(buf, byteOrder, byte(keyType))
  binary.Write(buf, byteOrder, v)
  return bytesToPtr(buf.Bytes())
}

func entryToPtr(term uint64, data []byte) (unsafe.Pointer, C.size_t) {
  termBuf := uintToBytes(term)
  bb := append(termBuf, data...)
  return bytesToPtr(bb)
}

func uintToPtr(v uint64) (unsafe.Pointer, C.size_t) {
  bb := uintToBytes(v)
  return bytesToPtr(bb)
}

func bytesToPtr(bb []byte) (unsafe.Pointer, C.size_t) {
  bsLen := C.size_t(len(bb))
  buf := C.malloc(bsLen)
  copy((*[1<<30]byte)(buf)[:], bb)
  return buf, bsLen
}

func keyToString(ptr unsafe.Pointer, len C.size_t) (int, string, error) {
  if len < 1 {
    return 0, "", errors.New("Invalid key")
  }
  bb := ptrToBytes(ptr, len)
  kt := int(bb[0])
  key := string(bb[1:])
  return kt, key, nil
}

func keyToUint(ptr unsafe.Pointer, len C.size_t) (int, uint64, error) {
  if len < 1 {
    return 0, 0, errors.New("Invalid key")
  }
  bb := ptrToBytes(ptr, len)
  buf := bytes.NewBuffer(bb)

  var ktb byte
  binary.Read(buf, byteOrder, &ktb)
  var key uint64
  binary.Read(buf, byteOrder, &key)

  return int(ktb), key, nil
}

func uintToBytes(v uint64) []byte {
  buf := &bytes.Buffer{}
  binary.Write(buf, byteOrder, v)
  return buf.Bytes()
}

func ptrToBytes(ptr unsafe.Pointer, len C.size_t) []byte {
  bb := make([]byte, int(len))
  copy(bb[:], (*[1<<30]byte)(unsafe.Pointer(ptr))[:])
  return bb
}

func ptrToUint(ptr unsafe.Pointer, len C.size_t) uint64 {
  bb := ptrToBytes(ptr, len)
  buf := bytes.NewBuffer(bb)
  var ret uint64
  binary.Read(buf, byteOrder, &ret)
  return ret
}

func ptrToEntry(ptr unsafe.Pointer, len C.size_t) (uint64, []byte, error) {
  if len < 8 {
    return 0, nil, errors.New("Invalid entry")
  }
  bb := ptrToBytes(ptr, len)
  termBuf := bytes.NewBuffer(bb[:8])
  var rae uint64
  binary.Read(termBuf, byteOrder, &rae)
  if len == 8 {
    return rae, nil, nil
  }
  return rae, bb[8:], nil
}
