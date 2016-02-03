package storage

/*
#include <stdlib.h>
#include "leveldb_native.h"
*/
import "C"

import (
  "bytes"
  "errors"
  "fmt"
  "math"
  "unsafe"
  "encoding/binary"
)

/* These have to match constants in leveldb_native.h */
const (
  KeyVersion = 1
  MetadataKey = 1
  IndexKey = 2
  EntryKey = 10
  EntryValue = 3
)

var maxKeyLen uint16 =   math.MaxUint16 - 3
var startRange uint16 =  math.MaxUint16 - 2
var endRange uint16 =    math.MaxUint16 - 1
var null []byte = []byte{ 0 }

/*
 * Key format: There are three. The comparator in leveldb_native.c will
 * keep all three of them in order.
 *
 * All keys start with a single byte that is divided into two parts: version and type:
 *   4 bytes: version (currently 1)
 *   4 bytes: type
 *
 * Format 1: Metadata.
 *   Key is a uint64 in "little endian" format. Sorted based on that.
 *   Entry is just raw bytes, often converted to a uint64
 *
 * Format 2: Index Record
 *   See below for key format
 *   Value should exactly match the key to the index entry
 *
 * Format 10: Indexed Entry Record
 *   Key is a uint64 in "little endian". Sorted based on that.
 *   Entry is a protobuf as defined by "EntryPb" in the .proto file for this package,
 *   preceded by a version and type field.
 *   The field contains the tenant, collection, and key records.
 *   (The protobuf lets us save a little bit of space.)
 *
 * The entry records are deliberately kept in format "10" so that we can always
 * scan from the last index in the file efficiently to get the highest index.
 * This used used often by the Raft protocol. That format must always have the
 * highest ID for that reason.
 *
 * Special formatting of index record so that we can efficiently parse it
 * in C code. All ints in "little endian" format:
 *
 * tenant length: uint16
 * collection length: uint16
 * key length: uint16
 * tenant value (string, null-terminated(!)): [tenant length + 6] from start
 * collection value (string, null-terminated): [tenant length + collection length + 7] from start
 * key value (string, null-terminated): [tenant length + collection length + key length + 8] from start
 *
 * In addition:
 *   collection length = 0xfffe denotes "start of tenant" record
 *   collection length = 0xffff denotes "end of tenant" record
 *   key length = 0xfffe denotes "start of collection" record
 *   key length = 0xffff denotes "end of collection" record
 */

// Byte order needs to match the native byte order of the host,
// or the C code to compare keys doesn't work.
var byteOrder binary.ByteOrder = binary.LittleEndian

/*
 * uint64 key, used for metadata and entries.
 */
func uintToKey(keyType int, v uint64) (unsafe.Pointer, C.size_t) {
  buf := &bytes.Buffer{}
  binary.Write(buf, byteOrder, keyPrefix(keyType)[0])
  binary.Write(buf, byteOrder, v)
  return bytesToPtr(buf.Bytes())
}

/*
 * uint64 key, used for metadata and entries.
 */
func keyToUint(ptr unsafe.Pointer, len C.size_t) (int, uint64, error) {
  if len < 1 {
    return 0, 0, errors.New("Invalid key")
  }
  bb := ptrToBytes(ptr, len)
  buf := bytes.NewBuffer(bb)

  var ktb byte
  binary.Read(buf, byteOrder, &ktb)
  vers, kt := parseKeyPrefix(ktb)
  if vers != KeyVersion {
    return 0, 0, fmt.Errorf("Invalid key version %d", vers)
  }
  var key uint64
  binary.Read(buf, byteOrder, &key)

  return kt, key, nil
}

/*
 * Given a tenant, collection, and key, turn them into a composite key.
 */
func indexKeyToPtr(entry *Entry) (unsafe.Pointer, C.size_t, error) {
  tenantBytes := []byte(entry.Tenant)
  tenantLen := uint16(len(tenantBytes))
  if tenantLen > maxKeyLen {
    return nil, 0, fmt.Errorf("Tenant name is longer than maximum of %d", maxKeyLen)
  }
  collBytes := []byte(entry.Collection)
  collLen := uint16(len(collBytes))
  if collLen > maxKeyLen {
    return nil, 0, fmt.Errorf("Collection name is longer than maximum of %d", maxKeyLen)
  }
  keyBytes := []byte(entry.Key)
  keyLen := uint16(len(keyBytes))
  if keyLen > maxKeyLen {
    return nil, 0, fmt.Errorf("Key is longer than maximum of %d", maxKeyLen)
  }

  buf := &bytes.Buffer{}
  buf.Write(keyPrefix(IndexKey))
  binary.Write(buf, byteOrder, tenantLen)
  binary.Write(buf, byteOrder, collLen)
  binary.Write(buf, byteOrder, keyLen)

  if tenantLen > 0 {
    buf.Write(tenantBytes)
    buf.Write(null)
  }
  if collLen > 0 {
    buf.Write(collBytes)
    buf.Write(null)
  }
  if keyLen > 0 {
    buf.Write(keyBytes)
    buf.Write(null)
  }

  ptr, len := bytesToPtr(buf.Bytes())
  return ptr, len, nil
}

/*
 * Given an encoded key, extract the parts of the key.
 */
func ptrToIndexKey(ptr unsafe.Pointer, len C.size_t) (*Entry, error) {
  if len < 6 {
    return nil, errors.New("Invalid entry")
  }

  buf := bytes.NewBuffer(ptrToBytes(ptr, len))
  pb, _ := buf.ReadByte()
  vers, kt := parseKeyPrefix(pb)
  if vers != KeyVersion {
    return nil, fmt.Errorf("Invalid index key version %d", vers)
  }
  if kt != IndexKey {
    return nil, fmt.Errorf("Invalid key type %d", kt)
  }

  var tenantLen uint16
  binary.Read(buf, byteOrder, &tenantLen)
  var collLen uint16
  binary.Read(buf, byteOrder, &collLen)
  var keyLen uint16
  binary.Read(buf, byteOrder, &keyLen)

  e := Entry{}

  if tenantLen > 0 {
    tb := make([]byte, tenantLen)
    buf.Read(tb)
    e.Tenant = string(tb)
    buf.ReadByte()
  }
  if collLen > 0 {
    tb := make([]byte, collLen)
    buf.Read(tb)
    e.Collection = string(tb)
    buf.ReadByte()
  }
  if keyLen > 0 {
    tb := make([]byte, keyLen)
    buf.Read(tb)
    e.Key = string(tb)
    buf.ReadByte()
  }

  return &e, nil
}

/*
 * Given an entire entry, format the data into an entire record.
 */
func entryToPtr(entry *Entry) (unsafe.Pointer, C.size_t) {
  marsh, err := EncodeEntry(entry)
  if err != nil { panic(err.Error()) }
  pfx := keyPrefix(EntryValue)
  bytes := append(pfx, marsh...)

  return bytesToPtr(bytes)
}

/*
 * Given a pointer to an entry record, return the actual Entry
 */
func ptrToEntry(ptr unsafe.Pointer, len C.size_t) (*Entry, error) {
  if len < 0 { return nil, errors.New("Invalid entry: invalid") }
  bytes := ptrToBytes(ptr, len)

  vers, kt := parseKeyPrefix(bytes[0])
  if vers != KeyVersion {
    return nil, fmt.Errorf("Invalid entry version %d", vers)
  }
  if kt != EntryValue {
    return nil, fmt.Errorf("Invalid entry type %d", kt)
  }

  return DecodeEntry(bytes[1:])
}

/*
 * Create a special index key for the start of a collection's index records.
 */
func startCollectionToPtr(tenantName string, collName string) (unsafe.Pointer, C.size_t, error) {
  return collRange(tenantName, collName, false)
}

func endCollectionToPtr(tenantName string, collName string) (unsafe.Pointer, C.size_t, error) {
  return collRange(tenantName, collName, true)
}

func collRange(tenantName string, collName string, isEnd bool) (unsafe.Pointer, C.size_t, error) {
  tenantBytes := []byte(tenantName)
  tenantLen := uint16(len(tenantBytes))
  if tenantLen > maxKeyLen {
    return nil, 0, fmt.Errorf("Tenant name is longer than maximum of %d", maxKeyLen)
  }
  collBytes := []byte(collName)
  collLen := uint16(len(collBytes))
  if collLen > maxKeyLen {
    return nil, 0, fmt.Errorf("Collection name is longer than maximum of %d", maxKeyLen)
  }
  if collLen == 0 {
    return nil, 0, errors.New("Collection name must be non-empty")
  }

  var keyLen uint16
  if isEnd {
    keyLen = endRange
  } else {
    keyLen = startRange
  }

  buf := &bytes.Buffer{}
  buf.Write(keyPrefix(IndexKey))
  binary.Write(buf, byteOrder, tenantLen)
  binary.Write(buf, byteOrder, collLen)
  binary.Write(buf, byteOrder, keyLen)

  if tenantLen > 0 {
    buf.Write(tenantBytes)
    buf.Write(null)
  }
  if collLen > 0 {
    buf.Write(collBytes)
    buf.Write(null)
  }

  ptr, len := bytesToPtr(buf.Bytes())
  return ptr, len, nil
}

/*
 * Create a special index key for the start of a tenant's index records.
 */
func startTenantToPtr(tenantName string) (unsafe.Pointer, C.size_t, error) {
  return tenantRange(tenantName, false)
}

func endTenantToPtr(tenantName string) (unsafe.Pointer, C.size_t, error) {
  return tenantRange(tenantName, true)
}

func tenantRange(tenantName string, isEnd bool) (unsafe.Pointer, C.size_t, error) {
  tenantBytes := []byte(tenantName)
  tenantLen := uint16(len(tenantBytes))
  if tenantLen > maxKeyLen {
    return nil, 0, fmt.Errorf("Tenant name is longer than maximum of %d", maxKeyLen)
  }

  var collLen uint16
  if isEnd {
    collLen = endRange
  } else {
    collLen = startRange
  }

  var keyLen uint16 = 0

  buf := &bytes.Buffer{}
  buf.Write(keyPrefix(IndexKey))
  binary.Write(buf, byteOrder, tenantLen)
  binary.Write(buf, byteOrder, collLen)
  binary.Write(buf, byteOrder, keyLen)

  if tenantLen > 0 {
    buf.Write(tenantBytes)
    buf.Write(null)
  }

  ptr, len := bytesToPtr(buf.Bytes())
  return ptr, len, nil
}

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

func keyPrefix(keyType int) []byte {
  flag := (KeyVersion << 4) | (keyType & 0xf)
  return []byte{ byte(flag) }
}

func parseKeyPrefix(b byte) (int, int) {
  bi := int(b)
  vers := (bi >> 4) & 0xf
  kt := bi & 0xf
  return vers, kt
}

func writeString(buf *bytes.Buffer, s string) {
  sb := []byte(s)
  var sl uint32 = uint32(len(sb))
  binary.Write(buf, byteOrder, sl)
  buf.Write(sb)
}

func readString(buf *bytes.Buffer) string {
  var len uint32
  binary.Read(buf, byteOrder, &len)
  if len <= 0 {
    return ""
  }
  bb := make([]byte, len)
  buf.Read(bb)
  return string(bb)
}

func testKeyComparison(ptra unsafe.Pointer, lena C.size_t, ptrb unsafe.Pointer, lenb C.size_t) int {
  cmp := C.go_compare_bytes(nil, ptra, lena, ptrb, lenb);
  return int(cmp)
}
