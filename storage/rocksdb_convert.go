package storage

/*
#include <stdlib.h>
#include "rocksdb_native.h"
*/
import "C"

import (
  "bytes"
  "errors"
  "fmt"
  "math"
  "unsafe"
  "encoding/binary"
  "github.com/satori/go.uuid"
)

/* These have to match constants in leveldb_native.h */
const (
  KeyVersion = 1
  MetadataKey = 1
  IndexKey = 2
  TenantIndexKey = 3
  EntryKey = 10
  EntryValue = 3
  UuidValue = 4
  StringValue = 5
)

var maxKeyLen uint16 =   math.MaxUint16 - 2
var startRange uint16 =  math.MaxUint16 - 1
var endRange uint16 =    math.MaxUint16
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
 * Format 3: Tenant Index key
 *   This key format is used for the tenant-specific change table. It consists of the
 * tenant ID as an eight-byte UUID, followed by the index as a uint64.
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
 * collection ID: eight bytes (UUID)
 * key length: uint16
 * key value (string, null-terminated): [1 + 16 + 2] from start
 *
 * In addition:
 *   collection length = 0xfffe denotes "start of collection" record
 *   collection length = 0xffff denotes "end of collection" record
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
func indexKeyToPtr(id *uuid.UUID, key string) (unsafe.Pointer, C.size_t, error) {
  keyBytes := []byte(key)
  keyLen := uint16(len(keyBytes))
  if keyLen > maxKeyLen {
    return nil, 0, fmt.Errorf("Key is longer than maximum of %d", maxKeyLen)
  }

  buf := &bytes.Buffer{}
  buf.Write(keyPrefix(IndexKey))
  buf.Write(id.Bytes())
  binary.Write(buf, byteOrder, keyLen)

  if keyLen > 0 {
    buf.Write(keyBytes)
  }
  buf.Write(null)

  ptr, len := bytesToPtr(buf.Bytes())
  return ptr, len, nil
}

/*
 * Given an encoded key, extract the parts of the key. The result will be an Entry. If the
 * key is a start or end of range key, then skip those parts.
 * Returns: collection ID, key type, and key (or "")
 * keyType is [startRange | endRange | 0]
 */
func ptrToIndexKey(ptr unsafe.Pointer, len C.size_t) (*uuid.UUID, uint16, string, error) {
  if len < 19 {
    return nil, 0, "", errors.New("Invalid entry")
  }

  buf := bytes.NewBuffer(ptrToBytes(ptr, len))
  pb, _ := buf.ReadByte()
  vers, kt := parseKeyPrefix(pb)
  if vers != KeyVersion {
    return nil, 0, "", fmt.Errorf("Invalid index key version %d", vers)
  }
  if kt != IndexKey {
    return nil, 0, "", fmt.Errorf("Invalid key type %d", kt)
  }

  ub := make([]byte, 16)
  _, err := buf.Read(ub)
  if err != nil { return nil, 0, "", fmt.Errorf("Error reading: %s", err) }
  id, err := uuid.FromBytes(ub)
  if err != nil { return nil, 0, "", fmt.Errorf("Invalid UUID: %s", err) }

  var keyLen uint16
  binary.Read(buf, byteOrder, &keyLen)

  if keyLen < 0 || keyLen > math.MaxUint16 {
    return nil, 0, "", errors.New("Invalid key length")
  }
  if keyLen == startRange {
    return &id, startRange, "", nil
  }
  if keyLen == endRange {
    return &id, endRange, "", nil
  }

  var key string
  if keyLen > 0 {
    tb := make([]byte, keyLen)
    _, err = buf.Read(tb)
    if err != nil { return nil, 0, "", fmt.Errorf("Error reading: %s", err) }
    key = string(tb)
  } else {
    key = ""
  }
  buf.ReadByte()

  return &id, 0, key, nil
}

/*
 * Tenant ID and index key, used for the tenant-specific change table.
 */
func tenantIndexToPtr(id *uuid.UUID, ix uint64) (unsafe.Pointer, C.size_t) {
  buf := &bytes.Buffer{}
  buf.Write(keyPrefix(TenantIndexKey))
  buf.Write(id.Bytes())
  binary.Write(buf, byteOrder, ix)

  return bytesToPtr(buf.Bytes())
}

func ptrToTenantIndex(ptr unsafe.Pointer, len C.size_t) (*uuid.UUID, uint64, error) {
  if len < 25 {
    return nil, 0, errors.New("Invalid index entry")
  }

  buf := bytes.NewBuffer(ptrToBytes(ptr, len))
  pb, _ := buf.ReadByte()
  vers, kt := parseKeyPrefix(pb)
  if vers != KeyVersion {
    return nil, 0, fmt.Errorf("Invalid index key version %d", vers)
  }
  if kt != TenantIndexKey {
    return nil, 0, fmt.Errorf("Invalid key type %d", kt)
  }

  ub := make([]byte, 16)
  _, err := buf.Read(ub)
  if err != nil { return nil, 0, fmt.Errorf("Error reading: %s", err) }
  id, err := uuid.FromBytes(ub)
  if err != nil { return nil, 0, fmt.Errorf("Invalid UUID: %s", err) }

  var ix uint64
  binary.Read(buf, byteOrder, &ix)

  return &id, ix, nil
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
func startIndexToPtr(id *uuid.UUID) (unsafe.Pointer, C.size_t, error) {
  return indexRange(id, false)
}

func endIndexToPtr(id *uuid.UUID) (unsafe.Pointer, C.size_t, error) {
  return indexRange(id, true)
}

func indexRange(id *uuid.UUID, isEnd bool) (unsafe.Pointer, C.size_t, error) {
  buf := &bytes.Buffer{}
  buf.Write(keyPrefix(IndexKey))
  buf.Write(id.Bytes())
  if isEnd {
    binary.Write(buf, byteOrder, endRange)
  } else {
    binary.Write(buf, byteOrder, startRange)
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

func uuidToPtr(id *uuid.UUID) (unsafe.Pointer, C.size_t) {
  buf := &bytes.Buffer{}
  buf.Write(keyPrefix(UuidValue))
  buf.Write(id.Bytes())

  ptr, len := bytesToPtr(buf.Bytes())
  return ptr, len
}

func ptrToUuid(ptr unsafe.Pointer, len C.size_t) (*uuid.UUID, error) {
  if len < 17 {
    return nil, errors.New("Invalid entry")
  }

  buf := bytes.NewBuffer(ptrToBytes(ptr, len))
  pb, _ := buf.ReadByte()
  vers, kt := parseKeyPrefix(pb)
  if vers != KeyVersion {
    return nil, fmt.Errorf("Invalid index key version %d", vers)
  }
  if kt != UuidValue {
    return nil, fmt.Errorf("Invalid key type %d", kt)
  }

  ub := make([]byte, 16)
  _, err := buf.Read(ub)
  if err != nil { return nil, fmt.Errorf("Error reading: %s", err) }
  id, err := uuid.FromBytes(ub)
  if err != nil { return nil, fmt.Errorf("Invalid UUID: %s", err) }

  return &id, nil
}

func stringToPtr(s string) (unsafe.Pointer, C.size_t) {
  var sl uint16 = uint16(len(s))
  buf := &bytes.Buffer{}
  buf.Write(keyPrefix(StringValue))
  binary.Write(buf, byteOrder, &sl)
  buf.Write([]byte(s))

  ptr, len := bytesToPtr(buf.Bytes())
  return ptr, len
}

func ptrToString(ptr unsafe.Pointer, len C.size_t) (string, error) {
  if len < 3 {
    return "", errors.New("Invalid entry")
  }

  buf := bytes.NewBuffer(ptrToBytes(ptr, len))
  pb, _ := buf.ReadByte()
  vers, kt := parseKeyPrefix(pb)
  if vers != KeyVersion {
    return "", fmt.Errorf("Invalid index key version %d", vers)
  }
  if kt != StringValue {
    return "", fmt.Errorf("Invalid key type %d", kt)
  }

  var sl uint16
  binary.Read(buf, byteOrder, &sl)
  sb := make([]byte, sl)
  buf.Read(sb)
  return string(sb), nil
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

func compareKeys(ptra unsafe.Pointer, lena C.size_t, ptrb unsafe.Pointer, lenb C.size_t) int {
  cmp := C.go_compare_bytes(nil, ptra, lena, ptrb, lenb);
  return int(cmp)
}
