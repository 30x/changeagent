package storage

/*
#include <stdlib.h>
#include "rocksdb_native.h"
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	"github.com/30x/changeagent/common"
)

/* These have to match constants in leveldb_native.h */
const (
	KeyVersion  = 1
	MetadataKey = 1
	EntryKey    = 10
	EntryValue  = 3
)

/*
 * Key format: There are three. The comparator in leveldb_native.c will
 * keep all three of them in order.
 *
 * All keys start with a single byte that is divided into two parts: version and type:
 *   4 bytes: version (currently 1)
 *   4 bytes: type
 *
 * Format 1: Metadata.
 *   Key is a string, sorted as per "strcmp".
 *   Entry is just raw bytes, often converted to a uint64
 *
 * Format 10: Indexed Entry Record
 *   Key is a uint64 in "little endian". Sorted based on that in numeric order.
 *   Entry is a protobuf as defined by "EntryPb" in the .proto file for this package,
 *   preceded by a version and type field.
 *   (The protobuf lets us save a little bit of space.)
 *
 * The entry records are deliberately kept in format "10" so that we can always
 * scan from the last index in the file efficiently to get the highest index.
 * This used used often by the Raft protocol. That format must always have the
 * highest ID for that reason. (However, this is less important now that we have column families.)
 */

// Byte order needs to match the native byte order of the host,
// or the C code to compare keys doesn't work.
var storageByteOrder binary.ByteOrder = binary.LittleEndian

/*
 * uint64 key, used for entries.
 */
func uintToKey(keyType int, v uint64) (unsafe.Pointer, C.size_t) {
	buf := &bytes.Buffer{}
	binary.Write(buf, storageByteOrder, keyPrefix(keyType)[0])
	binary.Write(buf, storageByteOrder, v)
	return bytesToPtr(buf.Bytes())
}

/*
 * uint64 key, used for entries.
 */
func keyToUint(ptr unsafe.Pointer, len C.size_t) (int, uint64, error) {
	if len < 1 {
		return 0, 0, errors.New("Invalid key")
	}
	bb := ptrToBytes(ptr, len)
	buf := bytes.NewBuffer(bb)

	var ktb byte
	binary.Read(buf, storageByteOrder, &ktb)
	vers, kt := parseKeyPrefix(ktb)
	if vers != KeyVersion {
		return 0, 0, fmt.Errorf("Invalid key version %d", vers)
	}
	var key uint64
	binary.Read(buf, storageByteOrder, &key)

	return kt, key, nil
}

/*
 * String key, used for metadata.
 */
func stringToKey(keyType int, k string) (unsafe.Pointer, C.size_t) {
	buf := &bytes.Buffer{}
	binary.Write(buf, storageByteOrder, keyPrefix(keyType)[0])
	buf.WriteString(k)
	return bytesToPtr(buf.Bytes())
}

func keyToString(ptr unsafe.Pointer, len C.size_t) (int, string, error) {
	if len < 1 {
		return 0, "", errors.New("Invalid key")
	}
	bb := ptrToBytes(ptr, len)
	buf := bytes.NewBuffer(bb)

	var ktb byte
	binary.Read(buf, storageByteOrder, &ktb)
	vers, kt := parseKeyPrefix(ktb)
	if vers != KeyVersion {
		return 0, "", fmt.Errorf("Invalid key version %d", vers)
	}
	k := buf.String()
	return kt, k, nil
}

/*
 * Given an entire entry, format the data into an entire record.
 */
func entryToPtr(entry *common.Entry) (unsafe.Pointer, C.size_t) {
	marsh := entry.Encode()
	pfx := keyPrefix(EntryValue)
	bytes := append(pfx, marsh...)
	return bytesToPtr(bytes)
}

/*
 * Given a pointer to an entry record, return the actual Entry
 */
func ptrToEntry(ptr unsafe.Pointer, len C.size_t) (*common.Entry, error) {
	if len < 0 {
		return nil, errors.New("Invalid entry: invalid")
	}
	bytes := ptrToBytes(ptr, len)

	vers, kt := parseKeyPrefix(bytes[0])
	if vers != KeyVersion {
		return nil, fmt.Errorf("Invalid entry version %d", vers)
	}
	if kt != EntryValue {
		return nil, fmt.Errorf("Invalid entry type %d", kt)
	}

	return common.DecodeEntry(bytes[1:])
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

func uintToPtr(v uint64) (unsafe.Pointer, C.size_t) {
	bb := uintToBytes(v)
	return bytesToPtr(bb)
}

func bytesToPtr(bb []byte) (unsafe.Pointer, C.size_t) {
	bsLen := C.size_t(len(bb))
	buf := C.malloc(bsLen)
	copy((*[1 << 30]byte)(buf)[:], bb)
	return buf, bsLen
}

func uintToBytes(v uint64) []byte {
	buf := &bytes.Buffer{}
	binary.Write(buf, storageByteOrder, v)
	return buf.Bytes()
}

func ptrToBytes(ptr unsafe.Pointer, len C.size_t) []byte {
	bb := make([]byte, int(len))
	copy(bb[:], (*[1 << 30]byte)(unsafe.Pointer(ptr))[:])
	return bb
}

func ptrToUint(ptr unsafe.Pointer, len C.size_t) uint64 {
	bb := ptrToBytes(ptr, len)
	buf := bytes.NewBuffer(bb)
	var ret uint64
	binary.Read(buf, storageByteOrder, &ret)
	return ret
}

func keyPrefix(keyType int) []byte {
	flag := (KeyVersion << 4) | (keyType & 0xf)
	return []byte{byte(flag)}
}

func parseKeyPrefix(b byte) (int, int) {
	bi := int(b)
	vers := (bi >> 4) & 0xf
	kt := bi & 0xf
	return vers, kt
}

func compareKeys(ptra unsafe.Pointer, lena C.size_t, ptrb unsafe.Pointer, lenb C.size_t) int {
	cmp := C.go_compare_bytes(nil, ptra, lena, ptrb, lenb)
	return int(cmp)
}
