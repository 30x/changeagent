/*
 * This file contains functions around writing, parsing, and validating
 * log records.
 *
 * The log record format is designed to be simple and predictable so that
 * we can quickly scan it. So no protobufs here, but simple data types.
 * Everything is byte-ordered little-endian.
 *
 * Log record format:
 *  len           uint32
 *  crc           uint32 (crc-32 of everything AFTER crc!)
 *  sequence      uint64
 *  tenantIdLen   uint16
 *  tenantId      utf8-encoded string
 *  keyLen        uint16
 *  key           utf8-encoded string
 *  dataLen       uint32
 *  data          bytes
 *
 *  ("LSN" is not written -- it is implicit from the file position and
 *   extent sequence number.)
 */

package changelog

import (
  "bytes"
  "errors"
  "encoding/binary"
  "hash/crc32"
)

var recEncoding = binary.LittleEndian

/*
 * Given a log record, return the marshaled form
 */
func marshalLogRecord(rec *LogRecord) []byte {
  // First assemble the interior of the record.
  bi := &bytes.Buffer{}
  binary.Write(bi, recEncoding, rec.sequence)
  writeString(bi, rec.tenantId)
  writeString(bi, rec.key)
  dataLen := uint32(len(rec.data))
  binary.Write(bi, recEncoding, dataLen)
  bi.Write(rec.data)

  // Now compute CRC of interior
  i := bi.Bytes()
  crc := crc32.ChecksumIEEE(i)

  // Now write 8-byte header
  bh := &bytes.Buffer{}
  binary.Write(bh, recEncoding, uint32(len(i) + 8))
  binary.Write(bh, recEncoding, crc)

  // Return assembly of both arrays
  return append(bh.Bytes(), i...)
}

/*
 * Given bytes from the log record NOT INCLUDING the
 * first four length bytes, return a log record, or error if
 * the buffer is the wrong length or the CRC does not match.
 */
func unmarshalLogRecord(buf []byte) (*LogRecord, error) {
  if len(buf) <= 4 {
    return nil, errors.New("Log record too short")
  }
  bb := bytes.NewBuffer(buf)

  rec := &LogRecord{}

  var recCrc uint32
  binary.Read(bb, recEncoding, &recCrc)
  ourCrc := crc32.ChecksumIEEE(buf[4:])
  if ourCrc != recCrc {
    return nil, errors.New("Invalid CRC on returned log record")
  }

  binary.Read(bb, recEncoding, &rec.sequence)
  rec.tenantId = readString(bb)
  rec.key = readString(bb)

  var dataLen uint32
  binary.Read(bb, recEncoding, &dataLen)
  if dataLen > 0 {
    // Copying data. Possible optimization to make a slice here.
    rec.data = make([]byte, dataLen)
    bb.Read(rec.data)
  }

  return rec, nil
}

func writeString(b *bytes.Buffer, s string) {
  sb := []byte(s)
  sl := uint16(len(sb))
  binary.Write(b, binary.LittleEndian, sl)
  b.Write(sb)
}

func readString(b *bytes.Buffer) string {
  var len uint16
  binary.Read(b, recEncoding, &len)
  if len == 0 {
    return ""
  }
  sb := make([]byte, len)
  b.Read(sb)
  return string(sb)
}
