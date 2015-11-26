package changelog

import (
  "testing"
  "bytes"
  "encoding/binary"
  "encoding/hex"
)

func TestEncoding(t *testing.T) {
  td := "Hello, World!"
  tdd := []byte(td)
  r1 := &LogRecord{
    sequence: 1234,
    tenantId: "one",
    key: "a",
    data: tdd,
  }

  enc := marshalLogRecord(r1)
  t.Logf("Encoded to \"%s\" (len %d)", hex.EncodeToString(enc), len(enc))

  rb := bytes.NewBuffer(enc)
  var ll uint32
  binary.Read(rb, binary.LittleEndian, &ll)
  if int(ll) != len(enc) {
    t.Fatalf("Encoded length %d doesn't match written length %d", len(enc), ll)
  }

  rr, err := unmarshalLogRecord(enc[4:])
  if err != nil {
    t.Fatalf("Error on unmarshal: %v", err)
  }

  if rr.sequence != 1234 {
    t.Fatalf("Invalid sequence value %d", rr.sequence)
  }
  if rr.tenantId != "one" {
    t.Fatalf("invalid tenant id %s", rr.tenantId)
  }
  if rr.key != "a" {
    t.Fatalf("invalid key %s", rr.key)
  }
  if !bytes.Equal(rr.data, tdd) {
    t.Fatal("Data bytes not equal")
  }
}

func TestEmptyEncoding(t *testing.T) {
  r1 := &LogRecord{
    sequence: 1234,
    tenantId: "",
    key: "",
    data: nil,
  }

  enc := marshalLogRecord(r1)
  t.Logf("Encoded to \"%s\" (len %d)", hex.EncodeToString(enc), len(enc))

  rb := bytes.NewBuffer(enc)
  var ll uint32
  binary.Read(rb, binary.LittleEndian, &ll)
  if int(ll) != len(enc) {
    t.Fatalf("Encoded length %d doesn't match written length %d", len(enc), ll)
  }

  rr, err := unmarshalLogRecord(enc[4:])
  if err != nil {
    t.Fatalf("Error on unmarshal: %v", err)
  }

  if rr.sequence != 1234 {
    t.Fatalf("Invalid sequence value %d", rr.sequence)
  }
  if rr.tenantId != "" {
    t.Fatalf("invalid tenant id %s", rr.tenantId)
  }
  if rr.key != "" {
    t.Fatalf("invalid key %s", rr.key)
  }
  if rr.data != nil {
    t.Fatal("Data bytes not nil")
  }
}
