package main

import (
  "bytes"
  "errors"
  "testing"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

const (
  testJson1In =
    "{\"one\": \"one\", \"two\": 2, \"three\": 3.0, \"four\": true}"
  testJson1Out =
    "{\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"
  testJson1Out2 =
    "{\"_id\":123,\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"
  testStringIn =
    "\"Hello, World!\""
  testStringOut =
    "{\"_id\":456,\"data\":\"Hello, World!\"}"
  testInvalidJson =
    "{InvalidKey:123}"
  testErrorOut =
    "{\"error\":\"Hello Error!\"}"
)

func TestMarshal(t *testing.T) {
  buf, err := unmarshalJson(bytes.NewReader([]byte(testJson1In)))
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Marshaled data into %d bytes: \"%s\"", len(buf), string(buf))

  outBuf := &bytes.Buffer{}
  err = marshalJson(buf, nil, outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Unmarshaled into %d bytes", outBuf.Len())

  if !bytes.Equal([]byte(testJson1Out), outBuf.Bytes()) {
    t.Fatalf("Unmarshaled bytes do not match marshaled:\n%s",
      string(outBuf.Bytes()))
  }
}

func TestMarshalMetadata(t *testing.T) {
  buf, err := unmarshalJson(bytes.NewReader([]byte(testJson1In)))
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Marshaled data into %d bytes: \"%s\"", len(buf), string(buf))

  md := &JsonData{
    Id: 123,
  }
  outBuf := &bytes.Buffer{}
  err = marshalJson(buf, md, outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Unmarshaled into %d bytes", outBuf.Len())

  if !bytes.Equal([]byte(testJson1Out2), outBuf.Bytes()) {
    t.Fatalf("Unmarshaled bytes do not match marshaled:\n%s",
      string(outBuf.Bytes()))
  }
}

func TestMarshalStringMetadata(t *testing.T) {
  buf, err := unmarshalJson(bytes.NewReader([]byte(testStringIn)))
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Marshaled data into %d bytes: \"%s\"", len(buf), string(buf))

  md := &JsonData{
    Id: 456,
  }
  outBuf := &bytes.Buffer{}
  err = marshalJson(buf, md, outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Unmarshaled into %d bytes", outBuf.Len())

  if !bytes.Equal([]byte(testStringOut), outBuf.Bytes()) {
    t.Fatalf("Unmarshaled bytes do not match marshaled:\n%s",
      string(outBuf.Bytes()))
  }
}


func TestMarshalInvalid(t *testing.T) {
  _, err := unmarshalJson(bytes.NewReader([]byte(testInvalidJson)))
  if err == nil { t.Fatal("Expected an error on invalid json") }
  t.Logf("Got parsing error %v", err)
}

func TestMarshalList(t *testing.T) {
  buf1, err := unmarshalJson(bytes.NewReader([]byte(testJson1In)))
  if err != nil { t.Fatalf("Error: %v", err) }
  buf2, err := unmarshalJson(bytes.NewReader([]byte(testStringIn)))
  if err != nil { t.Fatalf("Error: %v", err) }

  cl := []storage.Entry{
    {
      Index: 123,
      Data: buf1,
    },
    {
      Index: 456,
      Data: buf2,
    },
  }

  outBuf := &bytes.Buffer{}
  err = marshalChanges(cl, outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }

  outStr :=
    "[" + testJson1Out2 + "," + testStringOut + "]"

  if !bytes.Equal([]byte(outStr), outBuf.Bytes()) {
    t.Fatalf("Unmarshaled bytes do not match marshaled:\n%s",
      string(outBuf.Bytes()))
  }
}

func TestMarshalEmptyList(t *testing.T) {
  cl := []storage.Entry{}

  outBuf := &bytes.Buffer{}
  err := marshalChanges(cl, outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }

  outStr := "[]"

  if !bytes.Equal([]byte(outStr), outBuf.Bytes()) {
    t.Fatalf("Unmarshaled bytes do not match marshaled:\n%s",
      string(outBuf.Bytes()))
  }
}

func TestMarshalError(t *testing.T) {
  e := errors.New("Hello Error!")

  outBuf := &bytes.Buffer{}
  err := marshalError(e, outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }

  if !bytes.Equal([]byte(testErrorOut), outBuf.Bytes()) {
    t.Fatalf("Unmarshaled bytes do not match marshaled:\n%s",
      string(outBuf.Bytes()))
  }
}
