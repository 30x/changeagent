package main

import (
  "bytes"
  "errors"
  "regexp"
  "testing"
  "time"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

const (
  testJson1In =
    "{\"one\": \"one\", \"two\": 2, \"three\": 3.0, \"four\": true}"
  testJson1Out =
    "{\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"
  testJson1Out2 =
    "{\"_id\":123,\"_ts\":[0123456789]+,\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"

  testJson2In =
    "{\"tenant\": \"foo\", \"collection\": \"bar\", \"key\": \"baz\", \"data\": {\"one\": \"one\", \"two\": 2, \"three\": 3.0, \"four\": true}}"
  testJson2Out =
    "{\"tenant\":\"foo\",\"collection\":\"bar\",\"key\":\"baz\",\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"

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
  entry, err := unmarshalJson(bytes.NewReader([]byte(testJson1In)))
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Marshaled data into %s", entry)

  outBuf := &bytes.Buffer{}
  err = marshalJson(entry, outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Unmarshaled into %d bytes", outBuf.Len())

  match, _ := regexp.Match(testJson1Out, outBuf.Bytes())
  if !match {
    t.Fatalf("Unmarshaled bytes do not match marshaled:\n%s\n%s",
      testJson1Out, string(outBuf.Bytes()))
  }
}

func TestMarshalFull(t *testing.T) {
  entry, err := unmarshalJson(bytes.NewReader([]byte(testJson2In)))
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Marshaled data into %s", entry)

  outBuf := &bytes.Buffer{}
  err = marshalJson(entry, outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Unmarshaled into %d bytes", outBuf.Len())

  match, _ := regexp.Match(testJson2Out, outBuf.Bytes())
  if !match {
    t.Fatalf("Unmarshaled bytes do not match marshaled:\n%s\n%s",
      testJson2Out, string(outBuf.Bytes()))
  }
}

func TestMarshalMetadata(t *testing.T) {
  entry, err := unmarshalJson(bytes.NewReader([]byte(testJson1In)))
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Marshaled data into %s", entry)

  ts := time.Now()

  md := storage.Entry{
    Index: 123,
    Data: entry.Data,
    Timestamp: ts,
  }
  outBuf := &bytes.Buffer{}
  err = marshalJson(&md, outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Unmarshaled into %d bytes", outBuf.Len())

  match, _ := regexp.Match(testJson1Out2, outBuf.Bytes())
  if !match {
    t.Fatalf("Unmarshaled bytes do not match marshaled:\n%s",
      string(outBuf.Bytes()))
  }

  entry2, err := unmarshalJson(outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }
  if entry2.Timestamp != ts {
    t.Fatalf("Timestamp %s does not match expected %s", entry2.Timestamp, ts)
  }
}

func TestMarshalStringMetadata(t *testing.T) {
  entry, err := unmarshalJson(bytes.NewReader([]byte(testStringIn)))
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Marshaled data into %s", entry)

  md := storage.Entry{
    Index: 456,
    Data: entry.Data,
  }
  outBuf := &bytes.Buffer{}
  err = marshalJson(&md, outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }
  t.Logf("Unmarshaled into %d bytes", outBuf.Len())

  match, _ := regexp.Match(testStringOut, outBuf.Bytes())
  if !match {
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
  entry1, err := unmarshalJson(bytes.NewReader([]byte(testJson1In)))
  if err != nil { t.Fatalf("Error: %v", err) }
  entry2, err := unmarshalJson(bytes.NewReader([]byte(testStringIn)))
  if err != nil { t.Fatalf("Error: %v", err) }

  cl := []storage.Entry{
    {
      Index: 123,
      Data: entry1.Data,
    },
    {
      Index: 456,
      Data: entry2.Data,
    },
  }

  outBuf := &bytes.Buffer{}
  err = marshalChanges(cl, outBuf)
  if err != nil { t.Fatalf("Error: %v", err) }

  outStr :=
    "[" + testJson1Out2 + "," + testStringOut + "]"

  match, _ := regexp.Match(outStr, outBuf.Bytes())
  if !match {
    t.Fatalf("Unmarshaled bytes do not match marshaled:\n%s\n%s",
      outStr, string(outBuf.Bytes()))
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
