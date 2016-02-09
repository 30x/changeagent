package main

import (
  "bytes"
  "errors"
  "regexp"
  "time"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
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

var _ = Describe("JSON encoding tests", func() {
  It("Marshal JSON", func() {
    entry, err := unmarshalJson(bytes.NewReader([]byte(testJson1In)))
    Expect(err).Should(Succeed())

    outBuf := &bytes.Buffer{}
    err = marshalJson(entry, outBuf)
    Expect(err).Should(Succeed())

    match, _ := regexp.Match(testJson1Out, outBuf.Bytes())
    Expect(match).Should(BeTrue())
  })

  It("Marshal with metadata", func() {
    entry, err := unmarshalJson(bytes.NewReader([]byte(testJson2In)))
    Expect(err).Should(Succeed())

    outBuf := &bytes.Buffer{}
    err = marshalJson(entry, outBuf)
    Expect(err).Should(Succeed())

    match, _ := regexp.Match(testJson2Out, outBuf.Bytes())
    Expect(match).Should(BeTrue())
  })

  It("Marshal and add metadata", func() {
    entry, err := unmarshalJson(bytes.NewReader([]byte(testJson1In)))
    Expect(err).Should(Succeed())
    ts := time.Now()

    md := storage.Entry{
      Index: 123,
      Data: entry.Data,
      Timestamp: ts,
    }
    outBuf := &bytes.Buffer{}
    err = marshalJson(&md, outBuf)
    Expect(err).Should(Succeed())

    match, _ := regexp.Match(testJson1Out2, outBuf.Bytes())
    Expect(match).Should(BeTrue())
    entry2, err := unmarshalJson(outBuf)
    Expect(err).Should(Succeed())
    Expect(entry2.Timestamp).Should(Equal(ts))
  })

  It("Marshal string", func() {
    entry, err := unmarshalJson(bytes.NewReader([]byte(testStringIn)))
    Expect(err).Should(Succeed())

    md := storage.Entry{
      Index: 456,
      Data: entry.Data,
    }
    outBuf := &bytes.Buffer{}
    err = marshalJson(&md, outBuf)

    match, _ := regexp.Match(testStringOut, outBuf.Bytes())
    Expect(match).Should(BeTrue())
  })

  It("Marshal invalid data", func() {
    _, err := unmarshalJson(bytes.NewReader([]byte(testInvalidJson)))
    Expect(err).Should(HaveOccurred())
  })

  It("Test marshal list", func() {
    entry1, err := unmarshalJson(bytes.NewReader([]byte(testJson1In)))
    Expect(err).Should(Succeed())
    entry2, err := unmarshalJson(bytes.NewReader([]byte(testStringIn)))
    Expect(err).Should(Succeed())

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
    Expect(err).Should(Succeed())

    outStr :=
    "[" + testJson1Out2 + "," + testStringOut + "]"

    match, _ := regexp.Match(outStr, outBuf.Bytes())
    Expect(match).Should(BeTrue())
  })

  It("Test marshal empty list", func() {
    cl := []storage.Entry{}

    outBuf := &bytes.Buffer{}
    err := marshalChanges(cl, outBuf)
    Expect(err).Should(Succeed())

    outStr := "[]"

    Expect(outBuf.Bytes()).Should(Equal([]byte(outStr)))
  })

  It("Test marshal error", func() {
    e := errors.New("Hello Error!")

    outBuf := &bytes.Buffer{}
    err := marshalError(e, outBuf)
    Expect(err).Should(Succeed())

    Expect(outBuf.Bytes()).Should(BeEquivalentTo(testErrorOut))
  })

  It("Test marshal insert response", func() {
    e := storage.Entry{
      Index: 456,
    }
    outBuf := &bytes.Buffer{}
    err := marshalJson(&e, outBuf)
    Expect(err).Should(Succeed())

    Expect(outBuf.Bytes()).Should(BeEquivalentTo("{\"_id\":456}"))

    re, err := unmarshalJson(bytes.NewBuffer(outBuf.Bytes()))
    Expect(err).Should(Succeed())
    Expect(re.Index).Should(BeEquivalentTo(456))
  })
})

