package main

import (
  "bytes"
  "errors"
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
    "{\"collection\": \"C344ED17-5E73-4520-A940-0A80251E3B7A\", \"key\": \"baz\", \"data\": {\"one\": \"one\", \"two\": 2, \"three\": 3.0, \"four\": true}}"
  testJson2Out =
    "{\"collection\":\"c344ed17-5e73-4520-a940-0a80251e3b7a\",\"key\":\"baz\",\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"

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

    str, err := marshalJson(entry)
    Expect(err).Should(Succeed())
    Expect(str).Should(MatchRegexp(testJson1Out))
  })

  It("Marshal with metadata", func() {
    entry, err := unmarshalJson(bytes.NewReader([]byte(testJson2In)))
    Expect(err).Should(Succeed())

    str, err := marshalJson(entry)
    Expect(err).Should(Succeed())
    Expect(str).Should(MatchRegexp(testJson2Out))
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

    str, err := marshalJson(&md)
    Expect(err).Should(Succeed())
    Expect(str).Should(MatchRegexp(testJson1Out2))

    entry2, err := unmarshalJson(bytes.NewBuffer([]byte(str)))
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

    str, err := marshalJson(&md)
    Expect(err).Should(Succeed())
    Expect(str).Should(MatchRegexp(testStringOut))
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

    str, err := marshalChanges(cl)
    Expect(err).Should(Succeed())

    outStr :=
      "[" + testJson1Out2 + "," + testStringOut + "]"
    Expect(str).Should(MatchRegexp(outStr))
  })

  It("Test marshal empty list", func() {
    cl := []storage.Entry{}

    str, err := marshalChanges(cl)
    Expect(err).Should(Succeed())

    outStr := []byte("[]")
    Expect(str).Should(BeEquivalentTo(outStr))
  })

  It("Test marshal error", func() {
    e := errors.New("Hello Error!")

    str := marshalError(e)
    Expect(str).Should(Equal(testErrorOut))
  })

  It("Test marshal insert response", func() {
    e := storage.Entry{
      Index: 456,
    }

    str, err := marshalJson(&e)
    Expect(err).Should(Succeed())
    Expect(str).Should(MatchJSON("{\"_id\":456}"))

    re, err := unmarshalJson(bytes.NewBuffer([]byte(str)))
    Expect(err).Should(Succeed())
    Expect(re.Index).Should(BeEquivalentTo(456))
  })
})

