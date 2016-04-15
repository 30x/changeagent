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
  testJSON1In =
    "{\"one\": \"one\", \"two\": 2, \"three\": 3.0, \"four\": true}"
  testJSON1Out =
    "{\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"
  testJSON1Out2 =
    "{\"_id\":123,\"_ts\":[0123456789]+,\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"

  testJSON2In =
    "{\"tenant\": \"99442130-fa86-40f4-928a-911faa86cafa\", \"collection\": \"C344ED17-5E73-4520-A940-0A80251E3B7A\", \"key\": \"baz\", \"data\": {\"one\": \"one\", \"two\": 2, \"three\": 3.0, \"four\": true}}"
  testJSON2Out =
    "{\"tenant\":\"99442130-fa86-40f4-928a-911faa86cafa\",\"collection\":\"c344ed17-5e73-4520-a940-0a80251e3b7a\",\"key\":\"baz\",\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"

  testStringIn =
    "\"Hello, World!\""
  testStringOut =
    "{\"_id\":456,\"data\":\"Hello, World!\"}"
  testInvalidJSON =
    "{InvalidKey:123}"

  testErrorOut =
    "{\"error\":\"Hello Error!\"}"
)

var _ = Describe("JSON encoding tests", func() {
  It("Marshal JSON", func() {
    entry, err := unmarshalJSON(bytes.NewReader([]byte(testJSON1In)))
    Expect(err).Should(Succeed())

    str, err := marshalJSON(entry)
    Expect(err).Should(Succeed())
    Expect(str).Should(MatchRegexp(testJSON1Out))
  })

  It("Marshal with metadata", func() {
    entry, err := unmarshalJSON(bytes.NewReader([]byte(testJSON2In)))
    Expect(err).Should(Succeed())

    str, err := marshalJSON(entry)
    Expect(err).Should(Succeed())
    Expect(str).Should(MatchRegexp(testJSON2Out))
  })

  It("Marshal and add metadata", func() {
    entry, err := unmarshalJSON(bytes.NewReader([]byte(testJSON1In)))
    Expect(err).Should(Succeed())
    ts := time.Now()

    md := storage.Entry{
      Index: 123,
      Data: entry.Data,
      Timestamp: ts,
    }

    str, err := marshalJSON(md)
    Expect(err).Should(Succeed())
    Expect(str).Should(MatchRegexp(testJSON1Out2))

    entry2, err := unmarshalJSON(bytes.NewBuffer([]byte(str)))
    Expect(err).Should(Succeed())
    Expect(entry2.Timestamp).Should(Equal(ts))
  })

  It("Marshal string", func() {
    entry, err := unmarshalJSON(bytes.NewReader([]byte(testStringIn)))
    Expect(err).Should(Succeed())

    md := storage.Entry{
      Index: 456,
      Data: entry.Data,
    }

    str, err := marshalJSON(md)
    Expect(err).Should(Succeed())
    Expect(str).Should(MatchRegexp(testStringOut))
  })

  It("Marshal invalid data", func() {
    _, err := unmarshalJSON(bytes.NewReader([]byte(testInvalidJSON)))
    Expect(err).Should(HaveOccurred())
  })

  It("Test marshal list", func() {
    entry1, err := unmarshalJSON(bytes.NewReader([]byte(testJSON1In)))
    Expect(err).Should(Succeed())
    entry2, err := unmarshalJSON(bytes.NewReader([]byte(testStringIn)))
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
      "[" + testJSON1Out2 + "," + testStringOut + "]"
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

    str, err := marshalJSON(e)
    Expect(err).Should(Succeed())
    Expect(str).Should(MatchJSON("{\"_id\":456}"))

    re, err := unmarshalJSON(bytes.NewBuffer([]byte(str)))
    Expect(err).Should(Succeed())
    Expect(re.Index).Should(BeEquivalentTo(456))
  })
})

