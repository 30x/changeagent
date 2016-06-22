package main

import (
	"bytes"
	"errors"
	"time"

	"github.com/30x/changeagent/storage"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	testJSON1In   = "{\"one\": \"one\", \"two\": 2, \"three\": 3.0, \"four\": true}"
	testJSON1Out  = "{\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"
	testJSON1Out2 = "{\"_id\":123,\"_ts\":[0123456789]+,\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"

	testJSON2In  = "{\"data\": {\"one\": \"one\", \"two\": 2, \"three\": 3.0, \"four\": true}}"
	testJSON2Out = "{\"data\":{\"one\":\"one\",\"two\":2,\"three\":3.0,\"four\":true}}"

	testJSON3aIn  = "{\"data\": {\"count\": 1}, \"tags\": []}"
	testJSON3aOut = "{\"data\":{\"count\":1}}"
	testJSON3bIn  = "{\"data\": {\"count\": 1}, \"tags\": [\"one\"]}"
	testJSON3bOut = "{\"data\":{\"count\":1},\"tags\":[\"one\"]}"
	testJSON3cIn  = "{\"data\": {\"count\": 1}, \"tags\": [\"one\", \"two\"]}"
	testJSON3cOut = "{\"data\":{\"count\":1},\"tags\":[\"one\",\"two\"]}"

	testStringIn    = "\"Hello, World!\""
	testStringOut   = "{\"_id\":456,\"data\":\"Hello, World!\"}"
	testInvalidJSON = "{InvalidKey:123}"

	testErrorOut = "{\"error\":\"Hello Error!\"}"
)

var _ = Describe("JSON encoding tests", func() {
	It("Marshal JSON", func() {
		entry, err := unmarshalJSON(bytes.NewReader([]byte(testJSON1In)))
		Expect(err).Should(Succeed())

		str, err := marshalJSONToString(entry)
		Expect(err).Should(Succeed())
		Expect(str).Should(MatchRegexp(testJSON1Out))
	})

	It("Marshal with metadata", func() {
		entry, err := unmarshalJSON(bytes.NewReader([]byte(testJSON2In)))
		Expect(err).Should(Succeed())

		str, err := marshalJSONToString(entry)
		Expect(err).Should(Succeed())
		Expect(str).Should(MatchRegexp(testJSON2Out))
	})

	It("Marshal and add metadata", func() {
		entry, err := unmarshalJSON(bytes.NewReader([]byte(testJSON1In)))
		Expect(err).Should(Succeed())
		ts := time.Now()

		md := storage.Entry{
			Index:     123,
			Data:      entry.Data,
			Timestamp: ts,
		}

		str, err := marshalJSONToString(md)
		Expect(err).Should(Succeed())
		Expect(str).Should(MatchRegexp(testJSON1Out2))

		entry2, err := unmarshalJSON(bytes.NewBuffer([]byte(str)))
		Expect(err).Should(Succeed())
		Expect(entry2.Timestamp).Should(Equal(ts))
	})

	It("Marshal With Tags", func() {
		entry, err := unmarshalJSON(bytes.NewReader([]byte(testJSON3aIn)))
		Expect(err).Should(Succeed())

		str, err := marshalJSONToString(entry)
		Expect(err).Should(Succeed())
		Expect(str).Should(MatchJSON(testJSON3aOut))

		entry, err = unmarshalJSON(bytes.NewReader([]byte(testJSON3bIn)))
		Expect(err).Should(Succeed())

		str, err = marshalJSONToString(entry)
		Expect(err).Should(Succeed())
		Expect(str).Should(MatchJSON(testJSON3bOut))

		entry, err = unmarshalJSON(bytes.NewReader([]byte(testJSON3cIn)))
		Expect(err).Should(Succeed())

		str, err = marshalJSONToString(entry)
		Expect(err).Should(Succeed())
		Expect(str).Should(MatchJSON(testJSON3cOut))
	})

	It("Marshal string", func() {
		entry, err := unmarshalJSON(bytes.NewReader([]byte(testStringIn)))
		Expect(err).Should(Succeed())

		md := storage.Entry{
			Index: 456,
			Data:  entry.Data,
		}

		str, err := marshalJSONToString(md)
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
				Data:  entry1.Data,
			},
			{
				Index: 456,
				Data:  entry2.Data,
			},
		}

		out := &bytes.Buffer{}
		err = marshalChanges(cl, false, false, out)
		Expect(err).Should(Succeed())

		outStr :=
			"[" + testJSON1Out2 + "," + testStringOut + "]"
		Expect(out.String()).Should(MatchRegexp(outStr))
	})

	It("Test marshal empty list", func() {
		cl := []storage.Entry{}

		out := &bytes.Buffer{}
		err := marshalChanges(cl, false, false, out)
		Expect(err).Should(Succeed())

		outStr := "^{\"changes\":\\[\\]"
		Expect(out.String()).Should(MatchRegexp(outStr))
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

		str, err := marshalJSONToString(e)
		Expect(err).Should(Succeed())
		Expect(str).Should(MatchJSON("{\"_id\":456}"))

		re, err := unmarshalJSON(bytes.NewBuffer([]byte(str)))
		Expect(err).Should(Succeed())
		Expect(re.Index).Should(BeEquivalentTo(456))
	})
})

func marshalJSONToString(entry storage.Entry) (string, error) {
	out := &bytes.Buffer{}
	err := marshalJSON(entry, out)
	if err != nil {
		return "", err
	}
	return out.String(), nil
}
