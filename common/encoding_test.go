package common

import (
	"bytes"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Encoding Tests", func() {
	It("Encode Entry", func() {
		err := quick.Check(testEntry, nil)
		Expect(err).Should(Succeed())
	})

	It("Entry string", func() {
		tst := time.Now()
		e := &Entry{
			Index:     22,
			Term:      1,
			Timestamp: tst,
			Data:      []byte("Foo!"),
			Tags:      []string{"one", "two"},
		}
		s := e.String()
		Expect(s).Should(ContainSubstring("Index: 22"))
	})

	It("Entry Tags", func() {
		e1 := &Entry{
			Index: 1,
			Term:  1,
		}
		e2 := &Entry{
			Index: 1,
			Term:  1,
			Tags:  []string{"one"},
		}
		e3 := &Entry{
			Index: 1,
			Term:  1,
			Tags:  []string{"one", "two"},
		}

		Expect(e1.MatchesTags([]string{})).Should(BeTrue())
		Expect(e1.MatchesTags([]string{"one"})).Should(BeFalse())
		Expect(e1.MatchesTags([]string{"one", "two"})).Should(BeFalse())
		Expect(e1.MatchesTags([]string{"one", "two", "three"})).Should(BeFalse())

		Expect(e2.MatchesTags([]string{})).Should(BeTrue())
		Expect(e2.MatchesTags([]string{"one"})).Should(BeTrue())
		Expect(e2.MatchesTags([]string{"one", "two"})).Should(BeFalse())
		Expect(e2.MatchesTags([]string{"one", "two", "three"})).Should(BeFalse())

		Expect(e3.MatchesTags([]string{})).Should(BeTrue())
		Expect(e3.MatchesTags([]string{"one"})).Should(BeTrue())
		Expect(e3.MatchesTags([]string{"one", "two"})).Should(BeTrue())
		Expect(e3.MatchesTags([]string{"one", "two", "three"})).Should(BeFalse())
	})
})

func testEntry(term uint64, ts int64, key string, data []byte) bool {
	tst := time.Unix(0, ts)
	e := &Entry{
		Term:      term,
		Timestamp: tst,
		Data:      data,
		Tags:      []string{"one", "two"},
	}

	buf := e.Encode()
	re, err := DecodeEntry(buf)

	Expect(err).Should(Succeed())
	Expect(re.Term).Should(Equal(e.Term))
	Expect(re.Timestamp).Should(Equal(e.Timestamp))
	Expect(re.Tags).Should(Equal(e.Tags))
	Expect(bytes.Equal(re.Data, data)).Should(BeTrue())
	return true
}
