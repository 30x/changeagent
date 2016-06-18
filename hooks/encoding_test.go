package hooks

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	uri1 = "http://localhost:1234"
)

var _ = Describe("Hook Encoding Tests", func() {
	var wh1 = WebHook{
		URI: uri1,
		Headers: map[string]string{
			"foo": "bar",
			"two": "three",
		},
	}

	It("Encode to bytes", func() {
		hooks := []WebHook{wh1}
		buf := EncodeHooks(hooks)

		dec, err := DecodeHooks(buf)
		Expect(err).Should(Succeed())
		Expect(len(dec)).Should(Equal(1))
		Expect(dec[0].URI).Should(Equal(uri1))
		Expect(len(dec[0].Headers)).Should(Equal(2))
		Expect(dec[0].Headers["foo"]).Should(Equal("bar"))
		Expect(dec[0].Headers["two"]).Should(Equal("three"))
	})

	It("Encode to JSON", func() {
		hooks := []WebHook{wh1}

		buf := EncodeHooksJSON(hooks)

		dec, err := DecodeHooksJSON(buf)
		Expect(err).Should(Succeed())
		Expect(len(dec)).Should(Equal(1))
		Expect(dec[0].URI).Should(Equal(uri1))
		Expect(len(dec[0].Headers)).Should(Equal(2))
		Expect(dec[0].Headers["foo"]).Should(Equal("bar"))
		Expect(dec[0].Headers["two"]).Should(Equal("three"))
	})
})
