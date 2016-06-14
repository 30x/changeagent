package hooks

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	uri1 = "http://localhost:1234"
)

var _ = Describe("Hook Encoding Tests", func() {
	It("Encode to bytes", func() {
		hooks := []WebHook{
			WebHook{URI: uri1},
		}

		buf := EncodeHooks(hooks)

		dec, err := DecodeHooks(buf)
		Expect(err).Should(Succeed())
		Expect(len(dec)).Should(Equal(1))
		Expect(dec[0].URI).Should(Equal(uri1))
	})

	It("Encode to JSON", func() {
		hooks := []WebHook{
			WebHook{URI: uri1},
		}

		buf := EncodeHooksJSON(hooks)

		dec, err := DecodeHooksJSON(buf)
		Expect(err).Should(Succeed())
		Expect(len(dec)).Should(Equal(1))
		Expect(dec[0].URI).Should(Equal(uri1))
	})
})
