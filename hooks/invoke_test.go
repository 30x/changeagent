package hooks

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Hook invocation tests", func() {
	It("Invoke one", func() {
		uri := fmt.Sprintf("http://%s/good", testAddress)
		hooks := []WebHook{
			WebHook{URI: uri},
		}

		err := Invoke(hooks, []byte(goodTestBody), "text/plain")
		Expect(err).Should(Succeed())
	})

	It("Invoke multi", func() {
		uri := fmt.Sprintf("http://%s/good", testAddress)
		hooks := []WebHook{
			WebHook{URI: uri},
			WebHook{URI: uri},
			WebHook{URI: uri},
		}

		err := Invoke(hooks, []byte(goodTestBody), "text/plain")
		Expect(err).Should(Succeed())
	})

	It("Invoke one error", func() {
		uri := fmt.Sprintf("http://%s/good", testAddress)
		hooks := []WebHook{
			WebHook{URI: uri},
		}

		err := Invoke(hooks, []byte("Bad Test Body"), "text/plain")
		Expect(err).Should(MatchError("Error 0: WebHook returned status 400\n"))
	})

	It("Invoke some errors", func() {
		hooks := []WebHook{
			WebHook{URI: fmt.Sprintf("http://%s/bad", testAddress)},
			WebHook{URI: fmt.Sprintf("http://%s/good", testAddress)},
			WebHook{URI: "http://localhost:1111/notthere"},
		}

		err := Invoke(hooks, []byte(goodTestBody), "text/plain")
		Expect(err).Should(
			MatchError(MatchRegexp("Error 0: WebHook returned status 404\nError 1: .+\n")))
	})

	It("Invoke with header", func() {
		uri := fmt.Sprintf("http://%s/header", testAddress)
		hooks := []WebHook{
			WebHook{
				URI: uri,
				Headers: map[string]string{
					"X-Apigee-Testing": "yes",
				},
			},
		}

		err := Invoke(hooks, []byte(goodTestBody), "text/plain")
		Expect(err).Should(Succeed())
	})
})
