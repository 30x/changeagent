package main

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Hooks API Test", func() {
	var httpClient http.Client = http.Client{}

	It("Set and get hooks", func() {
		uri := listenURI + "/hooks"
		uriObj, err := url.Parse(uri)
		Expect(err).Should(Succeed())

		resp, err := http.Get(uri)
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(200))
		bod, err := ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		resp.Body.Close()
		Expect(string(bod)).Should(MatchJSON("[]"))

		hooks := "[{\"uri\":\"http://localhost:1234\"}]"

		resp, err = http.Post(uri, jsonContent, strings.NewReader(hooks))
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(200))

		resp, err = http.Get(uri)
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(200))
		bod, err = ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		resp.Body.Close()
		Expect(string(bod)).Should(MatchJSON(hooks))

		req := http.Request{
			Method: "DELETE",
			URL:    uriObj,
		}

		resp, err = httpClient.Do(&req)
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(200))

		resp, err = http.Get(uri)
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(200))
		bod, err = ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		resp.Body.Close()
		Expect(string(bod)).Should(MatchJSON("[]"))
	})
})
