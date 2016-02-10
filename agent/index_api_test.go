package main

import (
  "fmt"
  "strings"
  "encoding/json"
  "io/ioutil"
  "net/http"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

var _ = Describe("Index API Test", func() {
  BeforeEach(func() {
    waitForLeader()
    getLeaderIndex()

    uri := getLeaderURI() + "/tenants"
    request := "tenant=testTenant"

    pr, err := http.Post(uri, FormContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))
    pr.Body.Close()

    uri = fmt.Sprintf("%s/tenants/testTenant/collections", getLeaderURI())
    request = "collection=testCollection"

    pr, err = http.Post(uri, FormContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))
    pr.Body.Close()
  })

  It("POST new tenant", func() {
    uri := getLeaderURI() + "/tenants"
    request := "tenant=foo"

    pr, err := http.Post(uri, FormContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))
    defer pr.Body.Close()

    respBody, err := ioutil.ReadAll(pr.Body)
    Expect(err).Should(Succeed())
    fmt.Fprintf(GinkgoWriter, "Got response body %s\n", respBody)

    jsonBody := make(map[string]string)
    err = json.Unmarshal(respBody, &jsonBody)
    Expect(err).Should(Succeed())
    fmt.Fprintf(GinkgoWriter, "Got JSON response %v\n", jsonBody)

    Expect(jsonBody["name"]).Should(Equal("foo"))

    gr, err := http.Get(jsonBody["self"])
    Expect(err).Should(Succeed())
    Expect(gr.StatusCode).Should(Equal(200))
    defer gr.Body.Close()

    selfBody, err := ioutil.ReadAll(gr.Body)
    Expect(err).Should(Succeed())
    fmt.Fprintf(GinkgoWriter, "Got \"self\" response body %s\n", selfBody)

    gcr, err := http.Get(jsonBody["collections"])
    Expect(err).Should(Succeed())
    Expect(gcr.StatusCode).Should(Equal(200))
    defer gcr.Body.Close()

    collectionsBody, err := ioutil.ReadAll(gcr.Body)
    Expect(err).Should(Succeed())
    Expect(strings.TrimSpace(string(collectionsBody))).Should(Equal("{}"))
  })

  It("POST new collection", func() {
    uri := fmt.Sprintf("%s/tenants/foo/collections", getLeaderURI())
    request := "collection=bar"

    pr, err := http.Post(uri, FormContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))
    defer pr.Body.Close()

    respBody, err := ioutil.ReadAll(pr.Body)
    Expect(err).Should(Succeed())
    fmt.Fprintf(GinkgoWriter, "Got response body %s\n", respBody)
  })

  It("POST collection entry", func() {
    uri := getLeaderURI() + "/changes"
    request := "{\"tenant\":\"testTenant\",\"collection\":\"testCollection\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":789}}"

    pr, err := http.Post(uri, jsonContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))
    defer pr.Body.Close()

    uri = fmt.Sprintf("%s/tenants/testTenant/collections/testCollection/baz", getLeaderURI())
    gr, err := http.Get(uri)
    Expect(err).Should(Succeed())
    Expect(gr.StatusCode).Should(Equal(200))
    defer gr.Body.Close()

    collBody, err := ioutil.ReadAll(gr.Body)
    Expect(err).Should(Succeed())
    fmt.Fprintf(GinkgoWriter, "Got collection response: %s\n", collBody)
  })

  // TODO test PUT and DELETE
  // Write some easy way to verify that we got back what we sent
})
