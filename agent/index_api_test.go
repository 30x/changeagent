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
  var tenant string
  var collection string

  BeforeEach(func() {
    tenant = ensureTenant("testTenant")
    collection = ensureCollection(tenant, "testCollection")
  })

  It("Verify tenant", func() {
    gr, err := http.Get(fmt.Sprintf("%s/tenants/%s", listenURI, tenant))
    Expect(err).Should(Succeed())
    Expect(gr.StatusCode).Should(Equal(200))

    tenantBody := parseJSON(gr)
    Expect(tenantBody["_id"]).Should(Equal(tenant))
    Expect(tenantBody["name"]).Should(Equal("testTenant"))

    gr, err = http.Get(fmt.Sprintf("%s/tenants/%s", listenURI, "testTenant"))
    Expect(err).Should(Succeed())
    Expect(gr.StatusCode).Should(Equal(200))

    tenantBody = parseJSON(gr)
    Expect(tenantBody["_id"]).Should(Equal(tenant))
    Expect(tenantBody["name"]).Should(Equal("testTenant"))
  })

  It("Verify collection", func() {
    gr, err := http.Get(fmt.Sprintf("%s/collections/%s", listenURI, collection))
    Expect(err).Should(Succeed())
    Expect(gr.StatusCode).Should(Equal(200))

    collBody := parseJSON(gr)
    Expect(collBody["_id"]).Should(Equal(collection))
    Expect(collBody["name"]).Should(Equal("testCollection"))

    gr, err = http.Get(fmt.Sprintf("%s/tenants/%s/collections/%s", listenURI, tenant, collection))
    Expect(err).Should(Succeed())
    Expect(gr.StatusCode).Should(Equal(200))

    collBody = parseJSON(gr)
    Expect(collBody["_id"]).Should(Equal(collection))
    Expect(collBody["name"]).Should(Equal("testCollection"))

    gr, err = http.Get(fmt.Sprintf("%s/tenants/%s/collections/%s", listenURI, tenant, "testCollection"))
    Expect(err).Should(Succeed())
    Expect(gr.StatusCode).Should(Equal(200))

    collBody = parseJSON(gr)
    Expect(collBody["_id"]).Should(Equal(collection))
    Expect(collBody["name"]).Should(Equal("testCollection"))
  })

  It("Create tenant", func() {
    uri := listenURI + "/tenants"
    request := "name=foo"

    pr, err := http.Post(uri, FormContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))

    createResponse := parseJSON(pr)

    Expect(createResponse["name"]).Should(Equal("foo"))

    gr, err := http.Get(createResponse["_self"])
    Expect(err).Should(Succeed())
    Expect(gr.StatusCode).Should(Equal(200))

    getResponse := parseJSON(gr)

    Expect(getResponse).Should(Equal(createResponse))

    gcr, err := http.Get(createResponse["_collections"])
    Expect(err).Should(Succeed())
    Expect(gcr.StatusCode).Should(Equal(200))

    defer gcr.Body.Close()
    bytes, err := ioutil.ReadAll(gcr.Body)
    Expect(err).Should(Succeed())
    Expect(string(bytes)).Should(Equal("[]"))
  })

  It("Create collection", func() {
    uri := fmt.Sprintf("%s/tenants/%s/collections", listenURI, tenant)
    request := "name=bar"

    pr, err := http.Post(uri, FormContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))

    createResponse := parseJSON(pr)

    Expect(createResponse["name"]).Should(Equal("bar"))

    gr, err := http.Get(createResponse["_self"])
    Expect(err).Should(Succeed())
    Expect(gr.StatusCode).Should(Equal(200))

    getResponse := parseJSON(gr)

    Expect(getResponse).Should(Equal(createResponse))

    gcr, err := http.Get(createResponse["_keys"])
    Expect(err).Should(Succeed())
    Expect(gcr.StatusCode).Should(Equal(200))

    defer gcr.Body.Close()
    bytes, err := ioutil.ReadAll(gcr.Body)
    Expect(err).Should(Succeed())
    Expect(string(bytes)).Should(Equal("[]"))
  })
})

func parseJSON(resp *http.Response) map[string]string {
  defer resp.Body.Close()
  bytes, err := ioutil.ReadAll(resp.Body)
  Expect(err).Should(Succeed())

  jsonBody := make(map[string]string)
  err = json.Unmarshal(bytes, &jsonBody)
  Expect(err).Should(Succeed())

  fmt.Fprintf(GinkgoWriter, "Got JSON response %v\n", jsonBody)
  return jsonBody
}