package main

import (
  "strconv"
  "strings"
  "encoding/json"
  "io/ioutil"
  "net/http"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

var _ = Describe("Diagnostic API test", func() {
  It("Root Links", func() {
    links := getJSON("/")
    Expect(links["changes"]).ShouldNot(BeEmpty())
    testURI(links["changes"])
    Expect(links["diagnostics"]).ShouldNot(BeEmpty())
    testURI(links["diagnostics"])
  })

  It("Diagnostics Links", func() {
    links := getJSON("/diagnostics")
    Expect(links["id"]).ShouldNot(BeEmpty())
    testURI(links["id"])
    Expect(links["stack"]).ShouldNot(BeEmpty())
    testURI(links["stack"])
    Expect(links["raft"]).ShouldNot(BeEmpty())
    testURI(links["raft"])
  })

  It("Node ID", func() {
    uri := listenURI + "/diagnostics/id"
    idStr := testURI(uri)
    id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
    Expect(err).Should(Succeed())
    Expect(id).ShouldNot(BeZero())
  })

  It("Raft Info", func() {
    raft := getJSON("/diagnostics/raft")
    Expect(raft["state"]).Should(Equal("Leader"))
    id, err := strconv.ParseUint(strings.TrimSpace(raft["leader"]), 10, 64)
    Expect(err).Should(Succeed())
    Expect(id).ShouldNot(BeZero())
  })

  It("Stack", func() {
    uri := listenURI + "/diagnostics/stack"
    stack := testURI(uri)
    Expect(stack).ShouldNot(BeEmpty())
  })
})

func getJSON(path string) map[string]string {
  uri := listenURI + path
  resp, err := http.Get(uri)
  Expect(err).Should(Succeed())
  Expect(resp.StatusCode).Should(Equal(http.StatusOK))
  defer resp.Body.Close()

  Expect(resp.Header.Get("content-type")).Should(Equal("application/json"))
  dec := json.NewDecoder(resp.Body)
  var msg map[string]string
  err = dec.Decode(&msg)
  Expect(err).Should(Succeed())
  return msg
}

func testURI(uri string) string {
  resp, err := http.Get(uri)
  Expect(err).Should(Succeed())
  Expect(resp.StatusCode).Should(Equal(http.StatusOK))
  defer resp.Body.Close()

  buf, err := ioutil.ReadAll(resp.Body)
  Expect(err).Should(Succeed())
  return string(buf)
}