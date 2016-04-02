package main

import (
  "fmt"
  "strings"
  "time"
  "encoding/json"
  "net/http"
  "io/ioutil"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

const (
  jsonContent = "application/json"
)

var lastNewChange uint64
var numPosts int

var _ = Describe("Changes API Test", func() {
  var tenant string
  var collection string

  BeforeEach(func() {
    tenant = ensureTenant("indexTest")
    collection = ensureCollection(tenant, "indexTestCollection")
  })

  It("POST new change", func() {
    request := "{\"hello\": \"world!\", \"foo\": 123}"
    resp := postChange(request)

    entry, err := unmarshalJson(strings.NewReader(resp))
    Expect(err).Should(Succeed())
    expected := fmt.Sprintf("{\"_id\":%d}", entry.Index)
    Expect(resp).Should(MatchJSON(expected))

    lastNewChange = entry.Index

    // Upon return, change should immediately be represented at the leader
    respExpected :=
      fmt.Sprintf("[{\"_id\":%d,\"_ts\":[0-9]+,\"data\":{\"hello\":\"world!\",\"foo\":123}}]", lastNewChange)
    peerChanges := getChanges(lastNewChange - 1, 100, 0)
    Expect(peerChanges).Should(MatchRegexp(respExpected))

    peerChanges = getChanges(lastNewChange - 1, 100, 10)
    Expect(peerChanges).Should(MatchRegexp(respExpected))
    fmt.Fprintf(GinkgoWriter, "Get changes peer \"%s\"\n", peerChanges)
    Expect(err).Should(Succeed())

    url := fmt.Sprintf("%s/changes/%d", listenUri, lastNewChange)
    gr, err := http.Get(url)
    Expect(err).Should(Succeed())
    defer gr.Body.Close()
    Expect(gr.StatusCode).Should(Equal(200))

    change, err := unmarshalJson(gr.Body)
    Expect(err).Should(Succeed())
    Expect(gr.StatusCode).Should(Equal(200))
    Expect(change.Index).Should(Equal(lastNewChange))
  })

  It("POST indexed record", func() {
    request :=
      fmt.Sprintf("{\"tenant\":\"%s\",\"collection\":\"%s\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":456}}",
        tenant, collection)
    resp := postChange(request)

    lastNewChange++
    expected := fmt.Sprintf("{\"_id\":%d}", lastNewChange)
    Expect(resp).Should(MatchJSON(expected))

    respExpected :=
      fmt.Sprintf("[{\"_id\":%d,\"_ts\":[0-9]+,\"tenant\":\"%s\",\"collection\":\"%s\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":456}}]",
        lastNewChange, tenant, collection)
    peerChanges := getChanges(lastNewChange - 1, 100, 0)

    Expect(peerChanges).Should(MatchRegexp(respExpected))

    tenantChanges := getTenantChanges(tenant, lastNewChange - 1, 100, 0)
    Expect(tenantChanges).Should(MatchRegexp(respExpected))
  })

  It("POST indexed record 2", func() {
    request := "{\"key\":\"fooey\",\"data\":{\"hello\":\"world!\",\"foo\":888}}"

    pr, err := http.Post(
      fmt.Sprintf("%s/collections/%s/keys", listenUri, collection),
      jsonContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))

    body, err := ioutil.ReadAll(pr.Body)
    Expect(err).Should(Succeed())
    resp := string(body)

    lastNewChange++
    expected := fmt.Sprintf("{\"_id\":%d}", lastNewChange)
    Expect(resp).Should(MatchJSON(expected))

    respExpected :=
      fmt.Sprintf("[{\"_id\":%d,\"_ts\":[0-9]+,\"collection\":\"%s\",\"key\":\"fooey\",\"data\":{\"hello\":\"world!\",\"foo\":888}}]",
        lastNewChange, collection)
    peerChanges := getChanges(lastNewChange - 1, 100, 0)

    Expect(peerChanges).Should(MatchRegexp(respExpected))
  })

  It("POST empty change", func() {
    peerChanges := getChanges(lastNewChange, 100, 0)
    Expect(strings.TrimSpace(string(peerChanges))).Should(Equal("[]"))
  })

  It("Post and retrieve multiple", func() {
    changes := postChanges(collection, 2)

    templ :=
      "[{\"_id\":%d,\"_ts\":[0-9]+,\"collection\":\"%s\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"count\":1}}," +
      "{\"_id\":%d,\"_ts\":[0-9]+,\"collection\":\"%s\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"count\":2}}]"
    respExpected := fmt.Sprintf(templ, lastNewChange - 1, collection, lastNewChange, collection)
    peerChanges := getChanges(changes[0] - 1, 100, 0)

    Expect(peerChanges).Should(MatchRegexp(respExpected))
  })

  It("Retrieve all", func() {
    changes := postChanges(collection, 3)

    respBody := getChanges(0, 100, 0)
    var results []JsonData
    err := json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(BeNumerically(">=", 3))

    // Test various permutations of offset and limit now.
    respBody = getChanges(changes[0] - 1, 1, 0)
    err = json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(1))
    Expect(results[0].Id).Should(Equal(changes[0]))

    respBody = getChanges(changes[0] - 1, 2, 0)
    err = json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(2))
    Expect(results[0].Id).Should(Equal(changes[0]))
    Expect(results[1].Id).Should(Equal(changes[1]))
  })

  It("Blocking retrieval", func() {
    respBody := getChanges(lastNewChange, 100, 0)
    var results []JsonData
    err := json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(0))

    ch := make(chan uint64, 1)

    go func() {
      newResp := getChanges(lastNewChange, 100, 5)
      var newResults []JsonData
      json.Unmarshal(newResp, &newResults)
      if len(newResults) == 1 {
        ch <- newResults[0].Id
      } else {
        ch <- 0
      }
    }()

    request := "{\"hello\": \"world!\", \"foo\": 9999}"
    time.Sleep(500 * time.Millisecond)
    resp := postChange(request)

    var postResult JsonData
    err = json.Unmarshal([]byte(resp), &postResult)
    Expect(err).Should(Succeed())
    lastNewChange = postResult.Id

    waitResult := <- ch

    Expect(waitResult).Should(Equal(postResult.Id))
  })

  It("Blocking retrieval after abnormal change", func() {
    respBody := getChanges(lastNewChange, 100, 0)
    var results []JsonData
    err := json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(0))

    ensureTenant("blockTest")

    ch := make(chan uint64, 1)

    go func() {
      newResp := getChanges(lastNewChange, 100, 5)
      var newResults []JsonData
      json.Unmarshal(newResp, &newResults)
      if len(newResults) == 1 {
        ch <- newResults[0].Id
      } else {
        ch <- 0
      }
    }()

    request := "{\"hello\": \"world!\", \"foo\": 9999}"
    time.Sleep(500 * time.Millisecond)
    resp := postChange(request)

    var postResult JsonData
    err = json.Unmarshal([]byte(resp), &postResult)
    Expect(err).Should(Succeed())
    lastNewChange = postResult.Id

    waitResult := <- ch

    Expect(waitResult).Should(Equal(postResult.Id))
  })
})

func postChanges(collection string, count int) []uint64 {
  changes := make([]uint64, count)
  for n := 0; n < count; n++ {
    request :=
    fmt.Sprintf("{\"collection\":\"%s\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"count\":%d}}",
      collection, n + 1)
    resp := postChange(request)

    lastNewChange++
    expected := fmt.Sprintf("{\"_id\":%d}", lastNewChange)
    Expect(resp).Should(MatchJSON(expected))
    changes[n] = lastNewChange
  }
  return changes
}

func postChange(request string) string {
  uri := listenUri + "/changes"

  fmt.Fprintf(GinkgoWriter, "POST change: %s\n", request)
  pr, err := http.Post(uri, jsonContent, strings.NewReader(request))
  Expect(err).Should(Succeed())
  defer pr.Body.Close()
  body, err := ioutil.ReadAll(pr.Body)
  fmt.Fprintf(GinkgoWriter, "Response: %s\n", string(body))
  Expect(pr.StatusCode).Should(Equal(200))
  numPosts++

  Expect(err).Should(Succeed())
  resp := string(body)

  fmt.Fprintf(GinkgoWriter, "Got POST response %s\n", resp)
  return resp
}

func getChanges(since uint64, limit int, block int) []byte {
  url := fmt.Sprintf("%s/changes?since=%d&limit=%d&block=%d",
    listenUri, since, limit, block)
  gr, err := http.Get(url)
  Expect(err).Should(Succeed())
  defer gr.Body.Close()
  Expect(gr.StatusCode).Should(Equal(200))

  respBody, err := ioutil.ReadAll(gr.Body)
  Expect(err).Should(Succeed())
  resp := string(respBody)
  fmt.Fprintf(GinkgoWriter, "Got GET response %s\n", resp)
  return respBody
}

func getTenantChanges(tenant string, since uint64, limit int, block int) []byte {
  url := fmt.Sprintf("%s/tenants/%s/changes?since=%d&limit=%d&block=%d",
    listenUri, tenant, since, limit, block)
  gr, err := http.Get(url)
  Expect(err).Should(Succeed())
  defer gr.Body.Close()
  Expect(gr.StatusCode).Should(Equal(200))

  respBody, err := ioutil.ReadAll(gr.Body)
  Expect(err).Should(Succeed())
  resp := string(respBody)
  fmt.Fprintf(GinkgoWriter, "Got GET response %s\n", resp)
  return respBody
}
