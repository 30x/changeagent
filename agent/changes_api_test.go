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

  It("POST new change", func() {
    request := "{\"hello\": \"world!\", \"foo\": 123}"
    resp := postChange(request)

    entry, err := unmarshalJSON(strings.NewReader(resp))
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

    url := fmt.Sprintf("%s/changes/%d", listenURI, lastNewChange)
    gr, err := http.Get(url)
    Expect(err).Should(Succeed())
    defer gr.Body.Close()
    Expect(gr.StatusCode).Should(Equal(200))

    change, err := unmarshalJSON(gr.Body)
    Expect(err).Should(Succeed())
    Expect(gr.StatusCode).Should(Equal(200))
    Expect(change.Index).Should(Equal(lastNewChange))
  })

  It("POST empty change", func() {
    peerChanges := getChanges(lastNewChange, 100, 0)
    Expect(strings.TrimSpace(string(peerChanges))).Should(Equal("[]"))
  })

  It("Post and retrieve multiple", func() {
    changes := postChanges(2)

    templ :=
      "[{\"_id\":%d,\"_ts\":[0-9]+,\"data\":{\"hello\":\"world!\",\"count\":1}}," +
      "{\"_id\":%d,\"_ts\":[0-9]+,\"data\":{\"hello\":\"world!\",\"count\":2}}]"
    respExpected := fmt.Sprintf(templ, lastNewChange - 1, lastNewChange)
    peerChanges := getChanges(changes[0] - 1, 100, 0)

    Expect(peerChanges).Should(MatchRegexp(respExpected))
  })

  It("Retrieve all", func() {
    changes := postChanges(3)

    respBody := getChanges(0, 100, 0)
    var results []JSONData
    err := json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(BeNumerically(">=", 3))

    // Test various permutations of offset and limit now.
    respBody = getChanges(changes[0] - 1, 1, 0)
    err = json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(1))
    Expect(results[0].ID).Should(Equal(changes[0]))

    respBody = getChanges(changes[0] - 1, 2, 0)
    err = json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(2))
    Expect(results[0].ID).Should(Equal(changes[0]))
    Expect(results[1].ID).Should(Equal(changes[1]))
  })

  It("Blocking retrieval", func() {
    respBody := getChanges(lastNewChange, 100, 0)
    var results []JSONData
    err := json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(0))

    ch := make(chan uint64, 1)

    go func() {
      newResp := getChanges(lastNewChange, 100, 5)
      var newResults []JSONData
      json.Unmarshal(newResp, &newResults)
      if len(newResults) == 1 {
        ch <- newResults[0].ID
      } else {
        ch <- 0
      }
    }()

    request := "{\"hello\": \"world!\", \"foo\": 9999}"
    time.Sleep(500 * time.Millisecond)
    resp := postChange(request)

    var postResult JSONData
    err = json.Unmarshal([]byte(resp), &postResult)
    Expect(err).Should(Succeed())
    lastNewChange = postResult.ID

    waitResult := <- ch

    Expect(waitResult).Should(Equal(postResult.ID))
  })

  It("Blocking retrieval after abnormal change", func() {
    respBody := getChanges(lastNewChange, 100, 0)
    var results []JSONData
    err := json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(0))

    ch := make(chan uint64, 1)

    go func() {
      newResp := getChanges(lastNewChange, 100, 5)
      var newResults []JSONData
      json.Unmarshal(newResp, &newResults)
      if len(newResults) == 1 {
        ch <- newResults[0].ID
      } else {
        ch <- 0
      }
    }()

    request := "{\"hello\": \"world!\", \"foo\": 9999}"
    time.Sleep(500 * time.Millisecond)
    resp := postChange(request)

    var postResult JSONData
    err = json.Unmarshal([]byte(resp), &postResult)
    Expect(err).Should(Succeed())
    lastNewChange = postResult.ID

    waitResult := <- ch

    Expect(waitResult).Should(Equal(postResult.ID))
  })
})

func postChanges(count int) []uint64 {
  changes := make([]uint64, count)
  for n := 0; n < count; n++ {
    request :=
    fmt.Sprintf("{\"data\":{\"hello\":\"world!\",\"count\":%d}}", n + 1)
    resp := postChange(request)

    lastNewChange++
    expected := fmt.Sprintf("{\"_id\":%d}", lastNewChange)
    Expect(resp).Should(MatchJSON(expected))
    changes[n] = lastNewChange
  }
  return changes
}

func postChange(request string) string {
  uri := listenURI + "/changes"

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
    listenURI, since, limit, block)
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
