package main

import (
  "fmt"
  "regexp"
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
    waitForLeader()
    getLeaderIndex()

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
    peerChanges := getChanges(leaderIndex, lastNewChange - 1, 100, 0)
    Expect(peerChanges).Should(MatchRegexp(respExpected))

    // Check that the change was also replicated to all followers
    fmt.Fprintf(GinkgoWriter, "Leader is peer %d\n", leaderIndex)
    correctNodes := 0
    for i, _ := range (testAgents) {
      peerChanges := getChanges(i, lastNewChange - 1, 100, 10)
      match, err := regexp.MatchString(respExpected, string(peerChanges))
      fmt.Fprintf(GinkgoWriter, "Get changes peer %d: \"%s\"\n", i, peerChanges)
      Expect(err).Should(Succeed())
      if match {
        correctNodes++
      }
    }
    Expect(correctNodes).Should(Equal(len(testAgents)))
  })

  It("POST indexed record", func() {
    request :=
      fmt.Sprintf("{\"collection\":\"%s\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":456}}", collection)
    resp := postChange(request)

    lastNewChange++
    expected := fmt.Sprintf("{\"_id\":%d}", lastNewChange)
    Expect(resp).Should(MatchJSON(expected))

    respExpected :=
      fmt.Sprintf("[{\"_id\":%d,\"_ts\":[0-9]+,\"collection\":\"%s\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":456}}]",
        lastNewChange, collection)
    peerChanges := getChanges(leaderIndex, lastNewChange - 1, 100, 0)

    Expect(peerChanges).Should(MatchRegexp(respExpected))
  })

  It("POST empty change", func() {
    for i := range (testAgents) {
      peerChanges := getChanges(i, lastNewChange, 100, 0)
      Expect(strings.TrimSpace(string(peerChanges))).Should(Equal("[]"))
    }
  })

  It("Post and retrieve multiple", func() {
    changes := postChanges(collection, 2)

    templ :=
      "[{\"_id\":%d,\"_ts\":[0-9]+,\"collection\":\"%s\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"count\":1}}," +
      "{\"_id\":%d,\"_ts\":[0-9]+,\"collection\":\"%s\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"count\":2}}]"
    respExpected := fmt.Sprintf(templ, lastNewChange - 1, collection, lastNewChange, collection)
    peerChanges := getChanges(leaderIndex, changes[0] - 1, 100, 0)

    Expect(peerChanges).Should(MatchRegexp(respExpected))
  })

  It("Retrieve all", func() {
    changes := postChanges(collection, 3)

    respBody := getChanges(leaderIndex, 0, 100, 0)
    var results []JsonData
    err := json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(BeNumerically(">=", 3))

    // Test various permutations of offset and limit now.
    respBody = getChanges(leaderIndex, changes[0] - 1, 1, 0)
    err = json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(1))
    Expect(results[0].Id).Should(Equal(changes[0]))

    respBody = getChanges(leaderIndex, changes[0] - 1, 2, 0)
    err = json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(2))
    Expect(results[0].Id).Should(Equal(changes[0]))
    Expect(results[1].Id).Should(Equal(changes[1]))
  })

  It("Blocking retrieval", func() {
    respBody := getChanges(leaderIndex, lastNewChange, 100, 0)
    var results []JsonData
    err := json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(0))

    ch := make(chan uint64, 1)

    go func() {
      newResp := getChanges(leaderIndex, lastNewChange, 100, 5)
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
  uri := getLeaderURI() + "/changes"

  fmt.Fprintf(GinkgoWriter, "POST change: %s\n", request)
  pr, err := http.Post(uri, jsonContent, strings.NewReader(request))
  Expect(err).Should(Succeed())
  defer pr.Body.Close()
  Expect(pr.StatusCode).Should(Equal(200))
  numPosts++

  body, err := ioutil.ReadAll(pr.Body)
  Expect(err).Should(Succeed())
  resp := string(body)

  fmt.Fprintf(GinkgoWriter, "Got POST response %s\n", resp)
  return resp
}

func getChanges(peerIndex int, since uint64, limit int, block int) []byte {
  url := fmt.Sprintf("%s/changes?since=%d&limit=%d&block=%d",
    getListenerURI(peerIndex), since, limit, block)
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
