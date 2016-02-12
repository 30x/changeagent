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
  BeforeEach(func() {
    waitForLeader()
    getLeaderIndex()
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
    correctNodes := 0
    for i, a := range (testAgents) {
      a.raft.GetAppliedTracker().TimedWait(lastNewChange, 2 * time.Second)
      peerChanges := getChanges(i, lastNewChange - 1, 100, 0)
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
    request := "{\"tenant\":\"foo\",\"collection\":\"bar\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":456}}"
    resp := postChange(request)

    lastNewChange++
    expected := fmt.Sprintf("{\"_id\":%d}", lastNewChange)
    Expect(resp).Should(MatchJSON(expected))

    respExpected :=
      fmt.Sprintf("[{\"_id\":%d,\"_ts\":[0-9]+,\"tenant\":\"foo\",\"collection\":\"bar\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":456}}]", lastNewChange)
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
    request := "{\"tenant\":\"foo\",\"collection\":\"bar\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":888}}"
    resp := postChange(request)

    lastNewChange++
    expected := fmt.Sprintf("{\"_id\":%d}", lastNewChange)
    Expect(resp).Should(MatchJSON(expected))

    request = "{\"tenant\":\"foo\",\"collection\":\"bar\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":999}}"
    resp = postChange(request)

    lastNewChange++
    expected = fmt.Sprintf("{\"_id\":%d}", lastNewChange)
    Expect(resp).Should(MatchJSON(expected))

    templ :=
      "[{\"_id\":%d,\"_ts\":[0-9]+,\"tenant\":\"foo\",\"collection\":\"bar\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":888}}," +
      "{\"_id\":%d,\"_ts\":[0-9]+,\"tenant\":\"foo\",\"collection\":\"bar\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":999}}]"
    respExpected := fmt.Sprintf(templ, lastNewChange - 1, lastNewChange)
    peerChanges := getChanges(leaderIndex, lastNewChange - 2, 100, 0)

    Expect(peerChanges).Should(MatchRegexp(respExpected))
  })

  It("Retrieve all", func() {
    respBody := getChanges(leaderIndex, 0, 100, 0)
    var results []JsonData
    err := json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(BeEquivalentTo(numPosts))

    ids := make([]uint64, len(results))
    for i := range(results) {
      ids[i] = results[i].Id
    }

    // Test various permutations of offset and limit now.
    respBody = getChanges(leaderIndex, ids[0] - 1, 1, 0)
    err = json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(1))
    Expect(results[0].Id).Should(Equal(ids[0]))

    respBody = getChanges(leaderIndex, ids[0] - 1, 2, 0)
    err = json.Unmarshal(respBody, &results)
    Expect(err).Should(Succeed())
    Expect(len(results)).Should(Equal(2))
    Expect(results[0].Id).Should(Equal(ids[0]))
    Expect(results[1].Id).Should(Equal(ids[1]))
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
