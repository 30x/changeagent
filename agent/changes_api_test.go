package main

import (
  "bytes"
  "fmt"
  "regexp"
  "strings"
  "time"
  "net/http"
  "io/ioutil"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

const (
  jsonContent = "application/json"
)

var lastNewChange uint64

var _ = Describe("Changes API Test", func() {
  BeforeEach(func() {
    waitForLeader()
    getLeaderIndex()
  })

  It("POST new change", func() {
    uri := getLeaderURI() + "/changes"
    request := "{\"hello\": \"world!\", \"foo\": 123}"

    pr, err := http.Post(uri, jsonContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))
    defer pr.Body.Close()

    respBody, err := ioutil.ReadAll(pr.Body)
    Expect(err).Should(Succeed())
    fmt.Fprintf(GinkgoWriter, "Got response body %s\n", respBody)

    reqResp, err := unmarshalJson(bytes.NewBuffer(respBody))
    Expect(err).Should(Succeed())
    fmt.Fprintf(GinkgoWriter, "Got response %s\n", reqResp)

    lastChange := reqResp.Index
    lastNewChange = lastChange

    // Upon return, change should immediately be represented at the leader
    respExpected :=
      fmt.Sprintf("[{\"_id\":%d,\"_ts\":[0-9]+,\"data\":{\"hello\":\"world!\",\"foo\":123}}]", lastChange)
    peerChanges := getChanges(leaderIndex, lastChange - 1)
    match, err := regexp.MatchString(respExpected, peerChanges)
    fmt.Fprintf(GinkgoWriter, "Post response: \"%s\"\n", peerChanges)
    Expect(err).Should(Succeed())
    Expect(match).Should(BeTrue())

    // Check that the change was also replicated to all followers
    correctNodes := 0
    for i, a := range (testAgents) {
      a.raft.GetAppliedTracker().TimedWait(lastChange, 2 * time.Second)
      peerChanges := getChanges(i, lastChange - 1)
      match, err := regexp.MatchString(respExpected, peerChanges)
      fmt.Fprintf(GinkgoWriter, "Get changes peer %d: \"%s\"\n", i, peerChanges)
      Expect(err).Should(Succeed())
      if match {
        correctNodes++
      }
    }
    Expect(correctNodes).Should(Equal(len(testAgents)))
  })

  It("POST indexed record", func() {
    uri := getLeaderURI() + "/changes"
    request := "{\"tenant\":\"foo\",\"collection\":\"bar\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":456}}"

    pr, err := http.Post(uri, jsonContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))
    defer pr.Body.Close()

    reqResp, err := unmarshalJson(pr.Body)
    Expect(err).Should(Succeed())
    fmt.Fprintf(GinkgoWriter, "Got response %s\n", reqResp)

    newChange := reqResp.Index
    lastNewChange = newChange

    respExpected :=
      fmt.Sprintf("[{\"_id\":%d,\"_ts\":[0-9]+,\"tenant\":\"foo\",\"collection\":\"bar\",\"key\":\"baz\",\"data\":{\"hello\":\"world!\",\"foo\":456}}]", newChange)
    peerChanges := getChanges(leaderIndex, newChange - 1)
    match, err := regexp.MatchString(respExpected, peerChanges)
    fmt.Fprintf(GinkgoWriter, "Post response: \"%s\"\n", peerChanges)
    Expect(err).Should(Succeed())
    Expect(match).Should(BeTrue())
  })

  It("POST empty change", func() {
    respExpected := "[]"
    for i := range (testAgents) {
      peerChanges := getChanges(i, lastNewChange)
      Expect(peerChanges).Should(Equal(respExpected))
    }
  })
})

func getChanges(peerIndex int, since uint64) string{
  gr, err := http.Get(fmt.Sprintf("%s/changes?since=%d",
    getListenerURI(peerIndex), since))
  Expect(err).Should(Succeed())
  Expect(gr.StatusCode).Should(Equal(200))
  defer gr.Body.Close()

  respBody, err := ioutil.ReadAll(gr.Body)
  Expect(err).Should(Succeed())
  return string(respBody)
}
