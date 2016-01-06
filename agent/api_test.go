package main

import (
  "fmt"
  "strings"
  "testing"
  "time"
  "net/http"
  "io/ioutil"
)

const (
  jsonContent = "application/json"
)

var lastNewChange uint64 = 1

func TestPostChange(t *testing.T) {
  uri := getLeaderURI() + "/changes"
  request := "{\"hello\": \"world!\", \"foo\": 123}"

  pr, err := http.Post(uri, jsonContent, strings.NewReader(request))
  if err != nil { t.Fatalf("Post error: %v", err) }
  if pr.StatusCode != 200 {
    t.Fatalf("Invalid HTTP status: %d", pr.StatusCode)
  }
  defer pr.Body.Close()

  reqBody, err := ioutil.ReadAll(pr.Body)
  if err != nil { t.Fatalf("Read error: %v", err) }
  reqResp := string(reqBody)

  lastNewChange++
  expected := fmt.Sprintf("{\"_id\":%d}", lastNewChange)
  if reqResp != expected {
    t.Fatalf("Got response:\n%s\nexpected:\n%s", reqResp, expected)
  }

  // Upon return, change should immediately be represented at the leader
  respExpected :=
    fmt.Sprintf("[{\"_id\":%d,\"data\":{\"hello\":\"world!\",\"foo\":123}}]", lastNewChange)
  peerChanges := getChanges(t, leaderIndex, lastNewChange - 1)
  if peerChanges != respExpected {
    t.Fatalf("Peer %d: Got final response of \"%s\" instead of \"%s\"",
      leaderIndex, peerChanges, respExpected)
  }

  // Check that the change was also replicated to all followers
  for i, a := range(testAgents) {
    a.raft.GetAppliedTracker().TimedWait(lastNewChange - 1, 2 * time.Second)
    peerChanges:= getChanges(t, i, lastNewChange - 1)
    if peerChanges != respExpected {
      t.Fatalf("Peer %d: Got final response of \"%s\" instead of \"%s\"",
        leaderIndex, peerChanges, respExpected)
    }
  }
}

func TestEmptyChange(t *testing.T) {
  respExpected := "[]"
  for i := range(testAgents) {
    peerChanges:= getChanges(t, i, lastNewChange)
    if peerChanges != respExpected {
      t.Fatalf("Peer %d: Got final response of \"%s\" instead of \"%s\"",
        leaderIndex, peerChanges, respExpected)
    }
  }
}

func getChanges(t *testing.T, peerIndex int, since uint64) string {
  gr, err := http.Get(fmt.Sprintf("%s/changes?since=%d",
    getListenerURI(peerIndex), since))
  if err != nil { t.Fatalf("Get error: %v", err) }
  if gr.StatusCode != 200 {
    t.Fatalf("Invalid HTTP status: %d", gr.StatusCode)
  }
  defer gr.Body.Close()

  respBody, err := ioutil.ReadAll(gr.Body)
  if err != nil { t.Fatalf("Read error: %v", err) }
  return string(respBody)
}
