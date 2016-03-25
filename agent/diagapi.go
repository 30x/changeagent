package main

import (
  "encoding/json"
  "fmt"
  "runtime"
  "net/http"
  "revision.aeip.apigee.net/greg/changeagent/raft"
)

const (
  BaseURI = "/diagnostics"
  StackURI = BaseURI + "/stack"
  IdURI = BaseURI + "/id"
  RaftURI = BaseURI + "/raft"
  RaftStateURI = RaftURI + "/state"
  RaftLeaderURI = RaftURI + "/leader"

  PlainText = "text/plain"
  Json = "application/json"
)

type RaftState struct {
  State string `json:"state"`
  Leader uint64 `json:"leader"`
}

func (a *ChangeAgent) initDiagnosticApi() {
  a.router.HandleFunc(IdURI, a.handleIdCall).Methods("GET")
  a.router.HandleFunc(RaftStateURI, a.handleStateCall).Methods("GET")
  a.router.HandleFunc(RaftLeaderURI, a.handleLeaderCall).Methods("GET")
  a.router.HandleFunc(RaftURI, a.handleRaftInfo).Methods("GET")
  a.router.HandleFunc(StackURI, handleStackCall).Methods("GET")
}

func (a *ChangeAgent) handleIdCall(resp http.ResponseWriter, req *http.Request) {
  msg := fmt.Sprintf("%d\n", a.raft.MyId())
  resp.Header().Set("Content-Type", PlainText)
  resp.Write([]byte(msg))
}

func (a *ChangeAgent) handleStateCall(resp http.ResponseWriter, req *http.Request) {
  resp.Header().Set("Content-Type", PlainText)
  resp.Write([]byte(a.GetRaftState().String()))
}

func (a *ChangeAgent) handleLeaderCall(resp http.ResponseWriter, req *http.Request) {
  resp.Header().Set("Content-Type", PlainText)
  resp.Write([]byte(fmt.Sprintf("%d", a.getLeaderID())))
}

func (a *ChangeAgent) handleRaftInfo(resp http.ResponseWriter, req *http.Request) {
  state := RaftState{
    State: a.GetRaftState().String(),
    Leader: a.getLeaderID(),
  }

  body, _ := json.Marshal(&state)

  resp.Header().Set("Content-Type", Json)
  resp.Write(body)
}

func (a *ChangeAgent) getLeaderID() uint64 {
  if a.GetRaftState() == raft.Leader {
    return a.raft.MyId()
  }
  return a.raft.GetLeaderId()
}

func handleStackCall(resp http.ResponseWriter, req *http.Request) {
  stackBufLen := 64
  for {
    stackBuf := make([]byte, stackBufLen)
    stackLen := runtime.Stack(stackBuf, true)
    if stackLen == len(stackBuf) {
      // Must be truncated
      stackBufLen *= 2
    } else {
      resp.Header().Set("Content-Type", PlainText)
      resp.Write(stackBuf)
      return
    }
  }
}
