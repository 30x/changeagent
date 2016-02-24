package main

import (
  "runtime"
  "net/http"
)

const (
  StackURI = "/diagnostics/stack"
  PlainText = "text/plain"
)

func (a *ChangeAgent) initDiagnosticApi() {
  a.router.HandleFunc(StackURI, handleStackCall).Methods("GET")
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
