package main

import (
  "errors"
  "runtime"
  "net/http"
)

const (
  StackURI = "/diagnostics/stack"
  PlainText = "text/plain"
)

func initDiagnosticApi(mux *http.ServeMux) {
  mux.HandleFunc(StackURI, handleStackCall)
}

func handleStackCall(resp http.ResponseWriter, req *http.Request) {
  if req.URL.Path != StackURI {
    writeError(resp, 404, errors.New("Not found"))
    return
  }
  if req.Method != "GET" {
    writeError(resp, 405, errors.New("Method not allowed"))
    return
  }

  stackBufLen := 64
  for {
    stackBuf := make([]byte, stackBufLen)
    stackLen := runtime.Stack(stackBuf, true)
    if stackLen == len(stackBuf) {
      // Must be truncated
      stackBufLen *= 2
    } else {
      resp.Header().Add(http.CanonicalHeaderKey("Content-Type"), PlainText)
      resp.Write(stackBuf)
      return
    }
  }
}
