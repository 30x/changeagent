package main

import (
  "runtime"
  "github.com/gin-gonic/gin"
)

const (
  StackURI = "/diagnostics/stack"
  PlainText = "text/plain"
)

func (a *ChangeAgent) initDiagnosticApi() {
  a.api.GET(StackURI, handleStackCall)
}

func handleStackCall(c *gin.Context) {
  stackBufLen := 64
  for {
    stackBuf := make([]byte, stackBufLen)
    stackLen := runtime.Stack(stackBuf, true)
    if stackLen == len(stackBuf) {
      // Must be truncated
      stackBufLen *= 2
    } else {
      c.Header("Content-Type", PlainText)
      c.Writer.Write(stackBuf)
      return
    }
  }
}
