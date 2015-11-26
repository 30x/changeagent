package changelog

import (
  "testing"
  "os"
)

const (
  appenderTestDir = "./_appendertest"
  fileBase = "AppenderTest"
)

func TestAppender(t *testing.T) {
  os.Mkdir(appenderTestDir, 0777)
  defer os.RemoveAll(appenderTestDir)

  app, err := startAppender(appenderTestDir, fileBase, 0)
  if err != nil {
    t.Fatalf("Error starting appender: %v", err)
  }

  lr1 := &LogRecord{
    sequence: 1,
    key: "one",
    tenantId: "A",
    data: []byte("Record 1"),
  }
  pos, len, err := app.append(lr1)
  if err != nil {
    t.Fatalf("Error on append")
  }
  if pos != 0 {
    t.Fatalf("Position %d should be 0 because it's the first record", pos)
  }
  if len <= 0 {
    t.Fatalf("Expected a positive length")
  }

  lr2 := &LogRecord{
    sequence: 2,
    key: "two",
    tenantId: "A",
    data: []byte("Record 2"),
  }
  newPos, newLen, err := app.append(lr2)
  exPos := int64(len) + pos
  if err != nil {
    t.Fatalf("Error on append")
  }
  if newPos != exPos {
    t.Fatalf("Position %d should be %d because it's the first record", newPos, exPos)
  }
  if newLen <= 0 {
    t.Fatalf("Expected a positive length")
  }

  app.stop()
}
