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

  _, len := appendOne(t, app, 1, "one", "Record one", 0)
  newPos := int64(len)
  appendOne(t, app, 2, "two", "Second record", newPos)

  app.stop()
}

func TestAppenderSwitch(t *testing.T) {
  os.Mkdir(appenderTestDir, 0777)
  defer os.RemoveAll(appenderTestDir)

  app, err := startAppender(appenderTestDir, fileBase, 0)
  if err != nil {
    t.Fatalf("Error starting appender: %v", err)
  }

  _, len := appendOne(t, app, 1, "one", "Record one", 0)
  newPos := int64(len)
  appendOne(t, app, 2, "two", "Second record", newPos)
  app.switchFiles()
  appendOne(t, app, 2, "two", "Second record", 0)

  app.stop()
}

func appendOne(t *testing.T, app *logAppender, seq uint64, key string, msg string, expectedPos int64) (int64, int) {
  lr := &LogRecord{
    sequence: seq,
    key: key,
    tenantId: "A",
    data: []byte(msg),
  }
  pos, len, err := app.append(lr)
  if err != nil {
    t.Fatalf("Error on append")
  }
  if pos != expectedPos {
    t.Fatalf("Position %d doesn't match expected %d", pos, expectedPos)
  }
  if len <= 0 {
    t.Fatalf("Expected a positive length")
  }
  return pos, len
}
