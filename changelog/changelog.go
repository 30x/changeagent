package changelog

import (
  "errors"
)

type Log struct {
  dir string
  name string
  requestCh chan appendRequest
  stopCh chan bool
}

type LogRecord struct {
  lsn uint64
  sequence uint64
  tenantId string
  key string
  data []byte
}

func CreateChangeLog(dir string, name string) *Log {
  l := &Log{
    dir: dir,
    name: name,
    requestCh: make(chan appendRequest),
    stopCh: make(chan bool),
  }
  go l.runLoop()
  return l
}

func (l* Log) Close() {
  l.stopCh <- true
}

func (l* Log) Append(sequence uint64, tenantId string, key string, data []byte) (uint64, error) {
  // On open, read all file names from "dir" and sort numerically
  // Leave last one open for appending
  // send request to main run loop
  // It will delegate elsewhere
  return 0, errors.New("Not implemented")
}

func (l* Log) Read(lsn uint64) (*LogRecord, error) {
  // Use LSN to calculate file number
  // Cache opened files
  // Perhaps file opening can be done in a separate goroutine for each file?
  return nil, errors.New("Not implemented")
}

func (l* Log) ReadRange(lastLsn uint64, maxRecords int) ([]*LogRecord, error) {
  return nil, errors.New("Not implemented")
}

func (l* Log) runLoop() {
  running := true
  for running {
    select {
    case <- l.stopCh:
        running = false
    }
  }

  // Close files here
}
