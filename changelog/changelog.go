package changelog

import (
  "errors"
  "fmt"
  "os"
  "regexp"
  "strconv"

  "revision.aeip.apigee.net/greg/changeagent/log"
)

type Log struct {
  dir string
  name string
  extents []extentInfo
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

type extentInfo struct {
  index int
  startSequence uint64
}

func CreateChangeLog(dir string, name string) (*Log, error) {
  l := &Log{
    dir: dir,
    name: name,
    extents: make([]extentInfo, 0),
    requestCh: make(chan appendRequest),
    stopCh: make(chan bool),
  }

  err := l.readExtents()
  if err != nil {
    return nil, err
  }

  go l.runLoop()
  return l, nil
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

func (l* Log) readExtents() error {
  log.Infof("Reading log extents from %s", l.dir)
  dir, err := os.Open(l.dir)
  if err != nil {
    return err
  }
  defer dir.Close()

  re, err := regexp.Compile(fmt.Sprintf("^%s-([0-9]+)$", l.name))
  if err != nil {
    return err
  }

  names, err := dir.Readdirnames(-1)
  if err != nil {
    return err
  }

  for _, n := range(names) {
    matches := re.FindStringSubmatch(n)
    if matches != nil {
      extentId, _ := strconv.Atoi(matches[1])
      log.Infof("Found matching log extent %d", extentId)
    }
  }
  // TODO now put information in the extent table about this!
  return nil
}
