/*
 * The code in this file is responsible for appending records to a log file,
 * and for switching log records when told.
 */

package changelog

import (
  "fmt"
  "os"
  "path/filepath"

  "revision.aeip.apigee.net/greg/changeagent/log"
)

const (
  appendQueueSize = 10
)

type logAppender struct {
  curFile *os.File
  position int64
  baseDir string
  baseName string
  fileSeq int
  appendCh chan appendRequest
  switchCh chan bool
  stopCh chan bool
}

type appendResponse struct {
  position int64
  length int
  err error
}

type appendRequest struct {
  rec *LogRecord
  ch chan appendResponse
}

/*
 * Open the file specified by "startSeq" and be ready for appending.
 */
func startAppender(baseDir string, baseName string, startSeq int) (*logAppender, error) {
  a := &logAppender{
    baseDir: baseDir,
    baseName: baseName,
    fileSeq: startSeq,
    appendCh: make(chan appendRequest, appendQueueSize),
    switchCh: make(chan bool),
    stopCh: make(chan bool),
    position: 0,
  }

  // Open a new log file. We will bomb if it exists.
  err := a.openFile(startSeq)
  if err != nil {
    return nil, err
  }

  go a.appendLoop()
  return a, nil
}

/*
 * Stop the appender.
 */
func (a *logAppender) stop() {
  a.stopCh <- true
}

/*
 * Append a single log file. Return three values: The position at which
 * the record was appended; the number of bytes appended; and an error
 */
func (a *logAppender) append(rec *LogRecord) (int64, int, error) {
  respCh := make(chan appendResponse)
  req := appendRequest{
    rec: rec,
    ch: respCh,
  }
  a.appendCh <- req
  resp := <- respCh
  return resp.position, resp.length, resp.err
}

/*
 * Switch to a new log file. Every time this is called, we switch once.
 */


/*
 * Internal code -- the main append loop.
 */

func (a *logAppender) appendLoop() {
  running := true
  for running {
    select {
    case req := <- a.appendCh:
      a.doAppend(req)
    case <- a.switchCh:
      a.switchFiles()
    case <- a.stopCh:
      running = false
    }
  }

  log.Info("Done appending to log. Closing file.")
  a.curFile.Close()
}

func (a* logAppender) switchFiles() {
  a.fileSeq++
  a.openFile(a.fileSeq)
}

func (a* logAppender) openFile(fileSeq int) error {
  if a.curFile != nil {
    log.Debug("Closing existing log file")
    a.curFile.Close()
  }

  n := fmt.Sprintf("%s-%010d", a.baseName, fileSeq)
  fn := filepath.Join(a.baseDir, n)
  log.Infof("Opening %s to receive new log records", fn)

  file, err :=
    os.OpenFile(fn, os.O_WRONLY | os.O_CREATE | os.O_EXCL, 0666)
  if err != nil {
    log.Infof("Error opening log file: %v", err)
    return err
  }

  a.curFile = file
  return nil
}

func (a *logAppender) doAppend(req appendRequest) {
  bb := marshalLogRecord(req.rec)
  _, err := a.curFile.WriteAt(bb, a.position)
  if err != nil {
    req.ch <- appendResponse{err: err}
    return
  }

  resp := appendResponse{
    position: a.position,
    length: len(bb),
  }
  a.position += int64(len(bb))
  req.ch <- resp
}
