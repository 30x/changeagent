package main

import (
  "errors"
  "strconv"
  "time"
  "net/http"
  "github.com/golang/glog"
  "github.com/gorilla/mux"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

const (
  DefaultSince = "0"
  DefaultLimit = "100"
  DefaultBlock = "0"
  CommitTimeoutSeconds = 10

  ChangesURI = "/changes"
  SingleChange = ChangesURI + "/{change}"
)

func (a *ChangeAgent) initChangesAPI() {
  a.router.HandleFunc(ChangesURI, a.handlePostChanges).Methods("POST")
  a.router.HandleFunc(ChangesURI, a.handleGetChanges).Methods("GET")
  a.router.HandleFunc(SingleChange, a.handleGetChange).Methods("GET")
}

/*
 * POST a new change. Change must be valid JSON. Result will include the index
 * of the change.
 */
func (a *ChangeAgent) handlePostChanges(resp http.ResponseWriter, req *http.Request) {
  if req.Header.Get("Content-Type")!= JSONContent {
    // TODO regexp?
    writeError(resp, http.StatusUnsupportedMediaType, errors.New("Unsupported content type"))
    return
  }

  defer req.Body.Close()
  proposal, err := unmarshalJSON(req.Body)
  if err != nil {
    writeError(resp, http.StatusBadRequest, errors.New("Invalid JSON"))
    return
  }

  newEntry, err := a.makeProposal(proposal)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  body, err := marshalJSON(newEntry)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  resp.Header().Set("Content-Type", JSONContent)
  resp.Write(body)
}

/*
 * GET an array of changes.
 * Query params:
 *   limit (integer): Maximum number to return, default 100
 *   since (integer): If set, return all changes HIGHER than this. Default 0.
 *   block (integer): If set and there are no changes, wait for up to "block" seconds
 *     until there are some changes to return
 * Result will be an array of objects, with metadata plus original JSON data.
 */
func (a *ChangeAgent) handleGetChanges(resp http.ResponseWriter, req *http.Request) {
  qps := req.URL.Query()

  limitStr := qps.Get("limit")
  if limitStr == "" { limitStr = DefaultLimit }
  lmt, err := strconv.ParseUint(limitStr, 10, 32)
  if err != nil {
    writeError(resp, http.StatusBadRequest, errors.New("Invalid limit"))
    return
  }
  limit := uint(lmt)

  sinceStr := qps.Get("since")
  if sinceStr == "" { sinceStr = DefaultSince }
  since, err := strconv.ParseUint(sinceStr, 10, 64)
  if err != nil {
    writeError(resp, http.StatusBadRequest, errors.New("Invalid since"))
    return
  }

  blockStr := qps.Get("block")
  if blockStr == "" { blockStr = DefaultBlock }
  bk, err := strconv.ParseUint(blockStr, 10, 32)
  if err != nil {
    writeError(resp, http.StatusBadRequest, errors.New("Invalid block"))
    return
  }
  block := time.Duration(bk)

  tags := qps["tag"]

  entries, lastFullChange, err := a.fetchEntries(since, limit, tags, resp)
  if err != nil { return }

  if (len(entries) == 0) && (block > 0) {
    now := time.Now()
    waitEnd := now.Add(block * time.Second)
    waitFor := lastFullChange
    for len(entries) == 0 && waitEnd.After(now) {
      // Because of tags, do this in a loop, so we check for tags every time and re-wait
      waitFor++
      waitRemaining := waitEnd.Sub(now)
      glog.V(2).Infof("Waiting %d milliseconds for the next change after %d", waitRemaining, waitFor)
      a.raft.GetAppliedTracker().TimedWait(waitFor, waitRemaining)
      entries, _, err = a.fetchEntries(waitFor - 1, limit, tags, resp)
      if err != nil { return }
      glog.V(2).Infof("Got %d changes after blocking", len(entries))
      now = time.Now()
    }
  }

  outBody, err := marshalChanges(entries)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  resp.Header().Set("Content-Type", JSONContent)
  resp.Write(outBody)
}

func (a *ChangeAgent) fetchEntries(
    since uint64,
    limit uint,
    tags []string,
    resp http.ResponseWriter) ([]storage.Entry, uint64, error) {

  var entries []storage.Entry
  var err error
  lastRawChange := since

  glog.V(2).Infof("Fetching up to %d changes since %d", limit, since)

  entries, err = a.stor.GetEntries(since, limit,
    func(e *storage.Entry) bool {
      if e.Index > since {
        lastRawChange = e.Index
      }
      if e.Type != NormalChange {
        return false
      }
      if tags == nil {
        return true
      }
      return e.MatchesTags(tags)
    })

  if err == nil {
    glog.V(2).Infof("Retrieved %d changes. raw = %d", len(entries), lastRawChange)
    return entries, lastRawChange, nil
  }
  glog.Errorf("Error getting changes from DB: %v", err)
  writeError(resp, http.StatusInternalServerError, err)
  return nil, lastRawChange, err
}

func (a *ChangeAgent) handleGetChange(resp http.ResponseWriter, req *http.Request) {
  idStr := mux.Vars(req)["change"]
  id, err := strconv.ParseUint(idStr, 10, 64)
  if err != nil {
    writeError(resp, http.StatusBadRequest, errors.New("Invalid ID"))
    return
  }

  entry, err := a.stor.GetEntry(id)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  if entry == nil {
    writeError(resp, http.StatusNotFound, errors.New("Not found"))

  } else {
    outBody, err := marshalJSON(*entry)
    if err != nil {
      writeError(resp, http.StatusInternalServerError, err)
      return
    }
    resp.Header().Set("Content-Type", JSONContent)
    resp.Write(outBody)
  }
}
