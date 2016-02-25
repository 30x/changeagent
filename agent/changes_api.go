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
  proposal, err := unmarshalJson(req.Body)
  if err != nil {
    writeError(resp, http.StatusBadRequest, errors.New("Invalid JSON"))
    return
  }

  newEntry, err := a.makeProposal(proposal)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  body, err := marshalJson(newEntry)
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
  limit, err := strconv.ParseUint(limitStr, 10, 32)
  if err != nil {
    writeError(resp, http.StatusBadRequest, errors.New("Invalid limit"))
    return
  }

  sinceStr := qps.Get("since")
  if sinceStr == "" { sinceStr = DefaultSince }
  since, err := strconv.ParseUint(sinceStr, 10, 64)
  if err != nil {
    writeError(resp, http.StatusBadRequest, errors.New("Invalid since"))
    return
  }
  first := since + 1

  blockStr := qps.Get("block")
  if blockStr == "" { blockStr = DefaultBlock }
  bk, err := strconv.ParseUint(blockStr, 10, 32)
  if err != nil {
    writeError(resp, http.StatusBadRequest, errors.New("Invalid block"))
    return
  }
  block := time.Duration(bk)

  // Check how many changes there will be and block if we must
  last := a.raft.GetLastApplied()
  if (last < first) && (block > 0) {
    a.raft.GetAppliedTracker().TimedWait(first, time.Second * block)
    last = a.raft.GetLastApplied()
  }

  // Still no changes?
  if last < first {
    resp.Header().Set("Content-Type", JSONContent)
    resp.Write([]byte("[]"))
    return
  }

  // Limit the result set if necessary
  if (last - first) + 1 > limit {
    last = first + limit - 1
  }
  // TODO what happens when we truncate stuff as below? handle that soon.

  glog.V(2).Infof("Getting changes from %d to %d", first, last)

  entries, err := a.stor.GetEntries(first, last)
  if err != nil {
    glog.Errorf("Error getting changes from DB: %v", err)
    writeError(resp, http.StatusInternalServerError, err)
    return
  }
  glog.V(2).Infof("Got %d entries", len(entries))

  toMarshal := make([]storage.Entry, 0)
  for _, entry := range(entries) {
    if entry.Type == NormalChange {
      toMarshal = append(toMarshal, entry)
    }
  }
  glog.V(2).Infof("Final list has %d entries", len(toMarshal))

  outBody, err := marshalChanges(toMarshal)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  resp.Header().Set("Content-Type", JSONContent)
  resp.Write(outBody)
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
    outBody, err := marshalJson(entry)
    if err != nil {
      writeError(resp, http.StatusInternalServerError, err)
      return
    }
    resp.Header().Set("Content-Type", JSONContent)
    resp.Write(outBody)
  }
}
