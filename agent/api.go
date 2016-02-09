package main

import (
  "errors"
  "strconv"
  "time"
  "net/http"
  "github.com/golang/glog"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

const (
  JSONContent = "application/json"
  ChangesURI = "/changes"
  DefaultSince = uint64(0)
  DefaultLimit = uint64(100)
  CommitTimeoutSeconds = 10
)

func (a *ChangeAgent) initAPI(mux *http.ServeMux) {
  mux.HandleFunc(ChangesURI, a.handleChangesCall)
}

func (a *ChangeAgent) handleChangesCall(resp http.ResponseWriter, req *http.Request) {
  if req.URL.Path == ChangesURI {
    if req.Method == "POST" {
      a.handlePostChanges(resp, req)
    } else if req.Method == "GET" {
      a.handleGetChanges(resp, req)
    } else {
      http.Error(resp, "Method not allowed", http.StatusMethodNotAllowed)
      return
    }
  } else {
    http.NotFound(resp, req)
  }
}

/*
 * POST a new change. Change must be valid JSON. Result will include the index
 * of the change.
 */
func (a *ChangeAgent) handlePostChanges(resp http.ResponseWriter, req *http.Request) {
  if req.Header.Get(http.CanonicalHeaderKey("content-type")) != JSONContent {
    // TODO regexp
    http.Error(resp, "Unsupported media type", http.StatusUnsupportedMediaType)
    return
  }

  defer req.Body.Close()
  proposal, err := unmarshalJson(req.Body)
  if err != nil {
    http.Error(resp, "Invalid JSON", http.StatusBadRequest)
    return
  }
  // Send the raft proposal. This happens asynchronously.
  newIndex, err := a.raft.Propose(proposal)
  if err != nil {
    glog.Warningf("Fatal error making Raft proposal: %v", err)
    writeError(resp, http.StatusInternalServerError, err)
    return
  }
  glog.V(2).Infof("Proposed new change with index %d", newIndex)

  // Wait for the new commit to be applied, or time out
  appliedIndex :=
    a.raft.GetAppliedTracker().TimedWait(newIndex, time.Second * CommitTimeoutSeconds)
  glog.V(2).Infof("New index %d is now applied", appliedIndex)
  if appliedIndex >= newIndex {
    newEntry := storage.Entry{
      Index: newIndex,
    }
    resp.Header().Add(http.CanonicalHeaderKey("content-type"), JSONContent)
    marshalJson(&newEntry, resp)
  } else {
    writeError(resp, 503, errors.New("Commit timeout"))
  }
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
  limit := DefaultLimit
  first := DefaultSince
  block := time.Duration(0)

  if qps["limit"] != nil {
    il, err := strconv.ParseUint(qps.Get("limit"), 10, 32)
    if err != nil {
      http.Error(resp, "Invalid limit", http.StatusBadRequest)
      return
    }
    limit = il
  }

  if qps["since"] != nil {
    ul, err := strconv.ParseUint(qps.Get("since"), 10, 64)
    if err != nil {
      http.Error(resp, "Invalid since value", http.StatusBadRequest)
      return
    }
    first = ul + 1
  }

  if qps["block"] != nil {
    bk, err := strconv.ParseUint(qps.Get("block"), 10, 32)
    if err != nil {
      http.Error(resp, "Invalid block value", http.StatusBadRequest)
      return
    }
    block = time.Duration(bk)
  }

  // Check how many changes there will be and block if we must
  last := a.raft.GetLastApplied()
  if (last < first) && (block > 0) {
    a.raft.GetAppliedTracker().TimedWait(first, time.Second * block)
    last = a.raft.GetLastApplied()
  }

  // Still no changes?
  if last < first {
    resp.Header().Add(http.CanonicalHeaderKey("content-type"), JSONContent)
    marshalChanges(nil, resp)
    return
  }

  // Limit the result set if necessary
  if (last - first) + 1 > limit {
    last = first + limit - 1
  }

  glog.V(2).Infof("Getting changes from %d to %d", first, last)

  entries, err := a.stor.GetEntries(first, last)
  if err != nil {
    glog.Errorf("Error getting changes from DB: %v", err)
    writeError(resp, http.StatusInternalServerError, err)
    return
  }
  glog.V(2).Infof("Got %d entries", len(entries))

  resp.Header().Add(http.CanonicalHeaderKey("content-type"), JSONContent)
  marshalChanges(entries, resp)
}

func writeError(resp http.ResponseWriter, code int, err error) {
  resp.Header().Set(http.CanonicalHeaderKey("content-type"), JSONContent)
  resp.WriteHeader(code)
  marshalError(err, resp)
}
