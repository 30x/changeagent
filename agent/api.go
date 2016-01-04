package main

import (
  "strconv"
  "net/http"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

const (
  JSONContent = "application/json"
  ChangesURI = "/changes"
  DefaultSince = uint64(0)
  DefaultLimit = 100
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

  err = a.raft.Propose(proposal)
  if err != nil {
    log.Infof("Fatal error making Raft proposal: %v", err)
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  // TODO do we want to wait for commit? I think that we do!
  // TODO we want raft to send back the index of the commit!

  metadata := &JsonData{
    Id: 123456,
  }

  resp.Header().Add(http.CanonicalHeaderKey("content-type"), JSONContent)
  marshalJson(nil, metadata, resp)
}

/*
 * GET an array of changes.
 * Query params:
 *   limit (integer): Maximum number to return, default 100
 *   since (integer): If set, return all changes HIGHER than this. Default 0.
 * Result will be an array of objects, with metadata plus original JSON data.
 */
func (a *ChangeAgent) handleGetChanges(resp http.ResponseWriter, req *http.Request) {
  qps := req.URL.Query()
  limit := DefaultLimit
  since := DefaultSince

  if qps["limit"] != nil {
    il, err := strconv.ParseUint(qps.Get("limit"), 10, 32)
    if err != nil {
      http.Error(resp, "Invalid limit", http.StatusBadRequest)
      return
    }
    limit = int(il)
  }

  if qps["since"] != nil {
    ul, err := strconv.ParseUint(qps.Get("since"), 10, 64)
    if err != nil {
      http.Error(resp, "Invalid since value", http.StatusBadRequest)
      return
    }
    since = ul
  }

  changes, err := a.stor.GetChanges(since, limit)
  if err != nil {
    log.Infof("Error getting changes from DB: %v", err)
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  resp.Header().Add(http.CanonicalHeaderKey("content-type"), JSONContent)
  marshalChanges(changes, resp)
}

func writeError(resp http.ResponseWriter, code int, err error) {
  resp.Header().Set(http.CanonicalHeaderKey("content-type"), JSONContent)
  resp.WriteHeader(code)
  marshalError(err, resp)
}
