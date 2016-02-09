package main

import (
  "errors"
  "strconv"
  "time"
  "net/http"
  "github.com/golang/glog"
  "github.com/gin-gonic/gin"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

const (
  JSONContent = "application/json"
  ChangesURI = "/changes"
  DefaultSince = "0"
  DefaultLimit = "100"
  DefaultBlock = "0"
  CommitTimeoutSeconds = 10
)

func (a *ChangeAgent) initChangesAPI() {
  a.api.POST(ChangesURI, a.handlePostChanges)
  a.api.GET(ChangesURI, a.handleGetChanges)
}

/*
 * POST a new change. Change must be valid JSON. Result will include the index
 * of the change.
 */
func (a *ChangeAgent) handlePostChanges(c *gin.Context) {
  if c.ContentType() != JSONContent {
    // TODO regexp?
    c.AbortWithStatus(http.StatusUnsupportedMediaType)
    return
  }

  defer c.Request.Body.Close()
  proposal, err := unmarshalJson(c.Request.Body)
  if err != nil {
    c.AbortWithError(http.StatusBadRequest, errors.New("Invalid JSON"))
    return
  }

  // Timestamp and otherwise update the proposal
  proposal.Timestamp = time.Now()

  // Send the raft proposal. This happens asynchronously.
  newIndex, err := a.raft.Propose(proposal)
  if err != nil {
    glog.Warningf("Fatal error making Raft proposal: %v", err)
    writeError(c, http.StatusInternalServerError, err)
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
    c.Header("Content-Type", JSONContent)
    marshalJson(&newEntry, c.Writer)
  } else {
    writeError(c, 503, errors.New("Commit timeout"))
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
func (a *ChangeAgent) handleGetChanges(c *gin.Context) {
  limitStr := c.DefaultQuery("limit", DefaultLimit)
  limit, err := strconv.ParseUint(limitStr, 10, 32)
  if err != nil {
    c.AbortWithError(http.StatusBadRequest, errors.New("Invalid limit"))
    return
  }

  sinceStr := c.DefaultQuery("since", DefaultSince)
  since, err := strconv.ParseUint(sinceStr, 10, 64)
  if err != nil {
    c.AbortWithError(http.StatusBadRequest, errors.New("Invalid since"))
    return
  }
  first := since + 1

  blockStr := c.DefaultQuery("block", DefaultBlock)
  bk, err := strconv.ParseUint(blockStr, 10, 32)
  if err != nil {
    c.AbortWithError(http.StatusBadRequest, errors.New("Invalid block"))
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
    c.Header("Content-Type", JSONContent)
    marshalChanges(nil, c.Writer)
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
    writeError(c, http.StatusInternalServerError, err)
    return
  }
  glog.V(2).Infof("Got %d entries", len(entries))

  c.Header("Content-Type", JSONContent)
  marshalChanges(entries, c.Writer)
}

func writeError(c *gin.Context, code int, err error) {
  c.Header("Content-Type", JSONContent)
  c.Status(code)
  marshalError(err, c.Writer)
}
