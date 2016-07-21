package main

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/30x/changeagent/common"
	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
)

const (
	defaultSince = "0"
	defaultLimit = "100"
	defaultBlock = "0"

	changesURI   = "/changes"
	singleChange = changesURI + "/:change"
)

func (a *ChangeAgent) initChangesAPI(prefix string) {
	a.router.POST(prefix+changesURI, a.handlePostChanges)
	a.router.GET(prefix+changesURI, a.handleGetChanges)
	a.router.GET(prefix+singleChange, a.handleGetChange)
}

/*
 * POST a new change. Change must be valid JSON. Result will include the index
 * of the change.
 */
func (a *ChangeAgent) handlePostChanges(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	if !isJSON(resp, req) {
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

	resp.Header().Set("Content-Type", jsonContent)
	marshalJSON(newEntry, resp)
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
func (a *ChangeAgent) handleGetChanges(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	qps := req.URL.Query()

	limitStr := qps.Get("limit")
	if limitStr == "" {
		limitStr = defaultLimit
	}
	lmt, err := strconv.ParseUint(limitStr, 10, 32)
	if err != nil {
		writeError(resp, http.StatusBadRequest, errors.New("Invalid limit"))
		return
	}
	limit := uint(lmt)

	sinceStr := qps.Get("since")
	if sinceStr == "" {
		sinceStr = defaultSince
	}
	since, err := strconv.ParseUint(sinceStr, 10, 64)
	if err != nil {
		writeError(resp, http.StatusBadRequest, errors.New("Invalid since"))
		return
	}

	blockStr := qps.Get("block")
	if blockStr == "" {
		blockStr = defaultBlock
	}
	bk, err := strconv.ParseUint(blockStr, 10, 32)
	if err != nil {
		writeError(resp, http.StatusBadRequest, errors.New("Invalid block"))
		return
	}
	block := time.Duration(bk)

	tags := qps["tag"]

	// Fetch more than we need so we can see if we're at the beginning or end
	fetchLimit := limit + 1
	fetchSince := since
	if fetchSince > 0 {
		fetchSince--
		fetchLimit++
	}

	entries, lastFullChange, err := a.fetchEntries(fetchSince, fetchLimit, tags, resp)
	if err != nil {
		return
	}
	entries, atStart, atEnd := pruneChanges(entries, since, limit)

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
			entries, _, err = a.fetchEntries(waitFor-1, fetchLimit, tags, resp)
			if err != nil {
				return
			}
			entries, atStart, atEnd = pruneChanges(entries, since, limit)
			glog.V(2).Infof("Got %d changes after blocking", len(entries))
			now = time.Now()
		}
	}

	resp.Header().Set("Content-Type", jsonContent)
	marshalChanges(entries, atStart, atEnd, resp)
}

func (a *ChangeAgent) fetchEntries(
	since uint64,
	limit uint,
	tags []string,
	resp http.ResponseWriter) ([]common.Entry, uint64, error) {

	var entries []common.Entry
	var err error
	lastRawChange := since

	glog.V(2).Infof("Fetching up to %d changes since %d", limit, since)

	entries, err = a.stor.GetEntries(since, limit,
		func(e *common.Entry) bool {
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

func (a *ChangeAgent) handleGetChange(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	idStr := params.ByName("change")
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
		resp.Header().Set("Content-Type", jsonContent)
		marshalJSON(entry, resp)
	}
}

/*
Given a list of changes, prune it so that the actual result matches "since"
and "limit." The list may include entries before "since" and after "limit,"
and we use those entries to understand whether we are at the start and / or end
of the list.
*/
func pruneChanges(entries []common.Entry, since uint64, limit uint) ([]common.Entry, bool, bool) {
	pruned := entries
	atStart := true
	atEnd := true

	if len(entries) > 0 && entries[0].Index <= since {
		// There was an entry before "since", so we're not at the start
		pruned = entries[1:]
		atStart = false
	}

	if len(pruned) > int(limit) {
		// There was an entry after "limit," so we're not at the end
		pruned = pruned[:len(pruned)-1]
		atEnd = false
	}

	glog.V(2).Infof("pruneChanges(%d, %d, %d): %d %v %v",
		len(entries), since, limit, len(pruned), atStart, atEnd)

	return pruned, atStart, atEnd
}
