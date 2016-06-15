package main

import (
	"io/ioutil"
	"net/http"

	"github.com/30x/changeagent/hooks"
)

const (
	hooksURI = "/hooks"
)

func (a *ChangeAgent) initHooksAPI(prefix string) {
	a.router.HandleFunc(prefix+hooksURI, a.handleGetHooks).Methods("GET")
	a.router.HandleFunc(prefix+hooksURI, a.handleSetHooks).Methods("POST")
	a.router.HandleFunc(prefix+hooksURI, a.handleClearHooks).Methods("DELETE")
}

func (a *ChangeAgent) handleGetHooks(resp http.ResponseWriter, req *http.Request) {
	h := a.raft.GetWebHooks()
	resp.Header().Set("Content-Type", jsonContent)
	if h == nil {
		resp.Write([]byte("[]"))
	} else {
		resp.Write(hooks.EncodeHooksJSON(h))
	}
}

func (a *ChangeAgent) handleSetHooks(resp http.ResponseWriter, req *http.Request) {
	if !isJSON(resp, req) {
		return
	}

	defer req.Body.Close()
	bod, err := ioutil.ReadAll(req.Body)
	if err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	h, err := hooks.DecodeHooksJSON(bod)
	if err != nil {
		writeError(resp, http.StatusBadRequest, err)
		return
	}

	a.updateWebHooks(resp, h)
}

func (a *ChangeAgent) handleClearHooks(resp http.ResponseWriter, req *http.Request) {
	a.updateWebHooks(resp, []hooks.WebHook{})
}

func (a *ChangeAgent) updateWebHooks(resp http.ResponseWriter, h []hooks.WebHook) {
	ix, err := a.raft.UpdateWebHooks(h)
	if err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	err = a.waitForCommit(ix)
	if err == nil {
		resp.Header().Set("Content-Type", jsonContent)
		resp.Write(hooks.EncodeHooksJSON(h))
	} else {
		writeError(resp, http.StatusServiceUnavailable, err)
	}
}
