package main

import (
	"io/ioutil"
	"net/http"
)

const (
	configURI = "/config"
)

func (a *ChangeAgent) initConfigAPI(prefix string) {
	a.router.HandleFunc(prefix+configURI, a.handleGetRaftConfig).Methods("GET")
	a.router.HandleFunc(prefix+configURI, a.handleSetRaftConfig).Methods("PUT", "POST")
}

func (a *ChangeAgent) handleGetRaftConfig(resp http.ResponseWriter, req *http.Request) {
	cfg := a.raft.GetRaftConfig()
	cfgBytes, err := cfg.Store()
	if err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	resp.Header().Set("Content-Type", yamlContent)
	resp.Write(cfgBytes)
}

func (a *ChangeAgent) handleSetRaftConfig(resp http.ResponseWriter, req *http.Request) {
	contentType := req.Header.Get("Content-Type")
	if !yamlContentRe.MatchString(contentType) {
		resp.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}

	defer req.Body.Close()
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		writeError(resp, http.StatusBadRequest, err)
		return
	}

	ix, err := a.raft.UpdateConfiguration(body)
	if err != nil {
		writeError(resp, http.StatusBadRequest, err)
		return
	}

	err = a.raft.WaitForCommit(ix)
	if err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	a.handleGetRaftConfig(resp, req)
}
