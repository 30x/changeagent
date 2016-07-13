package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/30x/changeagent/raft"
	"github.com/mholt/binding"
)

const (
	configURI = "/config"
)

// ConfigData is the JSON structure for configuration.
type ConfigData struct {
	MinPurgeRecords  uint32 `json:"minPurgeRecords"`
	MinPurgeDuration string `json:"minPurgeDuration"`
}

// FieldMap tells the binding package how to validate our input
func (c *ConfigData) FieldMap(req *http.Request) binding.FieldMap {
	return binding.FieldMap{
		&c.MinPurgeRecords:  "minPurgeRecords",
		&c.MinPurgeDuration: "minPurgeDuration",
	}
}

func (a *ChangeAgent) initConfigAPI(prefix string) {
	a.router.HandleFunc(prefix+configURI, a.handleGetRaftConfig).Methods("GET")
	a.router.HandleFunc(prefix+configURI, a.handleSetRaftConfig).Methods("PUT")
}

func (a *ChangeAgent) handleGetRaftConfig(resp http.ResponseWriter, req *http.Request) {
	cfg := a.raft.GetRaftConfig()
	cd := ConfigData{
		MinPurgeRecords:  cfg.MinPurgeRecords,
		MinPurgeDuration: cfg.MinPurgeDuration.String(),
	}
	resp.Header().Set("Content-Type", jsonContent)
	buf, err := json.MarshalIndent(&cd, indentPrefix, indentSpace)
	if err == nil {
		resp.Write(buf)
	} else {
		writeError(resp, http.StatusInternalServerError, err)
	}
}

func (a *ChangeAgent) handleSetRaftConfig(resp http.ResponseWriter, req *http.Request) {
	var data ConfigData
	defer req.Body.Close()
	bindingErr := binding.Bind(req, &data)
	if bindingErr.Handle(resp) {
		return
	}

	purgeDer, err := time.ParseDuration(data.MinPurgeDuration)
	if err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	cfg := raft.Config{
		MinPurgeDuration: purgeDer,
		MinPurgeRecords:  data.MinPurgeRecords,
	}

	ix, err := a.raft.UpdateRaftConfiguration(&cfg)
	if err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	err = a.waitForCommit(ix)
	if err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}
}
