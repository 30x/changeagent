package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/30x/changeagent/raft"
	"github.com/mholt/binding"
)

const (
	configURI = "/config"
)

// ConfigData is the JSON structure for configuration.
// We unfortunately copy that so that the internal "time.Duration"
// (for internal convenience) can be converted to a string
// (for external convenience)
type ConfigData struct {
	MinPurgeRecords  uint32 `json:"minPurgeRecords"`
	MinPurgeDuration string `json:"minPurgeDuration"`
	HeartbeatTimeout string `json:"heartbeatTimeout"`
	ElectionTimeout  string `json:"electionTimeout"`
}

// FieldMap tells the binding package how to validate our input
func (c *ConfigData) FieldMap(req *http.Request) binding.FieldMap {
	return binding.FieldMap{
		&c.MinPurgeRecords:  "minPurgeRecords",
		&c.MinPurgeDuration: "minPurgeDuration",
		&c.HeartbeatTimeout: "heartbeatTimeout",
		&c.ElectionTimeout:  "electionTimeout",
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
		HeartbeatTimeout: cfg.HeartbeatTimeout.String(),
		ElectionTimeout:  cfg.ElectionTimeout.String(),
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

	var err error
	var purgeDur time.Duration
	if data.MinPurgeDuration != "" {
		purgeDur, err = time.ParseDuration(data.MinPurgeDuration)
		if err != nil {
			writeError(resp, http.StatusBadRequest,
				fmt.Errorf("Invalid purge duration %s: %s", data.MinPurgeDuration, err))
			return
		}
	}

	var hbDur time.Duration
	if data.HeartbeatTimeout != "" {
		hbDur, err = time.ParseDuration(data.HeartbeatTimeout)
		if err != nil {
			writeError(resp, http.StatusBadRequest,
				fmt.Errorf("Invalid heartbeat timeout %s: %s", data.HeartbeatTimeout, err))
			return
		}
	}

	var elDur time.Duration
	if data.ElectionTimeout != "" {
		elDur, err = time.ParseDuration(data.ElectionTimeout)
		if err != nil {
			writeError(resp, http.StatusBadRequest,
				fmt.Errorf("Invalid election timeout %s: %s", data.ElectionTimeout, err))
			return
		}
	}

	cfg := raft.Config{
		MinPurgeDuration: purgeDur,
		MinPurgeRecords:  data.MinPurgeRecords,
		ElectionTimeout:  elDur,
		HeartbeatTimeout: hbDur,
	}

	ix, err := a.raft.UpdateRaftConfiguration(cfg)
	if err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	err = a.waitForCommit(ix)
	if err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}

	a.handleGetRaftConfig(resp, req)
}
