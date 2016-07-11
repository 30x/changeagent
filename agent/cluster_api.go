package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/30x/changeagent/common"
	"github.com/gorilla/mux"
	"github.com/mholt/binding"
)

const (
	clusterURI = "/cluster"
	membersURI = clusterURI + "/members"
	memberURI  = membersURI + "/{id}"
)

/*
ClusterInfo is a JSON response for cluster information.
*/
type ClusterInfo struct {
	ID      string            `json:"clusterId"`
	Members map[string]string `json:"members,_omitempty"`
}

/*
addressInfo is a request structure used to create a new member.
*/
type addressInfo struct {
	address string
}

/*
FieldMap is used by the "binding" package to parse the request body.
*/
func (a *addressInfo) FieldMap(req *http.Request) binding.FieldMap {
	return binding.FieldMap{
		&a.address: binding.Field{
			Form:     "address",
			Required: true,
		},
	}
}

func (a *ChangeAgent) initClusterAPI(prefix string) {
	a.router.HandleFunc(prefix+clusterURI, a.handleGetCluster).Methods("GET")
	a.router.HandleFunc(prefix+membersURI, a.handleGetClusterMembers).Methods("GET")
	a.router.HandleFunc(prefix+memberURI, a.handleGetClusterMember).Methods("GET")
	a.router.HandleFunc(prefix+memberURI, a.handleRemoveClusterMember).Methods("DELETE")
	a.router.HandleFunc(prefix+membersURI, a.handleAddClusterMember).Methods("POST")
}

func (a *ChangeAgent) handleGetCluster(resp http.ResponseWriter, req *http.Request) {
	id := a.raft.GetClusterID()
	info := ClusterInfo{
		ID:      id.String(),
		Members: a.getClusterMembers(),
	}

	bod, _ := json.MarshalIndent(&info, indentPrefix, indentSpace)

	resp.Header().Set("Content-Type", jsonContent)
	resp.Write(bod)
}

func (a *ChangeAgent) handleGetClusterMembers(resp http.ResponseWriter, req *http.Request) {
	members := a.getClusterMembers()
	bod, _ := json.MarshalIndent(&members, indentPrefix, indentSpace)

	resp.Header().Set("Content-Type", jsonContent)
	resp.Write(bod)
}

func (a *ChangeAgent) handleGetClusterMember(resp http.ResponseWriter, req *http.Request) {
	idStr := mux.Vars(req)["id"]
	id := common.ParseNodeID(idStr)
	if id == 0 {
		writeError(resp, http.StatusBadRequest, fmt.Errorf("Invalid Node ID: %s", idStr))
		return
	}

	node := a.raft.GetNodeConfig().GetNode(id)
	if node == nil {
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	resp.Header().Set("Content-Type", plainTextContent)
	resp.Write([]byte(node.Address))
}

func (a *ChangeAgent) handleAddClusterMember(resp http.ResponseWriter, req *http.Request) {
	addrInfo := &addressInfo{}
	bindingErr := binding.Bind(req, addrInfo)
	if bindingErr.Handle(resp) {
		return
	}

	var err error
	if a.raft.GetClusterID() == 0 {
		// We are not part of a cluster, so initialize it.
		err = a.raft.InitializeCluster(addrInfo.address)
	} else {
		err = a.raft.AddNode(addrInfo.address)
	}

	if err == nil {
		a.handleGetClusterMembers(resp, req)
	} else {
		writeError(resp, http.StatusBadRequest, err)
	}
}

func (a *ChangeAgent) handleRemoveClusterMember(resp http.ResponseWriter, req *http.Request) {
	idStr := mux.Vars(req)["id"]
	id := common.ParseNodeID(idStr)
	if id == 0 {
		writeError(resp, http.StatusBadRequest, fmt.Errorf("Invalid Node ID: %s", idStr))
		return
	}

	node := a.raft.GetNodeConfig().GetNode(id)
	if node == nil {
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	var err error
	forceCmd := req.URL.Query().Get("force")
	if strings.ToLower(forceCmd) == "true" {
		err = a.raft.RemoveNodeForcibly(node.NodeID)
	} else {
		err = a.raft.RemoveNode(node.NodeID)
	}

	if err == nil {
		a.handleGetClusterMembers(resp, req)
	} else {
		writeError(resp, http.StatusInternalServerError, err)
	}
}

func (a *ChangeAgent) getClusterMembers() map[string]string {
	m := make(map[string]string)
	cfg := a.raft.GetNodeConfig()
	if cfg == nil {
		return m
	}

	nodes := cfg.GetUniqueNodes()
	for _, n := range nodes {
		m[n.NodeID.String()] = n.Address
	}

	return m
}
