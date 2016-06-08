package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"

	"github.com/30x/changeagent/raft"
)

const (
	baseURI       = "/diagnostics"
	stackURI      = baseURI + "/stack"
	idURI         = baseURI + "/id"
	raftURI       = baseURI + "/raft"
	raftStateURI  = raftURI + "/state"
	raftLeaderURI = raftURI + "/leader"
)

/*
RaftState represents the state of the Raft implementation and is used to generate
a JSON response.
*/
type RaftState struct {
	State  string `json:"state"`
	Leader string `json:"leader"`
}

func (a *ChangeAgent) initDiagnosticAPI() {
	a.router.HandleFunc("/", a.handleRootCall).Methods("GET")
	a.router.HandleFunc(baseURI, a.handleDiagRootCall).Methods("GET")
	a.router.HandleFunc(idURI, a.handleIDCall).Methods("GET")
	a.router.HandleFunc(raftStateURI, a.handleStateCall).Methods("GET")
	a.router.HandleFunc(raftLeaderURI, a.handleLeaderCall).Methods("GET")
	a.router.HandleFunc(raftURI, a.handleRaftInfo).Methods("GET")
	a.router.HandleFunc(stackURI, handleStackCall).Methods("GET")
}

func (a *ChangeAgent) handleRootCall(resp http.ResponseWriter, req *http.Request) {
	links := make(map[string]string)
	// TODO convert links properly
	links["changes"] = makeLink(req, "/changes")
	links["diagnostics"] = makeLink(req, "/diagnostics")

	body, _ := json.Marshal(&links)

	resp.Header().Set("Content-Type", jsonContent)
	resp.Write(body)
}

func (a *ChangeAgent) handleDiagRootCall(resp http.ResponseWriter, req *http.Request) {
	links := make(map[string]string)
	// TODO convert links properly
	links["id"] = makeLink(req, idURI)
	links["stack"] = makeLink(req, stackURI)
	links["raft"] = makeLink(req, raftURI)
	body, _ := json.Marshal(&links)

	resp.Header().Set("Content-Type", jsonContent)
	resp.Write(body)
}

func (a *ChangeAgent) handleIDCall(resp http.ResponseWriter, req *http.Request) {
	msg := fmt.Sprintf("%d\n", a.raft.MyID())
	resp.Header().Set("Content-Type", plainTextContent)
	resp.Write([]byte(msg))
}

func (a *ChangeAgent) handleStateCall(resp http.ResponseWriter, req *http.Request) {
	resp.Header().Set("Content-Type", plainTextContent)
	resp.Write([]byte(a.GetRaftState().String()))
}

func (a *ChangeAgent) handleLeaderCall(resp http.ResponseWriter, req *http.Request) {
	resp.Header().Set("Content-Type", plainTextContent)
	resp.Write([]byte(fmt.Sprintf("%d", a.getLeaderID())))
}

func (a *ChangeAgent) handleRaftInfo(resp http.ResponseWriter, req *http.Request) {
	state := RaftState{
		State:  a.GetRaftState().String(),
		Leader: strconv.FormatUint(a.getLeaderID(), 10),
	}

	body, _ := json.Marshal(&state)

	resp.Header().Set("Content-Type", jsonContent)
	resp.Write(body)
}

func (a *ChangeAgent) getLeaderID() uint64 {
	if a.GetRaftState() == raft.Leader {
		return a.raft.MyID()
	}
	return a.raft.GetLeaderID()
}

func handleStackCall(resp http.ResponseWriter, req *http.Request) {
	stackBufLen := 64
	for {
		stackBuf := make([]byte, stackBufLen)
		stackLen := runtime.Stack(stackBuf, true)
		if stackLen == len(stackBuf) {
			// Must be truncated
			stackBufLen *= 2
		} else {
			resp.Header().Set("Content-Type", plainTextContent)
			resp.Write(stackBuf)
			return
		}
	}
}

func makeLink(req *http.Request, path string) string {
	var proto string
	if req.TLS == nil {
		proto = "http"
	} else {
		proto = "https"
	}

	return fmt.Sprintf("%s://%s%s", proto, req.Host, path)
}
