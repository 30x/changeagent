package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"runtime"
	"strconv"

	"github.com/30x/changeagent/discovery"
	"github.com/30x/changeagent/raft"
)

const (
	baseURI       = "/diagnostics"
	stackURI      = baseURI + "/stack"
	idURI         = baseURI + "/id"
	raftURI       = baseURI + "/raft"
	memURI        = baseURI + "/memory"
	cpuURI        = baseURI + "/cpu"
	raftStateURI  = raftURI + "/state"
	raftLeaderURI = raftURI + "/leader"
)

/*
RaftState represents the state of the Raft implementation and is used to generate
a JSON response.
*/
type RaftState struct {
	State      string                `json:"state"`
	Leader     string                `json:"leader"`
	NodeConfig *discovery.NodeConfig `json:"nodeConfig"`
}

func (a *ChangeAgent) initDiagnosticAPI(prefix string) {
	a.router.HandleFunc(path.Join(prefix, "/"), a.handleRootCall).Methods("GET")
	a.router.HandleFunc(prefix+baseURI, a.handleDiagRootCall).Methods("GET")
	a.router.HandleFunc(prefix+idURI, a.handleIDCall).Methods("GET")
	a.router.HandleFunc(prefix+raftStateURI, a.handleStateCall).Methods("GET")
	a.router.HandleFunc(prefix+raftLeaderURI, a.handleLeaderCall).Methods("GET")
	a.router.HandleFunc(prefix+raftURI, a.handleRaftInfo).Methods("GET")
	a.router.HandleFunc(prefix+stackURI, handleStackCall).Methods("GET")
	a.router.HandleFunc(prefix+memURI, handleMemoryCall).Methods("GET")
	a.router.HandleFunc(prefix+cpuURI, handleCPUCall).Methods("GET")
}

func (a *ChangeAgent) handleRootCall(resp http.ResponseWriter, req *http.Request) {
	links := make(map[string]string)
	// TODO convert links properly
	links["changes"] = a.makeLink(req, "/changes")
	links["diagnostics"] = a.makeLink(req, "/diagnostics")
	links["hooks"] = a.makeLink(req, "/hooks")

	body, _ := json.Marshal(&links)

	resp.Header().Set("Content-Type", jsonContent)
	resp.Write(body)
}

func (a *ChangeAgent) handleDiagRootCall(resp http.ResponseWriter, req *http.Request) {
	o := make(map[string]string)
	// TODO convert links properly
	o["arch"] = runtime.GOARCH
	o["os"] = runtime.GOOS
	o["maxprocs"] = strconv.Itoa(runtime.GOMAXPROCS(-1))
	o["id"] = a.makeLink(req, idURI)
	o["stack"] = a.makeLink(req, stackURI)
	o["raft"] = a.makeLink(req, raftURI)
	o["memory"] = a.makeLink(req, memURI)
	o["cpu"] = a.makeLink(req, cpuURI)
	body, _ := json.Marshal(&o)

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
		State:      a.GetRaftState().String(),
		Leader:     strconv.FormatUint(a.getLeaderID(), 10),
		NodeConfig: a.raft.GetNodeConfig(),
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

func handleMemoryCall(resp http.ResponseWriter, req *http.Request) {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	s := make(map[string]uint64)
	s["alloc"] = stats.Alloc
	s["totalAlloc"] = stats.TotalAlloc
	s["sys"] = stats.Sys
	s["lookups"] = stats.Lookups
	s["mallocs"] = stats.Mallocs
	s["frees"] = stats.Frees

	body, _ := json.Marshal(&s)
	resp.Header().Set("Content-Type", jsonContent)
	resp.Write(body)
}

func handleCPUCall(resp http.ResponseWriter, req *http.Request) {
	s := make(map[string]string)
	s["gomaxprocs"] = strconv.Itoa(runtime.GOMAXPROCS(-1))
	s["numcpu"] = strconv.Itoa(runtime.NumCPU())
	s["numcgocall"] = strconv.FormatInt(runtime.NumCgoCall(), 10)
	s["numgoroutine"] = strconv.Itoa(runtime.NumGoroutine())

	body, _ := json.Marshal(&s)
	resp.Header().Set("Content-Type", jsonContent)
	resp.Write(body)
}

func (a *ChangeAgent) makeLink(req *http.Request, path string) string {
	var proto string
	if req.TLS == nil {
		proto = "http"
	} else {
		proto = "https"
	}

	return fmt.Sprintf("%s://%s%s%s", proto, req.Host, a.uriPrefix, path)
}
