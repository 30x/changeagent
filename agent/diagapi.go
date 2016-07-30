package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/raft"
	"github.com/golang/gddo/httputil"
	"github.com/julienschmidt/httprouter"
	"github.com/mholt/binding"
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
	healthURI     = "/health"

	indentPrefix = ""
	indentSpace  = "  "
)

type healthReq struct {
	up string
}

func (h *healthReq) FieldMap(req *http.Request) binding.FieldMap {
	return binding.FieldMap{
		&h.up: binding.Field{
			Form:     "up",
			Required: true,
		},
	}
}

/*
RaftState represents the state of the Raft implementation and is used to generate
a JSON response.
*/
type RaftState struct {
	ID             string             `json:"id"`
	State          string             `json:"state"`
	Leader         string             `json:"leader"`
	Term           uint64             `json:"term"`
	NodeConfig     raft.NodeList      `json:"nodeConfig"`
	FirstIndex     uint64             `json:"firstIndex"`
	LastIndex      uint64             `json:"lastIndex"`
	AppliedIndex   uint64             `json:"appliedIndex"`
	CommittedIndex uint64             `json:"committedIndex"`
	PeerIndices    *map[string]uint64 `json:"peerIndices,omitempty"`
}

func (a *ChangeAgent) initDiagnosticAPI(prefix string) {
	a.router.GET(path.Join(prefix, "/"), a.handleRootCall)
	a.router.GET(prefix+baseURI, a.handleDiagRootCall)
	a.router.GET(prefix+idURI, a.handleIDCall)
	a.router.GET(prefix+raftStateURI, a.handleStateCall)
	a.router.GET(prefix+raftLeaderURI, a.handleLeaderCall)
	a.router.GET(prefix+raftURI, a.handleRaftInfo)
	a.router.GET(prefix+stackURI, handleStackCall)
	a.router.GET(prefix+memURI, handleMemoryCall)
	a.router.GET(prefix+cpuURI, handleCPUCall)
	a.router.GET(prefix+healthURI, a.handleHealthCheck)
	a.router.POST(prefix+healthURI, a.handleHealthUpdate)
	a.router.PUT(prefix+healthURI, a.handleHealthUpdate)
}

func (a *ChangeAgent) handleRootCall(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	links := make(map[string]string)
	// TODO convert links properly
	links["changes"] = a.makeLink(req, "/changes")
	links["diagnostics"] = a.makeLink(req, "/diagnostics")
	links["cluster"] = a.makeLink(req, "/cluster")
	links["config"] = a.makeLink(req, "/config")
	links["health"] = a.makeLink(req, "/health")

	contentType := isHTML(req)
	resp.Header().Set("Content-Type", contentType)

	if contentType == jsonContent {
		body, _ := json.MarshalIndent(&links, indentPrefix, indentSpace)
		resp.Write(body)
	} else {
		err := rootTemplate.Execute(resp, &links)
		if err != nil {
			writeError(resp, http.StatusInternalServerError, err)
		}
	}
}

func (a *ChangeAgent) handleDiagRootCall(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	o := make(map[string]interface{})
	// TODO convert links properly
	o["arch"] = runtime.GOARCH
	o["os"] = runtime.GOOS
	o["maxprocs"] = strconv.Itoa(runtime.GOMAXPROCS(-1))

	l := make(map[string]string)
	o["links"] = l
	l["id"] = a.makeLink(req, idURI)
	l["stack"] = a.makeLink(req, stackURI)
	l["raft"] = a.makeLink(req, raftURI)
	l["memory"] = a.makeLink(req, memURI)
	l["cpu"] = a.makeLink(req, cpuURI)

	contentType := isHTML(req)
	resp.Header().Set("Content-Type", contentType)

	if contentType == jsonContent {
		body, _ := json.MarshalIndent(&o, indentPrefix, indentSpace)
		resp.Write(body)
	} else {
		err := diagTemplate.Execute(resp, &o)
		if err != nil {
			writeError(resp, http.StatusInternalServerError, err)
		}
	}
}

func (a *ChangeAgent) handleIDCall(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	resp.Header().Set("Content-Type", plainTextContent)
	resp.Write([]byte(a.raft.MyID().String()))
}

func (a *ChangeAgent) handleStateCall(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	resp.Header().Set("Content-Type", plainTextContent)
	resp.Write([]byte(a.GetRaftState().String()))
}

func (a *ChangeAgent) handleLeaderCall(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	resp.Header().Set("Content-Type", plainTextContent)
	resp.Write([]byte(a.getLeaderID().String()))
}

func (a *ChangeAgent) handleRaftInfo(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	status := a.raft.GetRaftStatus()
	last, term := a.raft.GetLastIndex()
	first, _ := a.raft.GetFirstIndex()
	state := RaftState{
		ID:             a.raft.MyID().String(),
		State:          a.GetRaftState().String(),
		Leader:         a.getLeaderID().String(),
		Term:           term,
		NodeConfig:     a.raft.GetNodeConfig(),
		FirstIndex:     first,
		LastIndex:      last,
		AppliedIndex:   a.raft.GetLastApplied(),
		CommittedIndex: a.raft.GetCommitIndex(),
	}

	pis := make(map[string]uint64)
	if status.PeerIndices != nil {
		for pik, piv := range *status.PeerIndices {
			pis[pik.String()] = piv
		}
		state.PeerIndices = &pis
	}

	contentType := isHTML(req)
	resp.Header().Set("Content-Type", contentType)

	if contentType == jsonContent {
		body, _ := json.MarshalIndent(&state, indentPrefix, indentSpace)
		resp.Write(body)
	} else {
		err := diagRaftTemplate.Execute(resp, &state)
		if err != nil {
			writeError(resp, http.StatusInternalServerError, err)
		}
	}
}

func (a *ChangeAgent) getLeaderID() common.NodeID {
	if a.GetRaftState() == raft.Leader {
		return a.raft.MyID()
	}
	return a.raft.GetLeaderID()
}

func handleStackCall(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	stackBufLen := 64
	for {
		stackBuf := make([]byte, stackBufLen)
		stackLen := runtime.Stack(stackBuf, true)
		if stackLen == len(stackBuf) {
			// Must be truncated
			stackBufLen *= 2
		} else {
			resp.Header().Set("Content-Type", plainTextContent)
			resp.Write(stackBuf[:stackLen])
			return
		}
	}
}

func handleMemoryCall(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	s := make(map[string]uint64)
	s["alloc"] = stats.Alloc
	s["totalAlloc"] = stats.TotalAlloc
	s["sys"] = stats.Sys
	s["lookups"] = stats.Lookups
	s["mallocs"] = stats.Mallocs
	s["frees"] = stats.Frees

	contentType := isHTML(req)
	resp.Header().Set("Content-Type", contentType)

	if contentType == jsonContent {
		body, _ := json.MarshalIndent(&s, indentPrefix, indentSpace)
		resp.Write(body)
	} else {
		err := diagStatsTemplate.Execute(resp, &s)
		if err != nil {
			writeError(resp, http.StatusInternalServerError, err)
		}
	}
}

func handleCPUCall(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	s := make(map[string]int64)
	s["gomaxprocs"] = int64(runtime.GOMAXPROCS(-1))
	s["numcpu"] = int64(runtime.NumCPU())
	s["numcgocall"] = runtime.NumCgoCall()
	s["numgoroutine"] = int64(runtime.NumGoroutine())

	contentType := isHTML(req)
	resp.Header().Set("Content-Type", contentType)

	if contentType == jsonContent {
		body, _ := json.MarshalIndent(&s, indentPrefix, indentSpace)
		resp.Write(body)
	} else {
		err := diagStatsTemplate.Execute(resp, &s)
		if err != nil {
			writeError(resp, http.StatusInternalServerError, err)
		}
	}
}

func (a *ChangeAgent) handleHealthCheck(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	resp.Header().Set("Content-Type", "text/plain")
	markedDown := atomic.LoadInt32(&a.markedDown)
	if markedDown == 0 {
		// This will hang if the main loop is stuck somehow, so it's a good
		// thing to call here.
		a.raft.GetRaftStatus()
		resp.Write([]byte("ok"))
	} else {
		resp.WriteHeader(http.StatusServiceUnavailable)
		resp.Write([]byte("marked down"))
	}
}

func (a *ChangeAgent) handleHealthUpdate(
	resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	healthReq := &healthReq{}
	defer req.Body.Close()
	bindingErr := binding.Bind(req, healthReq)
	if bindingErr.Handle(resp) {
		return
	}
	var down int32
	if !strings.EqualFold(healthReq.up, "true") {
		down = 1
	}
	atomic.StoreInt32(&a.markedDown, down)
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

func isHTML(req *http.Request) string {
	return httputil.NegotiateContentType(req,
		[]string{jsonContent, htmlContent}, jsonContent)
}
