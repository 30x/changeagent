package communication

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/protobufs"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
)

const (
	// ContentType is the MIME type that this module will use for all of its
	// HTTP requests and responses.
	ContentType    = "application/changeagent+protobuf"
	requestVoteURI = "/raft/requestvote"
	appendURI      = "/raft/append"
	proposeURI     = "/raft/propose"
	discoveryURI   = "/raft/id"
	joinURI        = "/raft/join"
	requestTimeout = 10 * time.Second
)

var httpClient = &http.Client{
	Timeout: requestTimeout,
}

type httpCommunication struct {
	raft Raft
}

/*
StartHTTPCommunication creates an instance of the Communication interface that
runs over HTTP. Requests and responses are made in the form of encoded
protobufs.
*/
func StartHTTPCommunication(mux *http.ServeMux) (Communication, error) {
	comm := httpCommunication{}
	mux.HandleFunc(requestVoteURI, comm.handleRequestVote)
	mux.HandleFunc(appendURI, comm.handleAppend)
	mux.HandleFunc(proposeURI, comm.handlePropose)
	mux.HandleFunc(discoveryURI, comm.handleDiscovery)
	mux.HandleFunc(joinURI, comm.handleJoin)
	return &comm, nil
}

func (h *httpCommunication) SetRaft(raft Raft) {
	h.raft = raft
}

func (h *httpCommunication) Discover(addr string) (common.NodeID, error) {
	uri := fmt.Sprintf("http://%s%s", addr, discoveryURI)

	resp, err := httpClient.Get(uri)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("HTTP error getting node ID: %d %s", resp.StatusCode, resp.Status)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var respPb protobufs.DiscoveryResponsePb
	err = proto.Unmarshal(respBody, &respPb)
	if err != nil {
		return 0, err
	}

	return common.NodeID(respPb.GetNodeId()), nil
}

func (h *httpCommunication) RequestVote(addr string, req VoteRequest, ch chan<- VoteResponse) {
	go h.sendVoteRequest(addr, req, ch)
}

func (h *httpCommunication) sendVoteRequest(addr string, req VoteRequest, ch chan<- VoteResponse) {
	uri := fmt.Sprintf("http://%s%s", addr, requestVoteURI)

	reqPb := protobufs.VoteRequestPb{
		Term:         proto.Uint64(req.Term),
		CandidateId:  proto.Uint64(uint64(req.CandidateID)),
		LastLogIndex: proto.Uint64(req.LastLogIndex),
		LastLogTerm:  proto.Uint64(req.LastLogTerm),
		ClusterId:    proto.Uint64(uint64(req.ClusterID)),
	}
	reqBody, err := proto.Marshal(&reqPb)
	if err != nil {
		vr := VoteResponse{Error: err}
		ch <- vr
		return
	}

	resp, err := httpClient.Post(uri, ContentType, bytes.NewReader(reqBody))
	if err != nil {
		vr := VoteResponse{Error: err}
		ch <- vr
		return
	}
	defer resp.Body.Close()

	glog.V(2).Infof("Got back %d", resp.StatusCode)
	if resp.StatusCode != 200 {
		vr := VoteResponse{
			Error: fmt.Errorf("HTTP status %d %s", resp.StatusCode, resp.Status),
		}
		ch <- vr
		return
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		vr := VoteResponse{Error: err}
		ch <- vr
		return
	}

	var respPb protobufs.VoteResponsePb
	err = proto.Unmarshal(respBody, &respPb)
	if err != nil {
		vr := VoteResponse{Error: err}
		ch <- vr
		return
	}

	voteResp := VoteResponse{
		NodeID:      common.NodeID(respPb.GetNodeId()),
		NodeAddress: addr,
		Term:        respPb.GetTerm(),
		VoteGranted: respPb.GetVoteGranted(),
	}
	ch <- voteResp
}

func (h *httpCommunication) Append(addr string, req AppendRequest) (AppendResponse, error) {
	uri := fmt.Sprintf("http://%s%s", addr, appendURI)

	reqPb := protobufs.AppendRequestPb{
		Term:         proto.Uint64(req.Term),
		LeaderId:     proto.Uint64(uint64(req.LeaderID)),
		PrevLogIndex: proto.Uint64(req.PrevLogIndex),
		PrevLogTerm:  proto.Uint64(req.PrevLogTerm),
		LeaderCommit: proto.Uint64(req.LeaderCommit),
	}
	for _, e := range req.Entries {
		pb := e.EncodePb()
		reqPb.Entries = append(reqPb.Entries, &pb)
	}

	reqBody, err := proto.Marshal(&reqPb)
	if err != nil {
		return DefaultAppendResponse, err
	}

	resp, err := httpClient.Post(uri, ContentType, bytes.NewReader(reqBody))
	if err != nil {
		return DefaultAppendResponse, err
	}
	defer resp.Body.Close()

	glog.V(2).Infof("Got back %d", resp.StatusCode)
	if resp.StatusCode != 200 {
		return DefaultAppendResponse, fmt.Errorf("HTTP status %d %s", resp.StatusCode, resp.Status)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return DefaultAppendResponse, err
	}

	var respPb protobufs.AppendResponsePb
	err = proto.Unmarshal(respBody, &respPb)
	if err != nil {
		return DefaultAppendResponse, err
	}

	appResp := AppendResponse{
		Term:    respPb.GetTerm(),
		Success: respPb.GetSuccess(),
	}

	return appResp, nil
}

func (h *httpCommunication) Propose(addr string, e *common.Entry) (ProposalResponse, error) {
	uri := fmt.Sprintf("http://%s%s", addr, proposeURI)

	reqBody := e.Encode()

	resp, err := httpClient.Post(uri, ContentType, bytes.NewReader(reqBody))
	if err != nil {
		return DefaultProposalResponse, err
	}
	defer resp.Body.Close()

	glog.V(2).Infof("Got back %d", resp.StatusCode)
	if resp.StatusCode != 200 {
		return DefaultProposalResponse, fmt.Errorf("HTTP status %d %s", resp.StatusCode, resp.Status)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return DefaultProposalResponse, err
	}

	var respPb protobufs.ProposalResponsePb
	err = proto.Unmarshal(respBody, &respPb)
	if err != nil {
		return DefaultProposalResponse, err
	}

	appResp := ProposalResponse{
		NewIndex: respPb.GetNewIndex(),
	}
	if respPb.GetError() != "" {
		appResp.Error = errors.New(respPb.GetError())
	}

	return appResp, nil
}

func (h *httpCommunication) Join(addr string, req JoinRequest) (ProposalResponse, error) {
	uri := fmt.Sprintf("http://%s%s", addr, joinURI)

	joinPb := protobufs.JoinRequestPb{
		ClusterId: proto.Uint64(uint64(req.ClusterID)),
		Last:      proto.Bool(req.Last),
	}
	for _, e := range req.Entries {
		epb := e.EncodePb()
		joinPb.Entries = append(joinPb.Entries, &epb)
	}

	reqBody, err := proto.Marshal(&joinPb)
	if err != nil {
		return DefaultProposalResponse, err
	}

	resp, err := httpClient.Post(uri, ContentType, bytes.NewReader(reqBody))
	if err != nil {
		return DefaultProposalResponse, err
	}
	defer resp.Body.Close()

	glog.V(2).Infof("Got back %d on join", resp.StatusCode)
	if resp.StatusCode != 200 {
		return DefaultProposalResponse, fmt.Errorf("HTTP status %d %s", resp.StatusCode, resp.Status)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return DefaultProposalResponse, err
	}

	var respPb protobufs.ProposalResponsePb
	err = proto.Unmarshal(respBody, &respPb)
	if err != nil {
		return DefaultProposalResponse, err
	}

	appResp := ProposalResponse{
		NewIndex: respPb.GetNewIndex(),
	}
	if respPb.GetError() != "" {
		appResp.Error = errors.New(respPb.GetError())
	}

	return appResp, nil
}

func (h *httpCommunication) handleRequestVote(resp http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if req.Header.Get(http.CanonicalHeaderKey("content-type")) != ContentType {
		resp.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	var reqpb protobufs.VoteRequestPb
	err = proto.Unmarshal(body, &reqpb)
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	voteReq := VoteRequest{
		Term:         reqpb.GetTerm(),
		CandidateID:  common.NodeID(reqpb.GetCandidateId()),
		LastLogIndex: reqpb.GetLastLogIndex(),
		LastLogTerm:  reqpb.GetLastLogTerm(),
		ClusterID:    common.NodeID(reqpb.GetClusterId()),
	}

	voteResp, err := h.raft.RequestVote(voteReq)
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	nodeID := h.raft.MyID()
	respPb := protobufs.VoteResponsePb{
		NodeId:      proto.Uint64(uint64(nodeID)),
		Term:        proto.Uint64(voteResp.Term),
		VoteGranted: proto.Bool(voteResp.VoteGranted),
	}

	respBody, err := proto.Marshal(&respPb)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp.Header().Set(http.CanonicalHeaderKey("content-type"), ContentType)
	resp.Write(respBody)
}

func (h *httpCommunication) handleAppend(resp http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if req.Header.Get(http.CanonicalHeaderKey("content-type")) != ContentType {
		resp.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	var reqpb protobufs.AppendRequestPb
	err = proto.Unmarshal(body, &reqpb)
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	apReq := AppendRequest{
		Term:         reqpb.GetTerm(),
		LeaderID:     common.NodeID(reqpb.GetLeaderId()),
		PrevLogIndex: reqpb.GetPrevLogIndex(),
		PrevLogTerm:  reqpb.GetPrevLogTerm(),
		LeaderCommit: reqpb.GetLeaderCommit(),
	}
	for _, e := range reqpb.GetEntries() {
		newEntry := common.DecodeEntryFromPb(*e)
		apReq.Entries = append(apReq.Entries, *newEntry)
	}

	appResp, err := h.raft.Append(apReq)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	respPb := protobufs.AppendResponsePb{
		Term:    &appResp.Term,
		Success: &appResp.Success,
	}

	respBody, err := proto.Marshal(&respPb)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp.Header().Set(http.CanonicalHeaderKey("content-type"), ContentType)
	resp.Write(respBody)
}

func (h *httpCommunication) handlePropose(resp http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if req.Header.Get(http.CanonicalHeaderKey("content-type")) != ContentType {
		resp.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	newEntry, err := common.DecodeEntry(body)
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	newIndex, err := h.raft.Propose(newEntry)
	if err != nil {
		glog.V(1).Infof("Error in proposal: %s", err)
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	respPb := protobufs.ProposalResponsePb{
		NewIndex: &newIndex,
	}
	if err != nil {
		errMsg := err.Error()
		respPb.Error = &errMsg
	}

	respBody, err := proto.Marshal(&respPb)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp.Header().Set(http.CanonicalHeaderKey("content-type"), ContentType)
	resp.Write(respBody)
}

func (h *httpCommunication) handleJoin(resp http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if req.Header.Get(http.CanonicalHeaderKey("content-type")) != ContentType {
		resp.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	var joinPb protobufs.JoinRequestPb
	err = proto.Unmarshal(body, &joinPb)
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	joinReq := JoinRequest{
		ClusterID: common.NodeID(joinPb.GetClusterId()),
		Last:      joinPb.GetLast(),
	}
	for _, e := range joinPb.GetEntries() {
		joinReq.Entries = append(joinReq.Entries, *common.DecodeEntryFromPb(*e))
	}

	newIndex, err := h.raft.Join(joinReq)

	respPb := protobufs.ProposalResponsePb{
		NewIndex: proto.Uint64(newIndex),
	}
	if err != nil {
		errMsg := err.Error()
		respPb.Error = &errMsg
	}

	respBody, err := proto.Marshal(&respPb)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp.Header().Set(http.CanonicalHeaderKey("content-type"), ContentType)
	resp.Write(respBody)
}

func (h *httpCommunication) handleDiscovery(resp http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	if req.Method != "GET" {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	nodeID := h.raft.MyID()
	respPb := protobufs.DiscoveryResponsePb{
		NodeId: proto.Uint64(uint64(nodeID)),
	}

	respBody, err := proto.Marshal(&respPb)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp.Header().Set(http.CanonicalHeaderKey("content-type"), ContentType)
	resp.Write(respBody)
}
