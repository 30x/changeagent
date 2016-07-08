package communication

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
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

type httpCommunication struct {
	raft       Raft
	httpClient *http.Client
	listener   net.Listener
	httpProto  string
}

/*
StartHTTPCommunication creates an instance of the Communication interface that
runs over HTTP. It uses the default HTTP client over regular (insecure) HTTP
and connects to an existing HTTP listener.
*/
func StartHTTPCommunication(mux *http.ServeMux) (Communication, error) {
	comm := &httpCommunication{
		httpClient: &http.Client{
			Timeout: requestTimeout,
		},
		httpProto: "http",
	}
	comm.initMux(mux)
	return comm, nil
}

/*
StartSeparateCommunication starts listening for communication on a separate
port. It will open a new TCP listener on the specified port, and otherwise
works the same way.
*/
func StartSeparateCommunication(port int) (Communication, error) {
	addr := &net.TCPAddr{
		Port: port,
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	comm := httpCommunication{
		httpClient: &http.Client{
			Timeout: requestTimeout,
		},
		httpProto: "http",
		listener:  listener,
	}
	comm.initMux(mux)

	glog.Infof("Listening for cluster communications on port %d", comm.Port())

	go http.Serve(listener, mux)
	return &comm, nil
}

/*
StartSecureCommunication uses a separate port and also sets up encryption and
authentication using TLS. All three of the key, certificate, and CA file
must be specified. Communications are made using the specified key and certificate
for both client-side and server-side authentication. Verification is provided
against the specified CA file.

In order for this to work, the specified certificate must include the
"server_cert" and "usr_cert" options. Verification is always done manually
against the specified CA list. However, the "CA" is not checked.
This simplifies setup for most clusters on internal networks.
*/
func StartSecureCommunication(port int, key, cert, cas string) (Communication, error) {
	if key == "" || cert == "" || cas == "" {
		return nil, errors.New("All three of key, cert, and CA file must be specified")
	}
	keyPair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	certPool, err := loadCertPool(cas)
	if err != nil {
		return nil, err
	}

	// Set up cluster comms so that we only accept clients that have a TLS key
	// that was signed by the specified set of certificates.
	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{keyPair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
	}

	listener, err := tls.Listen("tcp", net.JoinHostPort("", strconv.Itoa(port)), serverCfg)
	if err != nil {
		return nil, err
	}

	// Set up cluster comms so that we only initiate communications with servers
	// that have a TLS key signed by the specifed set of certificates.
	clientCfg := &tls.Config{
		Certificates: []tls.Certificate{keyPair},
		// We are doing this because built-in hostname validation in Go's TLS package
		// makes it very hard to manage any clusters. Since we will validate the CA
		// we don't need to do this.
		InsecureSkipVerify: true,
	}

	mux := http.NewServeMux()
	comm := httpCommunication{
		httpClient: &http.Client{
			Timeout: requestTimeout,
			// This transport will verify the server cert.
			Transport: createVerifyingTransport(clientCfg, certPool),
		},
		httpProto: "https",
		listener:  listener,
	}
	comm.initMux(mux)

	glog.Infof("Listening for TLS cluster communications on port %d",
		comm.Port())

	go http.Serve(listener, mux)
	return &comm, nil
}

func (h *httpCommunication) initMux(mux *http.ServeMux) {
	mux.HandleFunc(requestVoteURI, h.handleRequestVote)
	mux.HandleFunc(appendURI, h.handleAppend)
	mux.HandleFunc(proposeURI, h.handlePropose)
	mux.HandleFunc(discoveryURI, h.handleDiscovery)
	mux.HandleFunc(joinURI, h.handleJoin)
}

func (h *httpCommunication) Close() {
	if h.listener != nil {
		h.listener.Close()
	}
}

func (h *httpCommunication) Port() int {
	if h.listener == nil {
		return 0
	}
	_, portStr, _ := net.SplitHostPort(h.listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	return port
}

func (h *httpCommunication) SetRaft(raft Raft) {
	h.raft = raft
}

func (h *httpCommunication) Discover(addr string) (common.NodeID, error) {
	uri := fmt.Sprintf("%s://%s%s", h.httpProto, addr, discoveryURI)

	resp, err := h.httpClient.Get(uri)
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
	uri := fmt.Sprintf("%s://%s%s", h.httpProto, addr, requestVoteURI)

	reqPb := protobufs.VoteRequestPb{
		Term:         proto.Uint64(req.Term),
		CandidateId:  proto.Uint64(uint64(req.CandidateID)),
		LastLogIndex: proto.Uint64(req.LastLogIndex),
		LastLogTerm:  proto.Uint64(req.LastLogTerm),
		ClusterId:    proto.Uint64(uint64(req.ClusterID)),
	}

	var respPb protobufs.VoteResponsePb
	err := h.transportClientRequest(uri, &reqPb, &respPb)
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
	uri := fmt.Sprintf("%s://%s%s", h.httpProto, addr, appendURI)

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

	var respPb protobufs.AppendResponsePb
	err := h.transportClientRequest(uri, &reqPb, &respPb)
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
	uri := fmt.Sprintf("%s://%s%s", h.httpProto, addr, proposeURI)

	reqPb := e.EncodePb()
	var respPb protobufs.ProposalResponsePb

	err := h.transportClientRequest(uri, &reqPb, &respPb)
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
	uri := fmt.Sprintf("%s://%s%s", h.httpProto, addr, joinURI)

	joinPb := protobufs.JoinRequestPb{
		ClusterId: proto.Uint64(uint64(req.ClusterID)),
		Last:      proto.Bool(req.Last),
	}
	for _, e := range req.Entries {
		epb := e.EncodePb()
		joinPb.Entries = append(joinPb.Entries, &epb)
	}

	var respPb protobufs.ProposalResponsePb
	err := h.transportClientRequest(uri, &joinPb, &respPb)
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
	var reqpb protobufs.VoteRequestPb

	if !h.readRequestBody(http.MethodPost, resp, req, &reqpb) {
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

	h.writeResponseBody(&respPb, resp)
}

func (h *httpCommunication) handleAppend(resp http.ResponseWriter, req *http.Request) {
	var reqpb protobufs.AppendRequestPb

	if !h.readRequestBody(http.MethodPost, resp, req, &reqpb) {
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

	h.writeResponseBody(&respPb, resp)
}

func (h *httpCommunication) handlePropose(resp http.ResponseWriter, req *http.Request) {
	var entryPb protobufs.EntryPb
	if !h.readRequestBody(http.MethodPost, resp, req, &entryPb) {
		return
	}

	newEntry := common.DecodeEntryFromPb(entryPb)

	newIndex, err := h.raft.Propose(newEntry)
	if err != nil {
		glog.V(2).Infof("Error in proposal: %s", err)
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

	h.writeResponseBody(&respPb, resp)
}

func (h *httpCommunication) handleJoin(resp http.ResponseWriter, req *http.Request) {
	var joinPb protobufs.JoinRequestPb
	if !h.readRequestBody(http.MethodPost, resp, req, &joinPb) {
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

	h.writeResponseBody(&respPb, resp)
}

func (h *httpCommunication) handleDiscovery(resp http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	if req.Method != http.MethodGet {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	nodeID := h.raft.MyID()
	respPb := protobufs.DiscoveryResponsePb{
		NodeId: proto.Uint64(uint64(nodeID)),
	}

	h.writeResponseBody(&respPb, resp)
}

func (h *httpCommunication) transportClientRequest(
	uri string, reqMsg, respMsg proto.Message) error {

	reqBody, err := proto.Marshal(reqMsg)
	if err != nil {
		return err
	}

	resp, err := h.httpClient.Post(uri, ContentType, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP status %d %s", resp.StatusCode, resp.Status)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return proto.Unmarshal(respBody, respMsg)
}

func (h *httpCommunication) readRequestBody(
	method string, resp http.ResponseWriter, req *http.Request,
	reqPb proto.Message) bool {
	defer req.Body.Close()

	if req.Method != method {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return false
	}

	if req.Header.Get("Content-Type") != ContentType {
		resp.WriteHeader(http.StatusUnsupportedMediaType)
		return false
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return false
	}

	err = proto.Unmarshal(body, reqPb)
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return false
	}
	return true
}

func (h *httpCommunication) writeResponseBody(respPb proto.Message, resp http.ResponseWriter) {
	respBody, err := proto.Marshal(respPb)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp.Header().Set("Content-Type", ContentType)
	resp.Write(respBody)
}
