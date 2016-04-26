package communication

import (
  "bytes"
  "errors"
  "fmt"
  "time"
  "io/ioutil"
  "net/http"
  "github.com/golang/glog"
  "github.com/golang/protobuf/proto"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

const (
  ContentType = "application/changeagent+protobuf"
  RequestVoteURI = "/raft/requestvote"
  AppendURI = "/raft/append"
  ProposeURI = "/raft/propose"
  DiscoveryURI = "/raft/id"
  RequestTimeout = 10 * time.Second
)

var httpClient = &http.Client{
  Timeout: RequestTimeout,
}

type HTTPCommunication struct {
  raft Raft
}

func StartHTTPCommunication(mux *http.ServeMux) (*HTTPCommunication, error) {
  comm := HTTPCommunication{}
  mux.HandleFunc(RequestVoteURI, comm.handleRequestVote)
  mux.HandleFunc(AppendURI, comm.handleAppend)
  mux.HandleFunc(ProposeURI, comm.handlePropose)
  mux.HandleFunc(DiscoveryURI, comm.handleDiscovery)
  return &comm, nil
}

func (h *HTTPCommunication) SetRaft(raft Raft) {
  h.raft = raft
}

func (h *HTTPCommunication) Discover(addr string) (uint64, error) {
  uri := fmt.Sprintf("http://%s%s", addr, DiscoveryURI)

  resp, err := httpClient.Get(uri)
  if err != nil { return 0, err }

  defer resp.Body.Close()
  if resp.StatusCode != 200 {
    return 0, fmt.Errorf("HTTP error getting node ID: %d %s", resp.StatusCode, resp.Status)
  }

  respBody, err := ioutil.ReadAll(resp.Body)
  if err != nil { return 0, err }

  var respPb DiscoveryResponsePb
  err = proto.Unmarshal(respBody, &respPb)
  if err != nil { return 0, err }

  return respPb.GetNodeId(), nil
}

func (h *HTTPCommunication) RequestVote(addr string, req VoteRequest, ch chan<- VoteResponse) {
  go h.sendVoteRequest(addr, req, ch)
}

func (h *HTTPCommunication) sendVoteRequest(addr string, req VoteRequest, ch chan<- VoteResponse) {
  uri := fmt.Sprintf("http://%s%s", addr, RequestVoteURI)

  reqPb := VoteRequestPb{
    Term: &req.Term,
    CandidateId: &req.CandidateID,
    LastLogIndex: &req.LastLogIndex,
    LastLogTerm: &req.LastLogTerm,
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

  var respPb VoteResponsePb
  err = proto.Unmarshal(respBody, &respPb)
  if err != nil {
    vr := VoteResponse{Error: err}
    ch <- vr
    return
  }

  voteResp := VoteResponse{
    NodeID: respPb.GetNodeId(),
    NodeAddress: addr,
    Term: respPb.GetTerm(),
    VoteGranted: respPb.GetVoteGranted(),
  }
  ch <- voteResp
}

func (h *HTTPCommunication) Append(addr string, req AppendRequest) (AppendResponse, error) {
  uri := fmt.Sprintf("http://%s%s", addr, AppendURI)

  reqPb := AppendRequestPb{
    Term: &req.Term,
    LeaderId: &req.LeaderID,
    PrevLogIndex: &req.PrevLogIndex,
    PrevLogTerm: &req.PrevLogTerm,
    LeaderCommit: &req.LeaderCommit,
  }
  for _, e := range req.Entries {
    ebytes, err := storage.EncodeEntry(&e)
    if err != nil { return DefaultAppendResponse, err }
    reqPb.Entries = append(reqPb.Entries, ebytes)
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

  var respPb AppendResponsePb
  err = proto.Unmarshal(respBody, &respPb)
  if err != nil {
    return DefaultAppendResponse, err
  }

  appResp := AppendResponse{
    Term: respPb.GetTerm(),
    Success: respPb.GetSuccess(),
  }

  return appResp, nil
}

func (h *HTTPCommunication) Propose(addr string, e storage.Entry) (ProposalResponse, error) {
  uri := fmt.Sprintf("http://%s%s", addr, ProposeURI)

  reqBody, err := storage.EncodeEntry(&e)
  if err != nil {
    return DefaultProposalResponse, err
  }

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

  var respPb ProposalResponsePb
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

func (h *HTTPCommunication) handleRequestVote(resp http.ResponseWriter, req *http.Request) {
  defer req.Body.Close()

  if req.Method != "POST" {
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

  var reqpb VoteRequestPb
  err = proto.Unmarshal(body, &reqpb)
  if err != nil {
    resp.WriteHeader(http.StatusBadRequest)
    return
  }

  voteReq := VoteRequest{
    Term: reqpb.GetTerm(),
    CandidateID: reqpb.GetCandidateId(),
    LastLogIndex: reqpb.GetLastLogIndex(),
    LastLogTerm: reqpb.GetLastLogTerm(),
  }

  voteResp, err := h.raft.RequestVote(voteReq)
  if err != nil {
    resp.WriteHeader(http.StatusBadRequest)
    return
  }

  nodeID := h.raft.MyID()
  respPb := VoteResponsePb{
    NodeId: &nodeID,
    Term: &voteResp.Term,
    VoteGranted: &voteResp.VoteGranted,
  }

  respBody, err := proto.Marshal(&respPb)
  if err != nil {
    resp.WriteHeader(http.StatusInternalServerError)
    return
  }

  resp.Header().Set(http.CanonicalHeaderKey("content-type"), ContentType)
  resp.Write(respBody)
}

func (h *HTTPCommunication) handleAppend(resp http.ResponseWriter, req *http.Request) {
  defer req.Body.Close()

  if req.Method != "POST" {
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

  var reqpb AppendRequestPb
  err = proto.Unmarshal(body, &reqpb)
  if err != nil {
    resp.WriteHeader(http.StatusBadRequest)
    return
  }

  apReq := AppendRequest{
    Term: reqpb.GetTerm(),
    LeaderID: reqpb.GetLeaderId(),
    PrevLogIndex: reqpb.GetPrevLogIndex(),
    PrevLogTerm: reqpb.GetPrevLogTerm(),
    LeaderCommit: reqpb.GetLeaderCommit(),
  }
  for _, e := range reqpb.GetEntries() {
    newEntry, err := storage.DecodeEntry(e)
    if err != nil {
      resp.WriteHeader(http.StatusBadRequest)
      return
    }

    apReq.Entries = append(apReq.Entries, *newEntry)
  }

  appResp, err := h.raft.Append(apReq)
  if err != nil {
    resp.WriteHeader(http.StatusInternalServerError)
    return
  }

  respPb := AppendResponsePb{
    Term: &appResp.Term,
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

func (h *HTTPCommunication) handlePropose(resp http.ResponseWriter, req *http.Request) {
  defer req.Body.Close()

  if req.Method != "POST" {
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

  newEntry, err := storage.DecodeEntry(body)
  if err != nil {
    resp.WriteHeader(http.StatusBadRequest)
    return
  }

  newIndex, err := h.raft.Propose(*newEntry)
  if err != nil {
    glog.V(1).Infof("Error in proposal: %s", err)
    resp.WriteHeader(http.StatusInternalServerError)
    return
  }

  respPb := ProposalResponsePb{
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

func (h *HTTPCommunication) handleDiscovery(resp http.ResponseWriter, req *http.Request) {
  defer req.Body.Close()

  if req.Method != "GET" {
    resp.WriteHeader(http.StatusMethodNotAllowed)
    return
  }

  nodeID := h.raft.MyID()
  respPb := DiscoveryResponsePb{
    NodeId: &nodeID,
  }

  respBody, err := proto.Marshal(&respPb)
  if err != nil {
    resp.WriteHeader(http.StatusInternalServerError)
    return
  }

  resp.Header().Set(http.CanonicalHeaderKey("content-type"), ContentType)
  resp.Write(respBody)
}
