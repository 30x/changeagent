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
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

const (
  ContentType = "application/changeagent+protobuf"
  RequestVoteUri = "/raft/requestvote"
  AppendUri = "/raft/append"
  ProposeUri = "/raft/propose"
  RequestTimeout = 10 * time.Second
)

var httpClient *http.Client = &http.Client{
  Timeout: RequestTimeout,
}

type HttpCommunication struct {
  raft Raft
  discovery discovery.Discovery
}

func StartHttpCommunication(mux *http.ServeMux,
                            discovery discovery.Discovery) (*HttpCommunication, error) {
  comm := HttpCommunication{
    discovery: discovery,
  }
  mux.HandleFunc(RequestVoteUri, comm.handleRequestVote)
  mux.HandleFunc(AppendUri, comm.handleAppend)
  mux.HandleFunc(ProposeUri, comm.handlePropose)
  return &comm, nil
}

func (h *HttpCommunication) SetRaft(raft Raft) {
  h.raft = raft
}

func (h *HttpCommunication) RequestVote(id uint64, req *VoteRequest, ch chan *VoteResponse) {
  addr := h.discovery.GetAddress(id)
  if addr == "" {
    vr := VoteResponse{
      Error: fmt.Errorf("Invalid peer ID %d", id),
    }
    ch <- &vr
    return
  }

  go h.sendVoteRequest(addr, req, ch)
}

func (h *HttpCommunication) sendVoteRequest(addr string, req *VoteRequest, ch chan *VoteResponse) {
  uri := fmt.Sprintf("http://%s%s", addr, RequestVoteUri)

  reqPb := VoteRequestPb{
    Term: &req.Term,
    CandidateId: &req.CandidateId,
    LastLogIndex: &req.LastLogIndex,
    LastLogTerm: &req.LastLogTerm,
  }
  reqBody, err := proto.Marshal(&reqPb)
  if err != nil {
    vr := VoteResponse{Error: err}
    ch <- &vr
    return
  }

  resp, err := httpClient.Post(uri, ContentType, bytes.NewReader(reqBody))
  if err != nil {
    vr := VoteResponse{Error: err}
    ch <- &vr
    return
  }
  defer resp.Body.Close()

  glog.V(2).Infof("Got back %d", resp.StatusCode)
  if resp.StatusCode != 200 {
    vr := VoteResponse{
      Error: fmt.Errorf("HTTP status %d %s", resp.StatusCode, resp.Status),
    }
    ch <- &vr
    return
  }

  respBody, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    vr := VoteResponse{Error: err}
    ch <- &vr
    return
  }

  var respPb VoteResponsePb
  err = proto.Unmarshal(respBody, &respPb)
  if err != nil {
    vr := VoteResponse{Error: err}
    ch <- &vr
    return
  }

  voteResp := VoteResponse{
    NodeId: respPb.GetNodeId(),
    Term: respPb.GetTerm(),
    VoteGranted: respPb.GetVoteGranted(),
  }
  ch <- &voteResp
}

func (h *HttpCommunication) Append(id uint64, req *AppendRequest) (*AppendResponse, error) {
  addr := h.discovery.GetAddress(id)
  if addr == "" {
    return nil, fmt.Errorf("Invalid peer ID %d", id)
  }

  uri := fmt.Sprintf("http://%s%s", addr, AppendUri)

  reqPb := AppendRequestPb{
    Term: &req.Term,
    LeaderId: &req.LeaderId,
    PrevLogIndex: &req.PrevLogIndex,
    PrevLogTerm: &req.PrevLogTerm,
    LeaderCommit: &req.LeaderCommit,
  }
  for _, e := range req.Entries {
    ebytes, err := storage.EncodeEntry(&e)
    if err != nil { return nil, err }
    reqPb.Entries = append(reqPb.Entries, ebytes)
  }

  reqBody, err := proto.Marshal(&reqPb)
  if err != nil {
    return nil, err
  }

  resp, err := httpClient.Post(uri, ContentType, bytes.NewReader(reqBody))
  if err != nil {
    return nil, err
  }
  defer resp.Body.Close()

  glog.V(2).Infof("Got back %d", resp.StatusCode)
  if resp.StatusCode != 200 {
    return nil, fmt.Errorf("HTTP status %d %s", resp.StatusCode, resp.Status)
  }

  respBody, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return nil, err
  }

  var respPb AppendResponsePb
  err = proto.Unmarshal(respBody, &respPb)
  if err != nil {
    return nil, err
  }

  appResp := &AppendResponse{
    Term: respPb.GetTerm(),
    Success: respPb.GetSuccess(),
  }
  return appResp, nil
}

func (h *HttpCommunication) Propose(id uint64, e *storage.Entry) (*ProposalResponse, error) {
  addr := h.discovery.GetAddress(id)
  if addr == "" {
    return nil, fmt.Errorf("Invalid peer ID %d", id)
  }

  uri := fmt.Sprintf("http://%s%s", addr, ProposeUri)

  reqBody, err := storage.EncodeEntry(e)
  if err != nil {
    return nil, err
  }

  resp, err := httpClient.Post(uri, ContentType, bytes.NewReader(reqBody))
  if err != nil {
    return nil, err
  }
  defer resp.Body.Close()

  glog.V(2).Infof("Got back %d", resp.StatusCode)
  if resp.StatusCode != 200 {
    return nil, fmt.Errorf("HTTP status %d %s", resp.StatusCode, resp.Status)
  }

  respBody, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return nil, err
  }

  var respPb ProposalResponsePb
  err = proto.Unmarshal(respBody, &respPb)
  if err != nil {
    return nil, err
  }

  appResp := &ProposalResponse{
    NewIndex: respPb.GetNewIndex(),
  }
  if respPb.GetError() != "" {
    appResp.Error = errors.New(respPb.GetError())
  }
  return appResp, nil
}

func (h *HttpCommunication) handleRequestVote(resp http.ResponseWriter, req *http.Request) {
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
    CandidateId: reqpb.GetCandidateId(),
    LastLogIndex: reqpb.GetLastLogIndex(),
    LastLogTerm: reqpb.GetLastLogTerm(),
  }

  voteResp, err := h.raft.RequestVote(&voteReq)
  if err != nil {
    resp.WriteHeader(http.StatusBadRequest)
    return
  }

  nodeId := h.raft.MyId()
  respPb := VoteResponsePb{
    NodeId: &nodeId,
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

func (h *HttpCommunication) handleAppend(resp http.ResponseWriter, req *http.Request) {
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
    LeaderId: reqpb.GetLeaderId(),
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

  appResp, err := h.raft.Append(&apReq)
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

func (h *HttpCommunication) handlePropose(resp http.ResponseWriter, req *http.Request) {
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

  newIndex, err := h.raft.Propose(newEntry)
  if err != nil {
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
