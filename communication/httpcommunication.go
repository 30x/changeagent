package communication

import (
  "bytes"
  "fmt"
  "time"
  "io/ioutil"
  "net/http"
  "github.com/golang/protobuf/proto"
  "github.com/gin-gonic/gin"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

const (
  ContentType = "application/changeagent+protobuf"
  RequestVoteUri = "/raft/requestvote"
  AppendUri = "/raft/append"
  RequestTimeout = 10 * time.Second
)

var httpClient *http.Client = &http.Client{
  Timeout: RequestTimeout,
}

type HttpCommunication struct {
  raft Raft
  discovery discovery.Discovery
}

func StartHttpCommunication(api *gin.Engine,
                            discovery discovery.Discovery) (*HttpCommunication, error) {
  comm := HttpCommunication{
    discovery: discovery,
  }
  api.POST(RequestVoteUri, comm.handleRequestVote)
  api.POST(AppendUri, comm.handleAppend)
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

  log.Infof("Got back %d", resp.StatusCode)
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
    ne := EntryPb{
      Term: &e.Term,
      Data: e.Data,
    }
    reqPb.Entries = append(reqPb.Entries, &ne)
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

  log.Infof("Got back %d", resp.StatusCode)
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


func (h *HttpCommunication) handleRequestVote(c *gin.Context) {
  defer c.Request.Body.Close()
  if c.ContentType() != ContentType {
    c.AbortWithStatus(415)
    return
  }
  body, err := ioutil.ReadAll(c.Request.Body)
  if err != nil {
    c.AbortWithError(500, err)
    return
  }

  var reqpb VoteRequestPb
  err = proto.Unmarshal(body, &reqpb)
  if err != nil {
    c.AbortWithError(400, err)
    return
  }

  req := VoteRequest{
    Term: reqpb.GetTerm(),
    CandidateId: reqpb.GetCandidateId(),
    LastLogIndex: reqpb.GetLastLogIndex(),
    LastLogTerm: reqpb.GetLastLogTerm(),
  }

  resp, err := h.raft.RequestVote(&req)
  if err != nil {
    c.AbortWithError(500, err)
    return
  }

  nodeId := h.raft.MyId()
  respPb := VoteResponsePb{
    NodeId: &nodeId,
    Term: &resp.Term,
    VoteGranted: &resp.VoteGranted,
  }

  respBody, err := proto.Marshal(&respPb)
  if err != nil {
    c.AbortWithError(500, err)
    return
  }

  c.Data(200, ContentType, respBody)
}

func (h *HttpCommunication) handleAppend(c *gin.Context) {
  defer c.Request.Body.Close()
  if c.ContentType() != ContentType {
    c.AbortWithStatus(415)
    return
  }
  body, err := ioutil.ReadAll(c.Request.Body)
  if err != nil {
    c.AbortWithError(500, err)
    return
  }

  var reqpb AppendRequestPb
  err = proto.Unmarshal(body, &reqpb)
  if err != nil {
    c.AbortWithError(400, err)
    return
  }

  req := AppendRequest{
    Term: reqpb.GetTerm(),
    LeaderId: reqpb.GetLeaderId(),
    PrevLogIndex: reqpb.GetPrevLogIndex(),
    PrevLogTerm: reqpb.GetPrevLogTerm(),
    LeaderCommit: reqpb.GetLeaderCommit(),
  }
  for _, e := range reqpb.GetEntries() {
    ne := storage.Entry{
      Term: e.GetTerm(),
      Data: e.GetData(),
    }
    req.Entries = append(req.Entries, ne)
  }

  resp, err := h.raft.Append(&req)
  if err != nil {
    c.AbortWithError(500, err)
    return
  }

  respPb := AppendResponsePb{
    Term: &resp.Term,
    Success: &resp.Success,
  }

  respBody, err := proto.Marshal(&respPb)
  if err != nil {
    c.AbortWithError(500, err)
    return
  }

  c.Data(200, ContentType, respBody)
}
