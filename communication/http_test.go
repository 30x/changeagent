package communication

import (
  "testing"
  "time"
  "github.com/gin-gonic/gin"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

func TestRaftCalls(t *testing.T) {
  addrs := make([]string, 1)
  addrs[0] = "localhost:33333"
  discovery := discovery.CreateStaticDiscovery(addrs)
  testRaft := makeTestRaft()
  api := gin.Default()
  comm, err := StartHttpCommunication(api, discovery)
  if err != nil { t.Fatalf("Error starting raft: %v", err) }
  comm.SetRaft(testRaft)
  go api.Run(":33333")
  time.Sleep(time.Second)

  req := VoteRequest{
    Term: 1,
    CandidateId: 1,
  }
  ch := make(chan *VoteResponse, 1)

  comm.RequestVote(1, &req, ch)
  resp := <- ch
  if resp.Error != nil { t.Fatalf("Error from voteResponse: %v", resp.Error) }
  if resp.Term != 1 { t.Fatalf("Expected term 1, got %d", resp.Term) }
  if resp.NodeId != 1 { t.Fatalf("Expected node is 1, got %d", resp.NodeId) }
  if !resp.VoteGranted { t.Fatal("Expected vote to be granted") }

  ar := AppendRequest{
    Term: 1,
    LeaderId: 1,
  }
  ach := make(chan *AppendResponse, 1)

  comm.Append(1, &ar, ach)
  aresp := <- ach
  if aresp.Error != nil { t.Fatalf("Error from voteResponse: %v", resp.Error) }
  if aresp.Term != 1 { t.Fatalf("Expected term 1, got %d", resp.Term) }
  if !aresp.Success { t.Fatal("Expected success") }

  ar = AppendRequest{
    Term: 2,
    LeaderId: 1,
  }
  e := storage.Entry{
    Term: 2,
  }
  ar.Entries = append(ar.Entries, e)
  e.Term = 3
  ar.Entries = append(ar.Entries, e)

  comm.Append(1, &ar, ach)
  aresp = <- ach
  if aresp.Error != nil { t.Fatalf("Error from voteResponse: %v", aresp.Error) }
  if aresp.Term != 2 { t.Fatalf("Expected term 2, got %d", resp.Term) }
  if aresp.Success { t.Fatal("Expected not success") }
}

type testRaft struct {
}

func makeTestRaft() *testRaft {
  return &testRaft{}
}

func (r *testRaft) MyId() uint64 {
  return 1
}

func (r *testRaft) RequestVote(req *VoteRequest) (*VoteResponse, error) {
  vr := VoteResponse{
    Term: req.Term,
    VoteGranted: req.Term == 1,
  }
  return &vr, nil
}

func (r *testRaft) Append(req *AppendRequest) (*AppendResponse, error) {
  vr := AppendResponse{
    Term: req.Term,
    Success: req.Term == 1,
  }
  return &vr, nil
}
