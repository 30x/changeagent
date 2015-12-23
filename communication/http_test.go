package communication

import (
  "bytes"
  "fmt"
  "testing"
  "net"
  "net/http"
  "time"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

var expectedEntries []storage.Entry

func TestRaftCalls(t *testing.T) {
  anyPort := &net.TCPAddr{}
  listener, err := net.ListenTCP("tcp", anyPort)
  if err != nil { t.Fatal("Can't listen on TCP port") }
  defer listener.Close()

  _, port, err := net.SplitHostPort(listener.Addr().String())
  if err != nil { t.Fatal("Error parsing port") }
  t.Logf("Listening on %s", port)
  addrs := []string{
    fmt.Sprintf("localhost:%s", port),
  }

  discovery := discovery.CreateStaticDiscovery(addrs)
  testRaft := makeTestRaft(t)
  mux := http.NewServeMux()
  comm, err := StartHttpCommunication(mux, discovery)
  if err != nil { t.Fatalf("Error starting raft: %v", err) }
  comm.SetRaft(testRaft)
  go http.Serve(listener, mux)
  if err != nil { t.Fatal("Error listening on http") }
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
  expectedEntries = nil

  aresp, err := comm.Append(1, &ar)
  if err != nil { t.Fatalf("Error from voteResponse: %v", resp.Error) }
  if aresp.Error != nil { t.Fatalf("Error from voteResponse: %v", resp.Error) }
  if aresp.Term != 1 { t.Fatalf("Expected term 1, got %d", resp.Term) }
  if !aresp.Success { t.Fatal("Expected success") }

  ar = AppendRequest{
    Term: 2,
    LeaderId: 1,
  }
  e := storage.Entry{
    Index: 1,
    Term: 2,
    Data: []byte("Hello!"),
  }
  ar.Entries = append(ar.Entries, e)
  e2 := storage.Entry{
    Index: 2,
    Term: 3,
    Data: []byte("Goodbye!"),
  }
  ar.Entries = append(ar.Entries, e2)
  expectedEntries = ar.Entries

  aresp, err = comm.Append(1, &ar)
  if err != nil { t.Fatalf("Error from voteResponse: %v", resp.Error) }
  if aresp.Error != nil { t.Fatalf("Error from voteResponse: %v", aresp.Error) }
  if aresp.Term != 2 { t.Fatalf("Expected term 2, got %d", resp.Term) }
  if aresp.Success { t.Fatal("Expected not success") }
}

type testRaft struct {
  t *testing.T
}

func makeTestRaft(t *testing.T) *testRaft {
  return &testRaft{
    t: t,
  }
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
  r.t.Logf("Expected: %v", expectedEntries)
  r.t.Logf("Got: %v", req.Entries)
  for i, e := range(req.Entries) {
    if e.Index != expectedEntries[i].Index {
      r.t.Fatalf("%d: Expected index %d and got %d", i, expectedEntries[i].Index, e.Index)
    }
    if e.Term != expectedEntries[i].Term {
      r.t.Fatalf("Expected term %d and got %d", expectedEntries[i].Term, e.Term)
    }
    if !bytes.Equal(e.Data, expectedEntries[i].Data) {
      r.t.Fatal("Bytes do not match")
    }
  }
  return &vr, nil
}
