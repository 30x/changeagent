package communication

import (
  "bytes"
  "errors"
  "fmt"
  "flag"
  "testing"
  "net"
  "net/http"
  "sync"
  "time"
  "github.com/satori/go.uuid"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

var expectedEntries []storage.Entry
var expectedLock = sync.Mutex{}

var tenant uuid.UUID = uuid.NewV4()
var collection uuid.UUID = uuid.NewV4()

func TestRaftCalls(t *testing.T) {
  flag.Set("logtostderr", "true")
  flag.Set("v", "2")
  flag.Parse()

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
  var comm Communication
  comm, err = StartHttpCommunication(mux, discovery)
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
    Timestamp: time.Now(),
    Tenant: &tenant,
    Collection: &collection,
    Key: "baz",
    Data: []byte("Hello!"),
  }
  ar.Entries = append(ar.Entries, e)
  e2 := storage.Entry{
    Index: 2,
    Term: 3,
    Timestamp: time.Now(),
    Tenant: &tenant,
    Collection: &collection,
    Key: "baz",
    Data: []byte("Goodbye!"),
  }
  ar.Entries = append(ar.Entries, e2)

  expectedLock.Lock()
  expectedEntries = ar.Entries
  expectedLock.Unlock()

  aresp, err = comm.Append(1, &ar)
  if err != nil { t.Fatalf("Error from voteResponse: %v", err) }
  if aresp.Error != nil { t.Fatalf("Error from voteResponse: %v", aresp.Error) }
  if aresp.Term != 2 { t.Fatalf("Expected term 2, got %d", resp.Term) }
  if aresp.Success { t.Fatal("Expected not success") }

  e3 := storage.Entry{
    Timestamp: time.Now(),
    Index: 3,
    Term: 3,
    Tenant: &tenant,
    Collection: &collection,
    Key: "baz",
    Data: []byte("Hello, World!"),
  }

  expectedLock.Lock()
  expectedEntries = make([]storage.Entry, 1)
  expectedEntries[0] = e3
  expectedLock.Unlock()

  presp, err := comm.Propose(1, &e3)
  if err != nil { t.Fatalf("Error from propose: %v", err) }
  if presp.Error != nil { t.Fatalf("Error from propose: %v", presp.Error) }
  if presp.NewIndex == 0 { t.Fatal("Expected a non-zero index") }
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
  expectedLock.Lock()
  defer expectedLock.Unlock()

  vr := AppendResponse{
    Term: req.Term,
    Success: req.Term == 1,
  }
  r.t.Logf("Expected: %v", expectedEntries)
  r.t.Logf("Got: %v", req.Entries)
  for i, e := range(req.Entries) {
    ee := expectedEntries[i]
    if e.Index != ee.Index {
      r.t.Fatalf("%d: Expected index %d and got %d", i, expectedEntries[i].Index, e.Index)
    }
    if e.Term != ee.Term {
      r.t.Fatal("Terms do not match")
    }
    if e.Timestamp != ee.Timestamp {
      r.t.Fatal("Timestamps do not match")
    }
    if !uuid.Equal(*e.Tenant, *ee.Tenant) {
      r.t.Fatal("Tenants do not match")
    }
    if !uuid.Equal(*e.Collection, *ee.Collection) {
      r.t.Fatal("Collections do not match")
    }
    if e.Key != ee.Key {
     r.t.Fatal("Keys do not match")
    }
    if !bytes.Equal(e.Data, expectedEntries[i].Data) {
      r.t.Fatal("Bytes do not match")
    }
  }
  return &vr, nil
}

func (r *testRaft) Propose(e *storage.Entry) (uint64, error) {
  expectedLock.Lock()
  defer expectedLock.Unlock()

  ee := expectedEntries[0]
  if ee.Index != e.Index { return 0, errors.New("Incorrect index") }
  if ee.Term != e.Term { return 0, errors.New("Incorrect term") }
  if ee.Timestamp != e.Timestamp { return 0, errors.New("Incorrect timestamp") }
  if !uuid.Equal(*ee.Tenant, *e.Tenant) { return 0, errors.New("Incorrect tenant") }
  if !uuid.Equal(*ee.Collection, *e.Collection) { return 0, errors.New("Incorrect collection") }
  if ee.Key != e.Key { return 0, errors.New("Incorrect key") }
  if !bytes.Equal(ee.Data, e.Data) { return 0, errors.New("Incorrect data") }

  return 123, nil
}
