package raft

import (
  "fmt"
  "os"
  "net"
  "net/http"
  "path"
  "testing"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

const (
  DataDir = "./rafttestdata"
  PreserveDatabases = false
  DebugMode = true
)

var testRafts []*RaftImpl
var testListener []*net.TCPListener
var testStates []*TestStateMachine
var unitTestRaft *RaftImpl
var unitTestListener *net.TCPListener

func TestMain(m *testing.M) {
  os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
  os.MkdirAll(DataDir, 0777)
  log.InitDebug(DebugMode)

  // Create three TCP listeners -- we'll use them for a cluster
  anyPort := &net.TCPAddr{}
  var addrs []string
  for li := 0; li < 3; li++ {
    listener, err := net.ListenTCP("tcp4", anyPort)
    if err != nil { panic("Can't listen on a TCP port") }
    _, port, err := net.SplitHostPort(listener.Addr().String())
    if err != nil { panic("Invalid listen address") }
    addrs = append(addrs, fmt.Sprintf("localhost:%s", port))
    testListener = append(testListener, listener)
    newState := createTestState()
    testStates = append(testStates, newState)
  }
  disco := discovery.CreateStaticDiscovery(addrs)

  // Create one more for unit tests
  unitTestListener, err := net.ListenTCP("tcp4", anyPort)
  if err != nil { panic("Can't listen on a TCP port") }
  _, port, err := net.SplitHostPort(unitTestListener.Addr().String())
  if err != nil { panic("Invalid listen address") }
  unitAddr := []string{fmt.Sprintf("localhost:%s", port)}
  unitDisco := discovery.CreateStaticDiscovery(unitAddr)

  raft1, err := startRaft(1, disco, testListener[0], path.Join(DataDir, "test1"), testStates[0])
  if err != nil {
    fmt.Printf("Error starting raft 1: %v", err)
    return 2
  }
  testRafts = append(testRafts, raft1)
  defer cleanRaft(raft1, testListener[0])

  raft2, err := startRaft(2, disco, testListener[1], path.Join(DataDir, "test2"), testStates[1])
  if err != nil {
    fmt.Printf("Error starting raft 2: %v", err)
    return 3
  }
  testRafts = append(testRafts, raft2)
  defer cleanRaft(raft2, testListener[1])

  raft3, err := startRaft(3, disco, testListener[2], path.Join(DataDir, "test3"), testStates[2])
  if err != nil {
    fmt.Printf("Error starting raft 3: %v", err)
    return 4
  }
  testRafts = append(testRafts, raft3)
  defer cleanRaft(raft3, testListener[2])

  unitTestRaft, err = startRaft(1, unitDisco, unitTestListener, path.Join(DataDir, "unit"), createTestState())
  if err != nil {
    fmt.Printf("Error starting unit test raft: %v", err)
    return 4
  }
  defer cleanRaft(unitTestRaft, unitTestListener)
  initUnitTests(unitTestRaft)

  return m.Run()
}

func startRaft(id uint64, disco discovery.Discovery, listener *net.TCPListener, dir string, state StateMachine) (*RaftImpl, error) {
  mux := http.NewServeMux()
  comm, err := communication.StartHttpCommunication(mux, disco)
  if err != nil { return nil, err }
  stor, err := storage.CreateSqliteStorage(dir)
  if err != nil { return nil, err }

  raft, err := StartRaft(id, comm, disco, stor, state)
  if err != nil { return nil, err }
  comm.SetRaft(raft)
  go http.Serve(listener, mux)

  return raft, nil
}

func cleanRaft(raft *RaftImpl, l *net.TCPListener) {
  raft.Close()
  raft.stor.Close()
  if !PreserveDatabases {
    raft.stor.Delete()
  }
  l.Close()
}

// Set up the state machine so that we can properly do some unit tests
func initUnitTests(raft *RaftImpl) {
  raft.setFollowerOnly(true)

  ar := &communication.AppendRequest{
    Term: 2,
    LeaderId: 1,
    PrevLogIndex: 0,
    PrevLogTerm: 0,
    LeaderCommit: 1,
    Entries: []storage.Entry{
      storage.Entry{
        Index: 1,
        Term: 1,
      },
      storage.Entry{
        Index: 2,
        Term: 2,
      },
      storage.Entry{
        Index: 3,
        Term: 2,
      },
    },
  }
  resp, err := raft.Append(ar)
  if err != nil {
    panic("expected successful append at startup")
  }
  if !resp.Success {
    panic("Expected append at startup to work")
  }
  if resp.CommitIndex != 1 {
    panic("Expected commit index to be 3 at startup")
  }
}

type TestStateMachine struct {
  entries map[uint64][]byte
}

func createTestState() *TestStateMachine {
  return &TestStateMachine{
    entries: make(map[uint64][]byte),
  }
}

func (s *TestStateMachine) ApplyEntry(index uint64, data []byte) error {
  s.entries[index] = data
  return nil
}

func (s *TestStateMachine) GetLastIndex() (uint64, error) {
  var max uint64 = 0
  for i := range(s.entries) {
    if i > max {
      max = i
    }
  }
  return max, nil
}
