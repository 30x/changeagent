package raft

import (
  "fmt"
  "os"
  "path"
  "testing"
  "time"
  "github.com/gin-gonic/gin"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

const (
  Port1 = 44444
  Port2 = 44445
  Port3 = 44446
  UTPort = 44447
  DataDir = "./rafttestdata"
)

var testRafts []*RaftImpl
var unitTestRaft *RaftImpl

func TestMain(m *testing.M) {
  os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
  os.MkdirAll(DataDir, 0777)
  log.InitDebug(true)

  addrs := []string{
    fmt.Sprintf("localhost:%d", Port1),
    fmt.Sprintf("localhost:%d", Port2),
    fmt.Sprintf("localhost:%d", Port3),
  }
  disco := discovery.CreateStaticDiscovery(addrs)

  unitAddr := []string{fmt.Sprintf("localhost:%d", UTPort)}
  unitDisco := discovery.CreateStaticDiscovery(unitAddr)

  defer cleanRafts()

  raft1, err := startRaft(1, disco, Port1, path.Join(DataDir, "test1"))
  if err != nil {
    fmt.Printf("Error starting raft 1: %v", err)
    return 2
  }
  testRafts = append(testRafts, raft1)

  // TODO Raft never converges if each node starts at exactly the same time!
  time.Sleep(time.Second)

  raft2, err := startRaft(2, disco, Port2, path.Join(DataDir, "test2"))
  if err != nil {
    fmt.Printf("Error starting raft 2: %v", err)
    return 3
  }
  testRafts = append(testRafts, raft2)

  time.Sleep(time.Second)

  raft3, err := startRaft(3, disco, Port3, path.Join(DataDir, "test3"))
  if err != nil {
    fmt.Printf("Error starting raft 3: %v", err)
    return 4
  }
  testRafts = append(testRafts, raft3)

  unitTestRaft, err = startRaft(1, unitDisco, UTPort, path.Join(DataDir, "unit"))
  if err != nil {
    fmt.Printf("Error starting unit test raft: %v", err)
    return 4
  }
  initUnitTests(unitTestRaft)

  return m.Run()
}

func startRaft(id uint64, disco discovery.Discovery, port int, dir string) (*RaftImpl, error) {
  api := gin.Default()
  comm, err := communication.StartHttpCommunication(api, disco)
  if err != nil { return nil, err }
  stor, err := storage.CreateSqliteStorage(dir)
  if err != nil { return nil, err }
  sm := createTestState()

  raft, err := StartRaft(id, comm, disco, stor, sm)
  if err != nil { return nil, err }
  comm.SetRaft(raft)
  go api.Run(fmt.Sprintf(":%d", port))

  return raft, nil
}

func cleanRafts() {
  for _, r := range(testRafts) {
    if r != nil {
      r.Close()
      r.stor.Close()
      r.stor.Delete()
    }
  }
  unitTestRaft.Close()
  unitTestRaft.stor.Close()
  unitTestRaft.stor.Delete()
}

// Set up the state machine so that we can properly do some unit tests
func initUnitTests(raft *RaftImpl) {
  raft.setFollowerOnly(true)

  ar := &communication.AppendRequest{
    Term: 2,
    LeaderId: 1,
    PrevLogIndex: 0,
    PrevLogTerm: 0,
    LeaderCommit: 3,
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
  if resp.CommitIndex != 3 {
    panic("Expected commit index to be 3 at startup")
  }
}

type TestStateMachine struct {
}

func createTestState() *TestStateMachine {
  return &TestStateMachine{}
}

func (s *TestStateMachine) ApplyEntry(data []byte) error {
  log.Infof("Applying %d bytes of data", len(data))
  return nil
}

func (s *TestStateMachine) GetLastIndex() (uint64, error) {
  return 0, nil
}
