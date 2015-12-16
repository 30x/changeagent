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
  DataDir = "./rafttestdata"
)

var testRafts []*RaftImpl

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
