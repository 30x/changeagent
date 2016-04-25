package raft

import (
  "fmt"
  "flag"
  "os"
  "net"
  "net/http"
  "path"
  "testing"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

const (
  DataDir = "./rafttestdata"
  PreserveDatabases = false
  DumpDatabases = false
  DebugMode = false
)

var testRafts []*Service
var testListener []*net.TCPListener
var unitTestRaft *Service
var unitTestListener *net.TCPListener
var testDiscovery discovery.Discovery
var anyPort net.TCPAddr
var unitTestAddr string

func TestRaft(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Raft Suite")
}

var _ = BeforeSuite(func() {
  os.MkdirAll(DataDir, 0777)
  flag.Set("logtostderr", "true")
  if DebugMode {
    flag.Set("v", "5")
  }
  flag.Parse()

  // Create three TCP listeners -- we'll use them for a cluster

  var addrs []string
  for li := 0; li < 3; li++ {
    listener, addr := startListener()
    addrs = append(addrs, addr)
    testListener = append(testListener, listener)
  }
  disco := discovery.CreateStaticDiscovery(addrs)
  testDiscovery = disco

  // Create one more for unit tests
  unitTestListener, unitAddr := startListener()
  unitTestAddr = unitAddr
  unitDisco := discovery.CreateStaticDiscovery([]string{unitAddr})

  raft1, err := startRaft(disco, testListener[0], path.Join(DataDir, "test1"))
  Expect(err).Should(Succeed())
  testRafts = append(testRafts, raft1)

  raft2, err := startRaft(disco, testListener[1], path.Join(DataDir, "test2"))
  Expect(err).Should(Succeed())
  testRafts = append(testRafts, raft2)

  raft3, err := startRaft(disco, testListener[2], path.Join(DataDir, "test3"))
  Expect(err).Should(Succeed())
  testRafts = append(testRafts, raft3)

  unitTestRaft, err = startRaft(unitDisco, unitTestListener, path.Join(DataDir, "unit"))
  Expect(err).Should(Succeed())
  // This happens normally -- need it to happen here for unit tests to work.
  unitTestRaft.addDiscoveredNode(unitTestRaft.id, unitAddr)
})

func startListener() (*net.TCPListener, string) {
  listener, err := net.ListenTCP("tcp4", &anyPort)
  Expect(err).Should(Succeed())
  _, port, err := net.SplitHostPort(listener.Addr().String())
  Expect(err).Should(Succeed())
  addr := fmt.Sprintf("localhost:%s", port)
  return listener, addr
}

var _ = AfterSuite(func() {
  cleanRafts()
})

func startRaft(disco discovery.Discovery, listener *net.TCPListener, dir string) (*Service, error) {
  mux := http.NewServeMux()
  comm, err := communication.StartHTTPCommunication(mux)
  if err != nil { return nil, err }
  stor, err := storage.CreateRocksDBStorage(dir, 1000)
  if err != nil { return nil, err }

  raft, err := StartRaft(comm, disco, stor, &dummyStateMachine{})
  if err != nil { return nil, err }
  comm.SetRaft(raft)
  go func(){
    // Disable HTTP keep-alives to make raft restart tests more reliable
    svr := &http.Server{
      Handler: mux,
    }
    svr.SetKeepAlivesEnabled(false)
    svr.Serve(listener)
  }()

  return raft, nil
}

func cleanRafts() {
  for i, r := range(testRafts) {
    cleanRaft(r, testListener[i])
  }
  cleanRaft(unitTestRaft, unitTestListener)
}

func cleanRaft(raft *Service, l *net.TCPListener) {
  raft.Close()
  if DumpDatabases {
    raft.stor.Dump(os.Stdout, 1000)
  }
  raft.stor.Close()
  if !PreserveDatabases {
    raft.stor.Delete()
  }
  l.Close()
}

type dummyStateMachine struct {
}

func (d *dummyStateMachine) Commit(e *storage.Entry) error {
  return nil
}