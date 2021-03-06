package raft

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"testing"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/communication"
	"github.com/30x/changeagent/storage"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	DataDir           = "./rafttestdata"
	PreserveDatabases = false
	DumpDatabases     = false
	DebugMode         = false
)

var testRafts []*Service
var testListener []*net.TCPListener
var unitTestRaft *Service
var unitTestListener *net.TCPListener
var anyPort net.TCPAddr
var webHookListener *net.TCPListener
var webHookAddr string
var unitTestID common.NodeID
var clusterSize int

func TestRaft(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Raft Suite")
}

var _ = BeforeSuite(func() {
	os.MkdirAll(DataDir, 0777)
	flag.Set("logtostderr", "true")
	if DebugMode || os.Getenv("TESTDEBUG") != "" {
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

	// Create one more for unit tests
	unitTestListener, unitTestAddr := startListener()

	raft1, err := startRaft(testListener[0], path.Join(DataDir, "test1"))
	Expect(err).Should(Succeed())
	testRafts = append(testRafts, raft1)

	raft2, err := startRaft(testListener[1], path.Join(DataDir, "test2"))
	Expect(err).Should(Succeed())
	testRafts = append(testRafts, raft2)

	raft3, err := startRaft(testListener[2], path.Join(DataDir, "test3"))
	Expect(err).Should(Succeed())
	testRafts = append(testRafts, raft3)

	unitTestRaft, err = startRaft(unitTestListener, path.Join(DataDir, "unit"))
	Expect(err).Should(Succeed())
	unitTestID = unitTestRaft.MyID()

	clusterSize = 3

	webHookListener, webHookAddr = startListener()
	go http.Serve(webHookListener, &webHookServer{})

	// Now build the cluster
	err = raft1.InitializeCluster(addrs[0])
	Expect(err).Should(Succeed())

	err = raft1.AddNode(addrs[1])
	Expect(err).Should(Succeed())
	assertOneLeader()

	err = raft1.AddNode(addrs[2])
	Expect(err).Should(Succeed())
	assertOneLeader()

	// Also make the unit test raft a leader of its own cluster.
	// Leaders work a little differently from standalone nodes.
	err = unitTestRaft.InitializeCluster(unitTestAddr)
	Expect(err).Should(Succeed())
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
	webHookListener.Close()
})

func startRaft(listener *net.TCPListener, dir string) (*Service, error) {
	mux := http.NewServeMux()
	comm, err := communication.StartHTTPCommunication(mux)
	if err != nil {
		return nil, err
	}
	stor, err := storage.CreateRocksDBStorage(dir, 1000)
	if err != nil {
		return nil, err
	}

	raft, err := StartRaft(comm, stor, &dummyStateMachine{}, "")
	if err != nil {
		return nil, err
	}
	comm.SetRaft(raft)
	go func() {
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
	for i, r := range testRafts {
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

func (d *dummyStateMachine) Commit(e *common.Entry) error {
	return nil
}

type webHookServer struct {
}

/*
This function implements a little webhook that will reject any request that
consists of the string "FailMe"
*/
func (w *webHookServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	bod, err := ioutil.ReadAll(req.Body)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
	} else if string(bod) == "FailMe" {
		resp.WriteHeader(http.StatusBadRequest)
	} else {
		resp.WriteHeader(http.StatusOK)
	}
}
