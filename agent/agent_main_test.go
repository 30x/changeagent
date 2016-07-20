package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"github.com/30x/changeagent/communication"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	DataDir           = "./agenttestdata"
	PreserveDatabases = false
	DebugMode         = false
	uriPrefix         = "/changetest"
)

var testListener *net.TCPListener
var testAgent *ChangeAgent
var listenAddr string
var listenURI string

func TestAgent(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Agent Suite")
}

var _ = BeforeSuite(func() {
	os.MkdirAll(DataDir, 0777)
	flag.Set("logtostderr", "true")
	if DebugMode {
		flag.Set("v", "5")
	}
	flag.Parse()

	anyPort := &net.TCPAddr{}

	var err error
	testListener, err = net.ListenTCP("tcp4", anyPort)
	if err != nil {
		panic("Can't listen on a TCP port")
	}
	_, port, err := net.SplitHostPort(testListener.Addr().String())
	if err != nil {
		panic("Invalid listen address")
	}
	listenAddr = fmt.Sprintf("localhost:%s", port)
	listenURI = fmt.Sprintf("http://localhost:%s%s", port, uriPrefix)
	fmt.Fprintf(GinkgoWriter, "Listening on port %s\n", port)

	testAgent, err = startAgent(1, path.Join(DataDir, "data1"), testListener)
	Expect(err).Should(Succeed())

	time.Sleep(time.Second)
})

var _ = AfterSuite(func() {
	cleanAgent(testAgent, testListener)
	os.RemoveAll(DataDir)
})

func startAgent(id uint64, dir string, listener *net.TCPListener) (*ChangeAgent, error) {
	mux := http.NewServeMux()

	comm, err := communication.StartHTTPCommunication(mux)
	if err != nil {
		return nil, err
	}

	agent, err := StartChangeAgent(dir, mux, uriPrefix, comm, "", "")
	if err != nil {
		return nil, err
	}
	go func() {
		err = http.Serve(listener, mux)
		if err != nil {
			fmt.Fprintf(GinkgoWriter, "Error serving HTTP: %s", err)
		}
	}()

	return agent, nil
}

func cleanAgent(agent *ChangeAgent, l *net.TCPListener) {
	agent.Close()
	if !PreserveDatabases {
		agent.Delete()
	}
	l.Close()
}

func parseJSON(resp *http.Response) map[string]string {
	defer resp.Body.Close()
	bytes, err := ioutil.ReadAll(resp.Body)
	Expect(err).Should(Succeed())

	jsonBody := make(map[string]string)
	err = json.Unmarshal(bytes, &jsonBody)
	Expect(err).Should(Succeed())

	fmt.Fprintf(GinkgoWriter, "Got JSON response %v\n", jsonBody)
	return jsonBody
}
