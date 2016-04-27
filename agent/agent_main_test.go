package main

import (
  "flag"
  "encoding/json"
  "fmt"
  "os"
  "io/ioutil"
  "net"
  "net/http"
  "testing"
  "time"
  "revision.aeip.apigee.net/greg/changeagent/discovery"

  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

const (
  DataDir = "./agenttestdata"
  PreserveDatabases = false
  DebugMode = false
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

  // Create three TCP listeners -- we'll use them for a cluster
  anyPort := &net.TCPAddr{}
  testListener, err := net.ListenTCP("tcp4", anyPort)
  if err != nil { panic("Can't listen on a TCP port") }
  _, port, err := net.SplitHostPort(testListener.Addr().String())
  if err != nil { panic("Invalid listen address") }
  listenAddr = fmt.Sprintf("localhost:%s", port)
  disco := discovery.CreateStaticDiscovery([]string{listenAddr})
  listenURI = fmt.Sprintf("http://localhost:%s", port)
  fmt.Fprintf(GinkgoWriter, "Listening on port %s\n", port)

  testAgent, err = startAgent(1, disco, DataDir, testListener)
  Expect(err).Should(Succeed())

  time.Sleep(time.Second)
})

var _ = AfterSuite(func() {
  cleanAgent(testAgent, testListener)
})

func startAgent(id uint64, disco discovery.Discovery, dir string, listener *net.TCPListener) (*ChangeAgent, error) {
  mux := http.NewServeMux()

  agent, err := StartChangeAgent(disco, dir, mux)
  if err != nil { return nil, err }
  go func() {
    err = http.Serve(listener, mux)
    if err != nil {
      panic(fmt.Sprintf("Error serving HTTP: %s", err))
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