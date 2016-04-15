package main

import (
  "flag"
  "fmt"
  "os"
  "net"
  "net/http"
  "strings"
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

  agent, err := StartChangeAgent(id, disco, dir, mux)
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

func ensureTenant(name string) string {
  fmt.Fprintf(GinkgoWriter, "Checking for tenant %s\n", name)
  gr, err := http.Get(fmt.Sprintf("%s/tenants/%s", listenURI, name))
  Expect(err).Should(Succeed())

  if gr.StatusCode == 404 {
    // Need to make that tenant the first time
    uri := listenURI + "/tenants"
    request := fmt.Sprintf("name=%s", name)

    fmt.Fprintf(GinkgoWriter, "Creating tenant %s\n", name)
    pr, err := http.Post(uri, FormContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))
    lastNewChange++

    resp := parseJSON(pr)
    fmt.Fprintf(GinkgoWriter, "New tenant %s created\n", resp["_id"])
    return resp["_id"]

  } else if gr.StatusCode == 200 {
    resp := parseJSON(gr)
    fmt.Fprintf(GinkgoWriter, "Tenant %s exists\n", resp["_id"])
    return resp["_id"]

  } else {
    Expect(gr.StatusCode).Should(Equal(200))
    return ""
  }
}

func ensureCollection(tenant, name string) string {
  fmt.Fprintf(GinkgoWriter, "Checking for collection %s\n", name)
  gr, err := http.Get(fmt.Sprintf("%s/tenants/%s/collections/%s", listenURI, tenant, name))
  Expect(err).Should(Succeed())

  if gr.StatusCode == 404 {
    // Need to make that tenant the first time
    uri := fmt.Sprintf("%s/tenants/%s/collections", listenURI, tenant)
    request := fmt.Sprintf("name=%s", name)

    fmt.Fprintf(GinkgoWriter, "Creating collection %s for tenant %s\n", name, tenant)
    pr, err := http.Post(uri, FormContent, strings.NewReader(request))
    Expect(err).Should(Succeed())
    Expect(pr.StatusCode).Should(Equal(200))
    lastNewChange++

    resp := parseJSON(pr)
    fmt.Fprintf(GinkgoWriter, "New collection %s created\n", resp["_id"])
    return resp["_id"]

  } else if gr.StatusCode == 200 {
    resp := parseJSON(gr)
    fmt.Fprintf(GinkgoWriter, "Collection %s exists\n", resp["_id"])
    return resp["_id"]

  } else {
    Expect(gr.StatusCode).Should(Equal(200))
    return ""
  }
}