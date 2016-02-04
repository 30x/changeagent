package main

import (
  "fmt"
  "flag"
  "os"
  "net"
  "syscall"
  "time"
  "os/signal"
  "net/http"
  "github.com/golang/glog"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
)

const (
  DefaultNode = 1
  DefaultPort = 8080
  DefaultConfigScan = 10 * time.Second
)

func main() {
  os.Exit(runAgentMain())
}

func runAgentMain() int {
  var nodeId uint64
  var port int
  var dbDir string
  var discoveryFile string
  var debug bool
  var help bool

  flag.Uint64Var(&nodeId, "id", DefaultNode, "Node ID. Must be in discovery data.")
  flag.IntVar(&port, "p", DefaultPort, "Port to listen on.")
  flag.StringVar(&dbDir, "d", "", "Directory in which to place data.")
  flag.StringVar(&discoveryFile, "s", "", "File from which to read list of peers")
  flag.BoolVar(&debug, "D", false, "Enable debugging")
  flag.BoolVar(&help, "h", false, "Print help message")

  flag.Parse()
  if help || !flag.Parsed() {
    flag.PrintDefaults()
    return 2
  }

  if dbDir == "" {
    fmt.Println("Database directory must be specified.")
    flag.PrintDefaults()
    return 3
  }
  if discoveryFile == "" {
    fmt.Println("Discovery file name must be specified.")
    return 4
  }

  disco, err := discovery.ReadDiscoveryFile(discoveryFile, DefaultConfigScan)
  if err != nil {
    fmt.Printf("Error reading discovery file: %s\n", err)
    return 5
  }
  defer disco.Close()

  mux := http.NewServeMux()
  agent, err := StartChangeAgent(nodeId, disco, dbDir, mux)
  if err != nil {
    fmt.Printf("Error starting agent: %s\n", err)
    return 6
  }
  defer agent.Close()

  addr := &net.TCPAddr{
    Port: port,
  }

  listener, err := net.ListenTCP("tcp4", addr)
  if err != nil {
    fmt.Printf("Error listening on TCP port: %s", err)
    return 7
  }
  defer listener.Close()

  glog.Infof("Listening on port %d", port)

  doneChan := make(chan bool, 1)
  signalChan := make(chan os.Signal, 1)
  signal.Notify(signalChan, syscall.SIGINT)
  signal.Notify(signalChan, syscall.SIGTERM)

  go func() {
    sig := <- signalChan
    glog.Infof("Got signal %v", sig)
    doneChan <- true
  }()

  go http.Serve(listener, mux)

  <- doneChan
  glog.Infof("Shutting down.")
  return 0
}
