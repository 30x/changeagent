package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/30x/changeagent/discovery"
	"github.com/golang/glog"
)

const (
	defaultPort       = 8080
	defaultConfigScan = 10 * time.Second
)

func main() {
	os.Exit(runAgentMain())
}

func runAgentMain() int {
	var port int
	var dbDir string
	var discoveryFile string
	var apiPrefix string
	var help bool

	flag.IntVar(&port, "p", defaultPort, "Port to listen on.")
	flag.StringVar(&dbDir, "d", "", "Directory in which to place data. Required.")
	flag.StringVar(&discoveryFile, "s", "", "File from which to read list of peers. Default is single-node operation.")
	flag.BoolVar(&help, "h", false, "Print help message.")
	flag.StringVar(&apiPrefix, "P", "", "API Path Prefix. Ex. \"foo\" means all APIs prefixed with \"/foo\"")

	flag.Parse()
	if help || !flag.Parsed() {
		printUsage("")
		return 2
	}

	if dbDir == "" {
		printUsage("Database directory must be specified.")
		return 3
	}

	var disco discovery.Discovery
	var err error
	if discoveryFile == "" {
		disco = discovery.CreateStandaloneDiscovery(fmt.Sprintf("localhost:%d", port))
	} else {
		disco, err = discovery.ReadDiscoveryFile(discoveryFile, defaultConfigScan)
		if err != nil {
			fmt.Printf("Error reading discovery file: %s\n", err)
			return 5
		}
	}
	defer disco.Close()

	mux := http.NewServeMux()
	agent, err := StartChangeAgent(disco, dbDir, mux, apiPrefix)
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
		sig := <-signalChan
		glog.Infof("Got signal %v", sig)
		doneChan <- true
	}()

	go http.Serve(listener, mux)

	<-doneChan
	glog.Infof("Shutting down.")
	return 0
}

func printUsage(msg string) {
	if msg != "" {
		fmt.Println(msg)
		fmt.Println()
	}
	fmt.Println("Usage:")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  agent -d ./data -p 9000 -s discovery")
}
