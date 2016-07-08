package main

import (
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/30x/changeagent/communication"
	"github.com/golang/glog"
)

const (
	defaultPort = 8080
)

func main() {
	os.Exit(runAgentMain())
}

func runAgentMain() int {
	var port int
	var dbDir string
	var apiPrefix string
	var help bool
	var clusterPort int
	var clusterKey string
	var clusterCert string
	var clusterCA string

	flag.IntVar(&port, "p", defaultPort, "Port to listen on")
	flag.StringVar(&dbDir, "d", "", "Directory in which to place data. Required.")
	flag.BoolVar(&help, "h", false, "Print help message.")
	flag.StringVar(&apiPrefix, "P", "", "API Path Prefix. Ex. \"foo\" means all APIs prefixed with \"/foo\"")
	flag.IntVar(&clusterPort, "cp", -1, "Cluster Port. Optional.")
	flag.StringVar(&clusterKey, "ckey", "", "Cluster TLS key file")
	flag.StringVar(&clusterCert, "ccert", "", "Cluster TLS Certificate")
	flag.StringVar(&clusterCA, "cca", "", "Cluster TLS certificate file for peer verification")

	flag.Parse()
	if help || !flag.Parsed() {
		printUsage("")
		return 2
	}

	if dbDir == "" {
		printUsage("Database directory must be specified.")
		return 3
	}

	mux := http.NewServeMux()

	var comm communication.Communication
	var err error

	if clusterKey != "" && clusterCert != "" && clusterCA != "" {
		if clusterKey == "" || clusterCert == "" || clusterCA == "" || clusterPort == 0 {
			err = errors.New("All of \"cp\", \"ckey\", \"ccert\", and \"cca\" must be set")
		} else {
			comm, err = communication.StartSecureCommunication(
				clusterPort, clusterKey, clusterCert, clusterCA)
		}
	} else if clusterPort > 0 {
		comm, err = communication.StartSeparateCommunication(clusterPort)
	} else {
		comm, err = communication.StartHTTPCommunication(mux)
	}
	if err != nil {
		fmt.Printf(err.Error())
		return 5
	}
	defer comm.Close()

	agent, err := StartChangeAgent(dbDir, mux, apiPrefix, comm)
	if err != nil {
		fmt.Printf("Error starting agent: %s\n", err)
		return 6
	}
	defer agent.Close()

	listener, listenPort, err := startListener(port)
	if err != nil {
		fmt.Printf("Error listening on TCP port: %s", err)
		return 7
	}
	defer listener.Close()

	glog.Infof("Listening on port %d", listenPort)

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
	fmt.Println("  agent -d ./data -p 9000")
}
