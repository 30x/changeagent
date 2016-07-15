package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/30x/changeagent/communication"
	"github.com/golang/glog"
	"github.com/tylerb/graceful"
)

const (
	defaultPort             = 8080
	gracefulShutdownTimeout = 60 * time.Second
)

func main() {
	exitCode := runAgentMain()
	os.Exit(exitCode)
}

func runAgentMain() int {
	var port int
	var securePort int
	var cfgFile string
	var dbDir string
	var apiPrefix string
	var help bool
	var key string
	var cert string
	var cas string
	var clusterPort int
	var clusterKey string
	var clusterCert string
	var clusterCA string

	flag.IntVar(&port, "p", defaultPort, "Port to listen on")
	flag.IntVar(&securePort, "sp", -1, "Port to listen on if TLS is configured")
	flag.StringVar(&dbDir, "d", "", "Directory in which to place data. Required.")
	flag.StringVar(&cfgFile, "f", "", "File to store changeagent configuration")
	flag.BoolVar(&help, "h", false, "Print help message.")
	flag.StringVar(&apiPrefix, "P", "", "API Path Prefix. Ex. \"foo\" means all APIs prefixed with \"/foo\"")
	flag.StringVar(&key, "key", "", "TLS key file")
	flag.StringVar(&cert, "cert", "", "TLS certificate file")
	flag.StringVar(&cas, "cas", "", "Trusted Client certificates for TLS")
	flag.IntVar(&clusterPort, "cp", -1, "Cluster Port. Required if any TLS information is used.")
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

	if key != "" || cert != "" {
		if key == "" || cert == "" || securePort < 0 {
			fmt.Fprintf(os.Stderr, "All three of \"key,\" \"cert,\" and \"cp\" must be set.\n")
			return 5
		}
	}

	if clusterKey != "" || clusterCert != "" || clusterCA != "" {
		if clusterKey == "" || clusterCert == "" || clusterCA == "" || clusterPort <= 0 {
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
		fmt.Fprintln(os.Stderr, err.Error())
		return 5
	}
	defer comm.Close()

	agent, err := StartChangeAgent(dbDir, mux, apiPrefix, comm, cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting agent: %s\n", err)
		return 6
	}
	defer agent.Close()

	listener, listenPort, err := startListener(port, "", "", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listening on TCP port: %s", err)
		return 7
	}
	defer listener.Close()
	glog.Infof("Listening on port %d", listenPort)

	var secureListener net.Listener
	if securePort >= 0 {
		if key == "" || cert == "" {
			fmt.Fprintln(os.Stderr,
				"Cannot listen on secure port unless both key and cert are specified")
			return 8
		}

		var secureListenerPort int
		secureListener, secureListenerPort, err =
			startListener(securePort, key, cert, cas)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listening on secure TCP port: %s\n", err)
			return 9
		}
		glog.Infof("Listening with TLS on port %d", secureListenerPort)
		defer secureListener.Close()
	}

	svr := &http.Server{
		Handler: mux,
	}

	if secureListener != nil {
		go graceful.Serve(svr, secureListener, gracefulShutdownTimeout)
	}

	graceful.Serve(svr, listener, gracefulShutdownTimeout)

	return 0
}

func printUsage(msg string) {
	if msg != "" {
		fmt.Fprintln(os.Stderr, msg)
		fmt.Fprintln(os.Stderr)
	}
	fmt.Fprintln(os.Stderr, "Usage:")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Example:")
	fmt.Fprintln(os.Stderr, "  agent -d ./data -p 9000")
}
