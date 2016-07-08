package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/30x/changeagent/communication"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	apiBasePort     = 14000
	clusterBasePort = 15000
	formContent     = "application/x-www-form-urlencoded"
)

var apiPort = apiBasePort
var clusterPort = clusterBasePort
var dataCount = 1

var _ = Describe("Cluster Tests", func() {

	It("Basic Cluster Test", func() {
		clusterTest(false, false)
	})

	It("Separate Cluster Port", func() {
		clusterTest(true, false)
	})

	It("Secure Cluster", func() {
		clusterTest(true, true)
	})
})

func clusterTest(useClusterPort, useSecureCluster bool) {
	listener1, port1, err := startListener(0)
	Expect(err).Should(Succeed())
	defer listener1.Close()
	clusterPort1 := port1

	mux1 := http.NewServeMux()
	var comm1 communication.Communication
	if useSecureCluster {
		comm1, err = communication.StartSecureCommunication(0,
			"../testkeys/clusterkey.pem", "../testkeys/clustercert.pem",
			"../testkeys/CA/certs/cacert.pem")
		clusterPort1 = comm1.Port()
	} else if useClusterPort {
		comm1, err = communication.StartSeparateCommunication(0)
		clusterPort1 = comm1.Port()
	} else {
		comm1, err = communication.StartHTTPCommunication(mux1)
	}
	Expect(err).Should(Succeed())

	agent1, err := StartChangeAgent(fmt.Sprintf("%s/data%d", DataDir, dataCount),
		mux1, "", comm1)
	dataCount++
	Expect(err).Should(Succeed())
	defer agent1.Close()
	go http.Serve(listener1, mux1)

	listener2, port2, err := startListener(0)
	Expect(err).Should(Succeed())
	defer listener2.Close()
	clusterPort2 := port2

	mux2 := http.NewServeMux()
	var comm2 communication.Communication
	if useSecureCluster {
		comm2, err = communication.StartSecureCommunication(0,
			"../testkeys/clusterkey.pem", "../testkeys/clustercert.pem",
			"../testkeys/CA/certs/cacert.pem")
		clusterPort2 = comm2.Port()
	} else if useClusterPort {
		comm2, err = communication.StartSeparateCommunication(0)
		clusterPort2 = comm2.Port()
	} else {
		comm2, err = communication.StartHTTPCommunication(mux2)
	}
	Expect(err).Should(Succeed())

	agent2, err := StartChangeAgent(fmt.Sprintf("%s/data%d", DataDir, dataCount),
		mux2, "", comm2)
	dataCount++
	Expect(err).Should(Succeed())
	defer agent2.Close()
	go http.Serve(listener2, mux2)

	addNode(port1, clusterPort1)
	addNode(port1, clusterPort2)

	first := sendOneChange(port1, "one")
	sendOneChange(port2, "two")
	verifyChanges(port1, first-1, []string{"one", "two"})
	verifyChanges(port2, first-1, []string{"one", "two"})
}

func addNode(apiPort, addPort int) {
	uri := fmt.Sprintf("http://localhost:%d/cluster/members", apiPort)
	msg := fmt.Sprintf("address=localhost:%d", addPort)
	resp, err := http.Post(uri, formContent, strings.NewReader(msg))
	Expect(err).Should(Succeed())
	Expect(resp.StatusCode).Should(Equal(200))
	defer resp.Body.Close()
}

func sendOneChange(port int, msg string) uint64 {
	uri := fmt.Sprintf("http://localhost:%d/changes", port)
	bod := fmt.Sprintf("{\"data\":{\"msg\":\"%s\"}}", msg)
	resp, err := http.Post(uri, jsonContent, strings.NewReader(bod))
	Expect(err).Should(Succeed())
	Expect(resp.StatusCode).Should(Equal(200))
	defer resp.Body.Close()

	var c Change
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&c)
	Expect(err).Should(Succeed())
	return c.ID
}

func verifyChanges(port int, since uint64, msgs []string) {
	uri := fmt.Sprintf("http://localhost:%d/changes?since=%d", port, since)
	resp, err := http.Get(uri)
	Expect(err).Should(Succeed())
	Expect(resp.StatusCode).Should(Equal(200))
	defer resp.Body.Close()

	var cl ChangeList
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&cl)
	Expect(err).Should(Succeed())

	Expect(len(cl.Changes)).Should(Equal(len(msgs)))
	for i, msg := range msgs {
		Expect(string(cl.Changes[i].Data)).Should(MatchJSON(fmt.Sprintf("{\"msg\":\"%s\"}", msg)))
	}
}
