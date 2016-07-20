package main

import (
	"crypto/tls"
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
		clusterTest(false, false, false)
	})

	It("Separate Cluster Port", func() {
		clusterTest(true, false, false)
	})

	It("Secure Cluster", func() {
		clusterTest(true, true, false)
	})

	It("Secure Cluster and API", func() {
		clusterTest(true, true, true)
	})
})

func clusterTest(useClusterPort, useSecureCluster, secureAPI bool) {
	key := ""
	cert := ""
	proto := "http"
	httpClient := http.DefaultClient
	if secureAPI {
		key = "../testkeys/key.pem"
		cert = "../testkeys/cert.pem"
		proto = "https"
		httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}

	listener1, port1, err := startListener(0, key, cert, "")
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
		mux1, "", comm1, "", "")
	dataCount++
	Expect(err).Should(Succeed())
	defer agent1.Close()
	go http.Serve(listener1, mux1)

	listener2, port2, err := startListener(0, key, cert, "")
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
		mux2, "", comm2, "", "")
	dataCount++
	Expect(err).Should(Succeed())
	defer agent2.Close()
	go http.Serve(listener2, mux2)

	addNode(port1, clusterPort1, proto, httpClient)
	addNode(port1, clusterPort2, proto, httpClient)

	first := sendOneChange(port1, "one", proto, httpClient)
	sendOneChange(port2, "two", proto, httpClient)
	verifyChanges(port1, first-1, []string{"one", "two"}, proto, httpClient)
	verifyChanges(port2, first-1, []string{"one", "two"}, proto, httpClient)
}

func addNode(apiPort, addPort int, proto string, client *http.Client) {
	uri := fmt.Sprintf("%s://localhost:%d/cluster/members", proto, apiPort)
	msg := fmt.Sprintf("address=localhost:%d", addPort)
	resp, err := client.Post(uri, formContent, strings.NewReader(msg))
	Expect(err).Should(Succeed())
	Expect(resp.StatusCode).Should(Equal(200))
	defer resp.Body.Close()
}

func sendOneChange(port int, msg, proto string, client *http.Client) uint64 {
	uri := fmt.Sprintf("%s://localhost:%d/changes", proto, port)
	bod := fmt.Sprintf("{\"data\":{\"msg\":\"%s\"}}", msg)
	resp, err := client.Post(uri, jsonContent, strings.NewReader(bod))
	Expect(err).Should(Succeed())
	Expect(resp.StatusCode).Should(Equal(200))
	defer resp.Body.Close()

	var c Change
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&c)
	Expect(err).Should(Succeed())
	return c.ID
}

func verifyChanges(port int, since uint64, msgs []string, proto string, client *http.Client) {
	uri := fmt.Sprintf("%s://localhost:%d/changes?since=%d", proto, port, since)
	resp, err := client.Get(uri)
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
