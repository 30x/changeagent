package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Diagnostic API test", func() {
	It("Root Links", func() {
		links := getJSON("")
		Expect(links["changes"]).ShouldNot(BeEmpty())
		testURI(links["changes"].(string))
		Expect(links["diagnostics"]).ShouldNot(BeEmpty())
		testURI(links["diagnostics"].(string))
	})

	It("Diagnostics Links", func() {
		links := getJSON("/diagnostics")
		Expect(links["id"]).ShouldNot(BeEmpty())
		testURI(links["id"].(string))
		Expect(links["stack"]).ShouldNot(BeEmpty())
		testURI(links["stack"].(string))
		Expect(links["raft"]).ShouldNot(BeEmpty())
		testURI(links["raft"].(string))
	})

	It("Node ID", func() {
		uri := listenURI + "/diagnostics/id"
		idStr := testURI(uri)
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 16, 64)
		Expect(err).Should(Succeed())
		Expect(id).ShouldNot(BeZero())
	})

	It("Raft Info", func() {
		raft := getJSON("/diagnostics/raft")
		Expect(raft["state"]).Should(Equal("Leader"))
		id, err := strconv.ParseUint(strings.TrimSpace(raft["leader"].(string)), 16, 64)
		Expect(err).Should(Succeed())
		Expect(id).ShouldNot(BeZero())
	})

	It("Stack", func() {
		uri := listenURI + "/diagnostics/stack"
		stack := testURI(uri)
		Expect(stack).ShouldNot(BeEmpty())
	})
})

func getJSON(path string) map[string]interface{} {
	uri := listenURI + path
	fmt.Fprintf(GinkgoWriter, "GET %s\n", uri)
	resp, err := http.Get(uri)
	Expect(err).Should(Succeed())
	Expect(resp.StatusCode).Should(Equal(http.StatusOK))
	defer resp.Body.Close()

	Expect(resp.Header.Get("content-type")).Should(Equal("application/json"))
	dec := json.NewDecoder(resp.Body)
	var msg map[string]interface{}
	err = dec.Decode(&msg)
	Expect(err).Should(Succeed())
	return msg
}

func testURI(uri string) string {
	fmt.Fprintf(GinkgoWriter, "Invoking %s\n", uri)
	resp, err := http.Get(uri)
	Expect(err).Should(Succeed())
	Expect(resp.StatusCode).Should(Equal(http.StatusOK))
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	Expect(err).Should(Succeed())
	return string(buf)
}
