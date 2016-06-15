package hooks

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	debugEnabled = false
	goodTestBody = "Good Request"
)

var testListener *net.TCPListener
var testAddress string

func TestHooks(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Hook Suite")
}

var _ = BeforeSuite(func() {
	flag.Set("logtostderr", "true")
	if debugEnabled {
		flag.Set("v", "2")
	}
	flag.Parse()

	anyPort := &net.TCPAddr{}
	listener, err := net.ListenTCP("tcp", anyPort)
	Expect(err).Should(Succeed())
	testListener = listener

	_, port, err := net.SplitHostPort(listener.Addr().String())
	Expect(err).Should(Succeed())
	fmt.Fprintf(GinkgoWriter, "Listening on %s\n", port)
	testAddress = fmt.Sprintf("localhost:%s", port)

	go http.Serve(listener, &testServer{})
	Expect(err).Should(Succeed())
})

var _ = AfterSuite(func() {
	testListener.Close()
})

type testServer struct {
}

func (s *testServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/good" {
		defer req.Body.Close()
		bod, err := ioutil.ReadAll(req.Body)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
		}
		if string(bod) != goodTestBody {
			resp.WriteHeader(http.StatusBadRequest)
		} else {
			resp.WriteHeader(http.StatusOK)
		}
	} else if req.URL.Path == "/header" {
		val := req.Header.Get("X-Apigee-Testing")
		if val != "yes" {
			resp.WriteHeader(http.StatusBadRequest)
		} else {
			resp.WriteHeader(http.StatusOK)
		}
	} else {
		resp.WriteHeader(http.StatusNotFound)
	}
}
