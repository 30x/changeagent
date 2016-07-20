package auth

import (
	"fmt"
	"net"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const watchTime = 1 * time.Second

var testListener net.Listener
var testURI string
var testStore *Store

var _ = BeforeSuite(func() {
	var err error
	testListener, err = net.ListenTCP("tcp", &net.TCPAddr{})
	Expect(err).Should(Succeed())
	_, port, err := net.SplitHostPort(testListener.Addr().String())
	Expect(err).Should(Succeed())
	testURI = fmt.Sprintf("http://localhost:%s/", port)

	testStore = NewAuthStore()
	err = testStore.Load("./testfiles/pw1")
	Expect(err).Should(Succeed())
	err = testStore.Watch(watchTime)
	Expect(err).Should(Succeed())

	handler := &testServer{}
	authHandler := testStore.CreateHandler(handler, "changeagent")
	go http.Serve(testListener, authHandler)
})

var _ = AfterSuite(func() {
	testListener.Close()
	testStore.Close()
})

var _ = Describe("HTTP Auth Suite", func() {
	It("Unauthenticated", func() {
		resp, err := http.Get(testURI)
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(401))
		Expect(resp.Header.Get("WWW-Authenticate")).Should(Equal("Basic realm=\"changeagent\""))
	})

	It("Authenticated", func() {
		req, err := http.NewRequest("GET", testURI, nil)
		Expect(err).Should(Succeed())
		req.SetBasicAuth("foo@bar.com", "baz")
		resp, err := http.DefaultClient.Do(req)
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(200))
	})

	It("Bad Password", func() {
		req, err := http.NewRequest("GET", testURI, nil)
		Expect(err).Should(Succeed())
		req.SetBasicAuth("foo@bar.com", "foo")
		resp, err := http.DefaultClient.Do(req)
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(401))
		Expect(resp.Header.Get("WWW-Authenticate")).Should(Equal("Basic realm=\"changeagent\""))
	})
})

type testServer struct {
}

func (s *testServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
}
