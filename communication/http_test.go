package communication

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/30x/changeagent/storage"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	debugEnabled = false
)

var expectedEntries []storage.Entry
var expectedLock = sync.Mutex{}

var testListener *net.TCPListener
var testRaft Raft
var address string
var comm Communication

func TestCommunication(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Communication Suite")
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
	address = fmt.Sprintf("localhost:%s", port)

	testRaft = makeTestRaft()
	mux := http.NewServeMux()

	comm, err = StartHTTPCommunication(mux)
	Expect(err).Should(Succeed())
	comm.SetRaft(testRaft)
	go http.Serve(listener, mux)
	Expect(err).Should(Succeed())
})

var _ = AfterSuite(func() {
	testListener.Close()
})

var _ = Describe("Communication", func() {
	It("Discover", func() {
		id, err := comm.Discover(address)
		Expect(err).Should(Succeed())
		Expect(id).Should(BeEquivalentTo(1))
	})

	It("Request Vote", func() {
		req := VoteRequest{
			Term:        1,
			CandidateID: 1,
		}
		ch := make(chan VoteResponse, 1)

		comm.RequestVote(address, req, ch)
		resp := <-ch
		Expect(resp.Error).Should(Succeed())
		Expect(resp.Term).Should(BeEquivalentTo(1))
		Expect(resp.NodeID).Should(BeEquivalentTo(1))
		Expect(resp.NodeAddress).Should(Equal(address))
		Expect(resp.VoteGranted).Should(BeTrue())
	})

	It("Append", func() {
		ar := AppendRequest{
			Term:     1,
			LeaderID: 1,
		}
		expectedEntries = nil

		aresp, err := comm.Append(address, ar)
		Expect(err).Should(Succeed())
		Expect(aresp.Error).Should(Succeed())
		Expect(aresp.Term).Should(BeEquivalentTo(1))
		Expect(aresp.Success).Should(BeTrue())
	})

	It("Append 2", func() {
		ar := AppendRequest{
			Term:     2,
			LeaderID: 1,
		}
		e := storage.Entry{
			Index:     1,
			Term:      2,
			Timestamp: time.Now(),
			Tags:      []string{"foo"},
			Data:      []byte("Hello!"),
		}
		ar.Entries = append(ar.Entries, e)
		e2 := storage.Entry{
			Index:     2,
			Term:      3,
			Timestamp: time.Now(),
			Tags:      []string{"bar", "baz"},
			Data:      []byte("Goodbye!"),
		}
		ar.Entries = append(ar.Entries, e2)

		expectedLock.Lock()
		expectedEntries = ar.Entries
		expectedLock.Unlock()

		aresp, err := comm.Append(address, ar)
		Expect(err).Should(Succeed())
		Expect(aresp.Error).Should(Succeed())
		Expect(aresp.Term).Should(BeEquivalentTo(2))
		Expect(aresp.Success).Should(BeFalse())
	})

	It("Propose", func() {
		e3 := storage.Entry{
			Timestamp: time.Now(),
			Index:     3,
			Term:      3,
			Data:      []byte("Hello, World!"),
		}

		expectedLock.Lock()
		expectedEntries = make([]storage.Entry, 1)
		expectedEntries[0] = e3
		expectedLock.Unlock()

		presp, err := comm.Propose(address, e3)
		Expect(err).Should(Succeed())
		Expect(presp.Error).Should(Succeed())
		Expect(presp.NewIndex).ShouldNot(BeZero())
	})
})

type testImpl struct {
}

func makeTestRaft() *testImpl {
	return &testImpl{}
}

func (r *testImpl) MyID() NodeID {
	return 1
}

func (r *testImpl) RequestVote(req VoteRequest) (VoteResponse, error) {
	vr := VoteResponse{
		Term:        req.Term,
		VoteGranted: req.Term == 1,
	}
	return vr, nil
}

func (r *testImpl) Append(req AppendRequest) (AppendResponse, error) {
	expectedLock.Lock()
	defer expectedLock.Unlock()

	vr := AppendResponse{
		Term:    req.Term,
		Success: req.Term == 1,
	}

	for i, e := range req.Entries {
		ee := expectedEntries[i]
		if e.Index != ee.Index {
			vr.Error = fmt.Errorf("%d: Expected index %d and got %d", i, expectedEntries[i].Index, e.Index)
		}
		if e.Term != ee.Term {
			vr.Error = fmt.Errorf("Terms do not match")
		}
		if e.Timestamp != ee.Timestamp {
			vr.Error = fmt.Errorf("Timestamps do not match")
		}
		if !bytes.Equal(e.Data, expectedEntries[i].Data) {
			vr.Error = fmt.Errorf("Bytes do not match")
		}
		if !reflect.DeepEqual(e.Tags, expectedEntries[i].Tags) {
			vr.Error = fmt.Errorf("Tags do not match")
		}
	}
	return vr, nil
}

func (r *testImpl) Propose(e storage.Entry) (uint64, error) {
	expectedLock.Lock()
	defer expectedLock.Unlock()

	ee := expectedEntries[0]
	if ee.Index != e.Index {
		return 0, errors.New("Incorrect index")
	}
	if ee.Term != e.Term {
		return 0, errors.New("Incorrect term")
	}
	if ee.Timestamp != e.Timestamp {
		return 0, errors.New("Incorrect timestamp")
	}
	if !bytes.Equal(ee.Data, e.Data) {
		return 0, errors.New("Incorrect data")
	}

	return 123, nil
}
