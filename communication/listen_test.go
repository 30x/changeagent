package communication

import (
	"fmt"
	"time"

	"github.com/30x/changeagent/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Listening Tests", func() {
	var comm1 Communication
	var comm2 Communication
	var entry common.Entry

	BeforeEach(func() {
		entry = common.Entry{
			Data:      []byte("Hello!"),
			Timestamp: time.Now(),
		}
		expectedLock.Lock()
		expectedEntries = []common.Entry{entry}
		expectedLock.Unlock()
	})

	AfterEach(func() {
		if comm1 != nil {
			comm1.Close()
			comm1 = nil
		}
		if comm2 != nil {
			comm2.Close()
			comm2 = nil
		}
	})

	It("Listen separate port", func() {
		var err error
		comm1, err = StartSeparateCommunication(0)
		Expect(err).Should(Succeed())
		Expect(comm1.Port()).ShouldNot(BeZero())
		comm1.SetRaft(makeTestRaft())

		comm2, err = StartSeparateCommunication(0)
		Expect(err).Should(Succeed())
		Expect(comm2.Port()).ShouldNot(BeZero())
		comm2.SetRaft(makeTestRaft())

		resp, err := comm1.Propose(fmt.Sprintf("localhost:%d", comm2.Port()), &entry)
		Expect(err).Should(Succeed())
		Expect(resp.Error).Should(Succeed())
	})

	It("Listen secure port", func() {
		var err error
		comm1, err = StartSecureCommunication(0,
			"../testkeys/clusterkey.pem", "../testkeys/clustercert.pem",
			"../testkeys/CA/certs/cacert.pem")
		Expect(err).Should(Succeed())
		Expect(comm1.Port()).ShouldNot(BeZero())
		comm1.SetRaft(makeTestRaft())

		comm2, err = StartSecureCommunication(0,
			"../testkeys/clusterkey.pem", "../testkeys/clustercert.pem",
			"../testkeys/CA/certs/cacert.pem")
		Expect(err).Should(Succeed())
		Expect(comm2.Port()).ShouldNot(BeZero())
		comm2.SetRaft(makeTestRaft())

		resp, err := comm1.Propose(fmt.Sprintf("localhost:%d", comm2.Port()), &entry)
		Expect(err).Should(Succeed())
		Expect(resp.Error).Should(Succeed())
	})

	It("Listen Bad Server Cert", func() {
		var err error
		comm1, err = StartSecureCommunication(0,
			"../testkeys/clusterkey.pem", "../testkeys/clustercert.pem",
			"../testkeys/CA/certs/cacert.pem")
		Expect(err).Should(Succeed())
		Expect(comm1.Port()).ShouldNot(BeZero())
		comm1.SetRaft(makeTestRaft())

		comm2, err = StartSecureCommunication(0,
			"../testkeys/selfkey.pem", "../testkeys/selfcert.pem",
			"../testkeys/selfcert.pem")
		Expect(err).Should(Succeed())
		Expect(comm2.Port()).ShouldNot(BeZero())
		comm2.SetRaft(makeTestRaft())

		_, err = comm1.Propose(fmt.Sprintf("localhost:%d", comm2.Port()), &entry)
		Expect(err).ShouldNot(Succeed())
	})

	It("Listen Bad Client Cert", func() {
		var err error
		comm1, err = StartSecureCommunication(0,
			"../testkeys/selfkey.pem", "../testkeys/selfcert.pem",
			"../testkeys/CA/certs/cacert.pem")
		Expect(err).Should(Succeed())
		Expect(comm1.Port()).ShouldNot(BeZero())
		comm1.SetRaft(makeTestRaft())

		comm2, err = StartSecureCommunication(0,
			"../testkeys/clusterkey.pem", "../testkeys/clustercert.pem",
			"../testkeys/CA/certs/cacert.pem")
		Expect(err).Should(Succeed())
		Expect(comm2.Port()).ShouldNot(BeZero())
		comm2.SetRaft(makeTestRaft())

		_, err = comm1.Propose(fmt.Sprintf("localhost:%d", comm2.Port()), &entry)
		Expect(err).ShouldNot(Succeed())
	})
})
