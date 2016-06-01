package storage

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Truncation test", func() {
	var truncateStore Storage

	BeforeEach(func() {
		stor, err := CreateRocksDBStorage("./truncatetest", 100)
		Expect(err).Should(Succeed())
		truncateStore = stor
	})

	AfterEach(func() {
		truncateStore.Close()
		err := truncateStore.Delete()
		Expect(err).Should(Succeed())
	})

	It("Truncate empty", func() {
		count, err := truncateStore.Truncate(1000, time.Minute)
		Expect(err).Should(Succeed())
		Expect(count).Should(BeEquivalentTo(0))
	})

	It("Truncate by count", func() {
		fillRecords(truncateStore, 1, 100, time.Now())
		countEntries(truncateStore, 1, 100)

		deleted, err := truncateStore.Truncate(50, 0)
		Expect(err).Should(Succeed())
		Expect(deleted).Should(BeEquivalentTo(50))
		countEntries(truncateStore, 51, 50)
	})

	It("Truncate nothing", func() {
		fillRecords(truncateStore, 1, 100, time.Now())

		deleted, err := truncateStore.Truncate(100, 0)
		Expect(err).Should(Succeed())
		Expect(deleted).Should(BeEquivalentTo(0))
		countEntries(truncateStore, 1, 100)
	})

	It("Truncate all", func() {
		fillRecords(truncateStore, 1, 100, time.Now())

		deleted, err := truncateStore.Truncate(0, 0)
		Expect(err).Should(Succeed())
		Expect(deleted).Should(BeEquivalentTo(100))

		deleted, err = truncateStore.Truncate(0, 0)
		Expect(err).Should(Succeed())
		Expect(deleted).Should(BeEquivalentTo(0))
	})

	It("Truncate by time", func() {
		now := time.Now()
		tenBack := now.Add(-10 * time.Minute)
		fiveBack := now.Add(-5 * time.Minute)
		oneBack := now.Add(-1 * time.Minute)

		fillRecords(truncateStore, 1, 10, tenBack)
		fillRecords(truncateStore, 11, 10, fiveBack)
		fillRecords(truncateStore, 21, 10, oneBack)
		countEntries(truncateStore, 1, 30)

		deleted, err := truncateStore.Truncate(2, 6*time.Minute)
		Expect(err).Should(Succeed())
		Expect(deleted).Should(BeEquivalentTo(10))
		countEntries(truncateStore, 11, 20)

		deleted, err = truncateStore.Truncate(2, 2*time.Minute)
		Expect(err).Should(Succeed())
		Expect(deleted).Should(BeEquivalentTo(10))
		countEntries(truncateStore, 21, 10)
	})
})

func fillRecords(s Storage, startIndex uint64, count int, rt time.Time) {
	ix := startIndex
	for i := 0; i < count; i++ {
		//fmt.Fprintf(GinkgoWriter, "Inserting %d\n", ix)
		e := Entry{
			Index:     ix,
			Timestamp: rt,
			Data:      []byte("Truncation test"),
		}
		err := s.AppendEntry(&e)
		Expect(err).Should(Succeed())
		ix++
	}
	Expect(ix).Should(Equal(startIndex + uint64(count)))
}

func countEntries(s Storage, startIx uint64, count uint) {
	lastIx := startIx + uint64(count)
	for ix := startIx; ix < lastIx; ix++ {
		fmt.Fprintf(GinkgoWriter, "Fetching %d\n", ix)
		e, err := s.GetEntry(ix)
		Expect(err).Should(Succeed())
		Expect(e).ShouldNot(BeNil())
		Expect(e.Index).Should(Equal(ix))
	}
}
