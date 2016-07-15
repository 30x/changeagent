package raft

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	startIndex         = 2
	maxWait            = 10 * time.Microsecond
	trackerTestTimeout = 10 * time.Second
	trackerCount       = 10000
)

var tracker *ChangeTracker

var _ = Describe("Change tracker", func() {
	BeforeEach(func() {
		tracker = CreateTracker()
		tracker.Update(startIndex)
	})

	AfterEach(func() {
		tracker.Close()
	})

	It("Behind", func() {
		behind := tracker.Wait(1)
		Expect(behind).Should(BeEquivalentTo(2))
	})

	It("Caught up", func() {
		behind := tracker.Wait(2)
		Expect(behind).Should(BeEquivalentTo(2))
	})

	It("Timeout", func() {
		blocked := tracker.TimedWait(3, 250*time.Millisecond)
		Expect(blocked).Should(BeEquivalentTo(2))
	})

	It("Up to date", func() {
		doneChan := make(chan uint64, 1)

		go func() {
			new := tracker.Wait(3)
			doneChan <- new
		}()

		tracker.Update(3)
		gotVal := <-doneChan
		Expect(gotVal).Should(BeEquivalentTo(3))
	})

	It("Up to date with timeout", func() {
		doneChan := make(chan uint64, 1)

		go func() {
			new := tracker.TimedWait(3, 2*time.Second)
			doneChan <- new
		}()

		tracker.Update(3)
		gotVal := <-doneChan
		Expect(gotVal).Should(BeEquivalentTo(3))
	})

	It("Up to date timeout", func() {
		doneChan := make(chan uint64, 1)

		go func() {
			new := tracker.TimedWait(3, 250*time.Millisecond)
			doneChan <- new
		}()

		time.Sleep(1 * time.Second)
		tracker.Update(3)
		gotVal := <-doneChan
		Expect(gotVal).Should(BeEquivalentTo(2))
	})

	It("Update", func() {
		doneChan := make(chan uint64, 1)

		go func() {
			new := tracker.Wait(4)
			doneChan <- new
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.Update(3)
		time.Sleep(250 * time.Millisecond)
		tracker.Update(4)
		gotVal := <-doneChan
		Expect(gotVal).Should(BeEquivalentTo(4))
	})

	It("Update twice", func() {
		doneChan := make(chan uint64, 1)
		doneChan2 := make(chan uint64, 1)

		go func() {
			new := tracker.Wait(4)
			doneChan <- new
		}()

		go func() {
			new2 := tracker.Wait(4)
			doneChan2 <- new2
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.Update(3)
		time.Sleep(250 * time.Millisecond)
		tracker.Update(4)
		gotVal := <-doneChan
		Expect(gotVal).Should(BeEquivalentTo(4))
		gotVal = <-doneChan2
		Expect(gotVal).Should(BeEquivalentTo(4))
	})

	It("Multi Update", func() {
		prematureDoneChan := make(chan uint64, 1)
		doneChan := make(chan uint64, 1)

		go func() {
			oldNew := tracker.Wait(10)
			prematureDoneChan <- oldNew
		}()

		go func() {
			new := tracker.Wait(4)
			doneChan <- new
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.Update(3)
		time.Sleep(250 * time.Millisecond)
		tracker.Update(4)

		// No loop -- we expect that the first case arrive before the second
		select {
		case gotVal := <-doneChan:
			Expect(gotVal).Should(BeEquivalentTo(4))
		case <-prematureDoneChan:
			Expect(true).Should(BeFalse())
		}
	})

	It("Close", func() {
		tracker := CreateTracker()
		tracker.Update(2)
		done := make(chan uint64, 1)

		go func() {
			new := tracker.Wait(3)
			done <- new
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.Close()

		val := <-done
		Expect(val).Should(BeEquivalentTo(2))
	})

	It("Naming", func() {
		name1 := GetNamedTracker("test1")
		name2 := GetNamedTracker("test2")
		name1same := GetNamedTracker("test1")
		Expect(name1).Should(Equal(name1same))
		Expect(name1).ShouldNot(Equal(name2))
	})

	It("Stress 1, 1", func() {
		trackerStress(1, 1, trackerCount)
	})

	It("Stress 100, 1", func() {
		trackerStress(100, 1, trackerCount)
	})

	It("Stress 1, 100", func() {
		trackerStress(1, 100, trackerCount)
	})

	It("Stress 1, 1000", func() {
		trackerStress(1, 1000, trackerCount)
	})

	It("Stress 100, 100", func() {
		trackerStress(100, 100, trackerCount)
	})

	It("Stress 100, 1000", func() {
		trackerStress(100, 1000, trackerCount)
	})
})

func trackerStress(producers, consumers int, max uint64) {
	var start uint64 = startIndex

	prodDone := make(chan bool)
	consDone := make(chan bool)

	for i := 0; i <= producers; i++ {
		go func() {
			for atomic.LoadUint64(&start) < max {
				waitTime := rand.Int63n(int64(maxWait))
				time.Sleep(time.Duration(waitTime))
				val := atomic.AddUint64(&start, 1)
				tracker.Update(val)
			}
			prodDone <- true
		}()
	}

	for i := 0; i <= consumers; i++ {
		go func() {
			var last uint64 = startIndex
			for last < max {
				waitTime := rand.Int63n(int64(maxWait))
				last = tracker.TimedWait(last+1, time.Duration(waitTime))
			}
			consDone <- true
		}()
	}

	prodCount := 0
	consCount := 0

	timeout := time.NewTimer(trackerTestTimeout)
	for prodCount < producers || consCount < consumers {
		select {
		case <-prodDone:
			prodCount++
		case <-consDone:
			consCount++
		case <-timeout.C:
			fmt.Fprintf(GinkgoWriter,
				"Test timed out after %d producers and %d consumers\n",
				prodCount, consCount)
			buf := make([]byte, 1024*1024)
			stackLen := runtime.Stack(buf, true)
			fmt.Println(string(buf[:stackLen]))
			Expect(false).Should(BeTrue())
			return
		}
	}
}
