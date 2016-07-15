package raft

import (
	"sync"
	"time"
)

type changeWaiter struct {
	change     uint64
	resultChan chan changeResult
}

type changeResult struct {
	key    int
	result uint64
}

/*
A ChangeTracker allows clients to submit a change, and to wait for a change
to occur. The overall effect is like a condition variable, in that waiters
are notified when something changes. This work is done using a goroutine,
which is simpler and faster than the equivalent using a condition variable.
*/
type ChangeTracker struct {
	updateChan chan uint64
	waiterChan chan changeWaiter
	cancelChan chan int
	stopChan   chan bool
	lastKey    int
	waiters    map[int]changeWaiter
	lastChange uint64
}

var changeTrackers = make(map[string]*ChangeTracker)
var trackerLock = &sync.Mutex{}

/*
CreateTracker creates a new change tracker with "lastChange" set to zero.
*/
func CreateTracker() *ChangeTracker {
	tracker := &ChangeTracker{
		updateChan: make(chan uint64, 100),
		waiterChan: make(chan changeWaiter, 100),
		cancelChan: make(chan int, 100),
		stopChan:   make(chan bool, 1),
	}
	go tracker.run()
	return tracker
}

/*
GetNamedTracker retrieves a tracker from a thread-safe global table of names
trackers. If no tracker with the specified name exists, then one is created.
*/
func GetNamedTracker(name string) *ChangeTracker {
	trackerLock.Lock()
	defer trackerLock.Unlock()

	ret := changeTrackers[name]
	if ret == nil {
		ret = CreateTracker()
		changeTrackers[name] = ret
	}
	return ret
}

/*
Close stops the change tracker from delivering notifications.
*/
func (t *ChangeTracker) Close() {
	t.stopChan <- true
}

/*
Update indicates that the current sequence has changed. Wake up any waiting
waiters and tell them about it.
*/
func (t *ChangeTracker) Update(change uint64) {
	t.updateChan <- change
}

/*
Wait blocks the calling gorouting forever until the change tracker has reached a value at least as high as
"curChange." Return the current value when that happens.
*/
func (t *ChangeTracker) Wait(curChange uint64) uint64 {
	resultChan := t.doWait(curChange)
	firstResult := <-resultChan

	if firstResult.key < 0 {
		// Got a result already
		return firstResult.result
	}

	// Need to wait for a second result
	result := <-resultChan
	return result.result
}

/*
TimedWait blocks the current gorouting until either a new value higher than
"curChange" has been reached, or "maxWait" has been exceeded.
*/
func (t *ChangeTracker) TimedWait(curChange uint64, maxWait time.Duration) uint64 {
	resultChan := t.doWait(curChange)
	firstResult := <-resultChan

	if firstResult.key < 0 {
		// Got a result already
		return firstResult.result
	}

	// Need to wait for a second result
	timer := time.NewTimer(maxWait)
	select {
	case result := <-resultChan:
		return result.result
	case <-timer.C:
		t.cancelChan <- firstResult.key
		return firstResult.result
	}
}

func (t *ChangeTracker) doWait(curChange uint64) chan changeResult {
	resultChan := make(chan changeResult, 1)
	w := changeWaiter{
		change:     curChange,
		resultChan: resultChan,
	}
	t.waiterChan <- w

	return resultChan
}

/*
 * This is the goroutine. It receives updates for new waiters, and updates
 * for new sequences, and distributes them appropriately.
 */
func (t *ChangeTracker) run() {
	t.waiters = make(map[int]changeWaiter)

	running := true
	for running {
		select {
		case update := <-t.updateChan:
			t.handleUpdate(update)
		case waiter := <-t.waiterChan:
			t.handleWaiter(waiter)
		case cancelKey := <-t.cancelChan:
			t.handleCancel(cancelKey)
		case <-t.stopChan:
			running = false
		}
	}

	// Close out all waiting waiters
	for _, w := range t.waiters {
		r := changeResult{
			key:    -1,
			result: t.lastChange,
		}
		w.resultChan <- r
	}

	close(t.updateChan)
	close(t.waiterChan)
	close(t.cancelChan)
	close(t.stopChan)
}

func (t *ChangeTracker) handleUpdate(change uint64) {
	t.lastChange = change
	for k, w := range t.waiters {
		if change >= w.change {
			r := changeResult{
				key:    -1,
				result: change,
			}
			w.resultChan <- r
			delete(t.waiters, k)
		}
	}
}

func (t *ChangeTracker) handleWaiter(w changeWaiter) {
	if t.lastChange >= w.change {
		r := changeResult{
			key:    -1,
			result: t.lastChange,
		}
		w.resultChan <- r

	} else {
		key := t.lastKey
		r := changeResult{
			key:    key,
			result: t.lastChange,
		}
		t.lastKey++
		t.waiters[key] = w
		w.resultChan <- r
	}
}

func (t *ChangeTracker) handleCancel(key int) {
	delete(t.waiters, key)
}
