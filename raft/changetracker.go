package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	closed = iota
	newWaiter
	cancelWaiter
	update
)

type trackerUpdate struct {
	updateType int
	key        int32
	change     uint64
	waiter     changeWaiter
}

type changeWaiter struct {
	change uint64
	rc     chan uint64
}

/*
A ChangeTracker allows clients to submit a change, and to wait for a change
to occur. The overall effect is like a condition variable, in that waiters
are notified when something changes. This work is done using a goroutine,
which is simpler and faster than the equivalent using a condition variable.
*/
type ChangeTracker struct {
	updateChan chan trackerUpdate
	lastKey    int32
	waiters    map[int32]changeWaiter
	lastChange uint64
}

var changeTrackers = make(map[string]*ChangeTracker)
var trackerLock = &sync.Mutex{}

/*
CreateTracker creates a new change tracker with "lastChange" set to zero.
*/
func CreateTracker() *ChangeTracker {
	tracker := &ChangeTracker{
		updateChan: make(chan trackerUpdate, 100),
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
	u := trackerUpdate{
		updateType: closed,
	}
	t.updateChan <- u
}

/*
Update indicates that the current sequence has changed. Wake up any waiting
waiters and tell them about it.
*/
func (t *ChangeTracker) Update(change uint64) {
	u := trackerUpdate{
		updateType: update,
		change:     change,
	}
	t.updateChan <- u
}

/*
Wait blocks the calling gorouting forever until the change tracker has reached a value at least as high as
"curChange." Return the current value when that happens.
*/
func (t *ChangeTracker) Wait(curChange uint64) uint64 {
	_, resultChan := t.doWait(curChange)
	return <-resultChan
}

/*
TimedWait blocks the current gorouting until either a new value higher than
"curChange" has been reached, or "maxWait" has been exceeded.
*/
func (t *ChangeTracker) TimedWait(curChange uint64, maxWait time.Duration) uint64 {
	key, resultChan := t.doWait(curChange)
	timer := time.NewTimer(maxWait)
	select {
	case result := <-resultChan:
		return result
	case <-timer.C:
		u := trackerUpdate{
			updateType: cancelWaiter,
			key:        key,
		}
		t.updateChan <- u
		return 0
	}
}

func (t *ChangeTracker) doWait(curChange uint64) (int32, chan uint64) {
	key := atomic.AddInt32(&t.lastKey, 1)
	resultChan := make(chan uint64, 1)
	u := trackerUpdate{
		updateType: newWaiter,
		key:        key,
		waiter: changeWaiter{
			change: curChange,
			rc:     resultChan,
		},
	}
	t.updateChan <- u
	return key, resultChan
}

/*
 * This is the goroutine. It receives updates for new waiters, and updates
 * for new sequences, and distributes them appropriately.
 */
func (t *ChangeTracker) run() {
	t.waiters = make(map[int32]changeWaiter)

	running := true
	for running {
		up := <-t.updateChan
		switch up.updateType {
		case closed:
			running = false
		case update:
			t.handleUpdate(up.change)
		case cancelWaiter:
			t.handleCancel(up.key)
		case newWaiter:
			t.handleWaiter(up)
		}
	}

	// Close out all waiting waiters
	for _, w := range t.waiters {
		w.rc <- t.lastChange
	}
}

func (t *ChangeTracker) handleUpdate(change uint64) {
	t.lastChange = change
	for k, w := range t.waiters {
		if change >= w.change {
			w.rc <- change
			delete(t.waiters, k)
		}
	}
}

func (t *ChangeTracker) handleWaiter(u trackerUpdate) {
	if t.lastChange >= u.waiter.change {
		u.waiter.rc <- t.lastChange
	} else {
		t.waiters[u.key] = u.waiter
	}
}

func (t *ChangeTracker) handleCancel(key int32) {
	delete(t.waiters, key)
}
