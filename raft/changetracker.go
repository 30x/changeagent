package raft

import (
  "sync"
  "time"
  "math"
  "container/heap"
)

/*
 * This module is like a condition variable in that it tracks a number,
 * and lets callers atomically wait if it hasn't changed since last time.
 * We could use a condition variable here but we're going to try and be
 * go-like and use channels instead.
 */

 type changeWaiter struct {
   change uint64
   timeout time.Time
   waiter chan uint64
 }

 type changeHeap struct {
   items []changeWaiter
 }

type ChangeTracker struct {
  lastChange uint64
  updateChan chan uint64
  waiterChan chan changeWaiter
  stopChan chan bool
  waiters *changeHeap
}

var changeTrackers map[string]*ChangeTracker = make(map[string]*ChangeTracker)
var trackerLock *sync.Mutex = new(sync.Mutex)
var timeMax time.Time = time.Unix(1 << 40, 0)

func CreateTracker(lastChange uint64) *ChangeTracker {
  waiters := &changeHeap{}
  heap.Init(waiters)

  tracker := &ChangeTracker{
    lastChange: lastChange,
    updateChan: make(chan uint64, 1),
    waiterChan: make(chan changeWaiter, 1),
    stopChan: make(chan bool, 1),
    waiters: waiters,
  }
  go tracker.run()
  return tracker
}

func GetNamedTracker(name string) *ChangeTracker {
  trackerLock.Lock()
  defer trackerLock.Unlock()

  ret := changeTrackers[name]
  if ret == nil {
    ret = CreateTracker(0)
    changeTrackers[name] = ret
  }
  return ret
}

/*
 * Stop the change tracker from delivering notifications.
 */
func (t* ChangeTracker) Close() {
  t.stopChan <- true
}

/*
 * Indicate that the current sequence has changed. Wake up any waiting
 * waiters and tell them about it.
 */
func (t *ChangeTracker) Update(change uint64) {
  t.updateChan <- change
}

/*
 * Wait forever until the change tracker has reached a value at least as high as
 * "curChange." Return the current value when that happens.
 */
func (t *ChangeTracker) Wait(curChange uint64) uint64 {
  return t.doWait(curChange, timeMax)
}

/*
 * Wait for a certain time, just like "wait". If the timeout expires then
 * we will return the current value.
 */
func (t *ChangeTracker) TimedWait(curChange uint64, maxWait time.Duration) uint64 {
  timeout := time.Now().Add(maxWait)
  return t.doWait(curChange, timeout)
}

func (t *ChangeTracker) doWait(curChange uint64, timeout time.Time) uint64 {
  waitMe := make(chan uint64, 1)
  w := changeWaiter{
    change: curChange,
    waiter: waitMe,
    timeout: timeout,
  }
  t.waiterChan <- w
  changed := <- waitMe
  return changed
}

/*
 * This is the goroutine. It receives updates for new waiters, and updates
 * for new sequences, and distributes them appropriately.
 */
func (t *ChangeTracker) run() {
  running := true

  for running {
    now := time.Now()
    var sleepTime time.Duration
    if t.waiters.Len() == 0 {
      sleepTime = math.MaxInt64
    } else {
      sleepTime = t.waiters.items[0].timeout.Sub(now)
    }

    if sleepTime <= 0 {
      t.handleTimeout(now)

    } else {
      timer := time.NewTimer(sleepTime)
      select {
        case update := <- t.updateChan:
          t.handleUpdate(update)
        case waiter := <- t.waiterChan:
          t.handleWaiter(waiter)
        case <- timer.C:
          t.handleTimeout(now)
        case <- t.stopChan:
          running = false
      }
      timer.Stop()
    }
  }

  // Close out waiting waiters on close
  t.handleUpdate(t.lastChange)
}

func (t *ChangeTracker) handleUpdate(change uint64) {
  // Need to cycle through all changes and only remove those that should be waiting
  t.lastChange = change
  i := 0
  for i < len(t.waiters.items) {
    w := t.waiters.items[i]
    if change >= w.change {
      heap.Remove(t.waiters, i)
      w.waiter <- change
    } else {
      i++
    }
  }
}

func (t *ChangeTracker) handleWaiter(w changeWaiter) {
  if t.lastChange >= w.change {
    w.waiter <- t.lastChange
  } else {
    heap.Push(t.waiters, w)
  }
}

func (t *ChangeTracker) handleTimeout(now time.Time) {
  for t.waiters.Len() > 0 {
    it := t.waiters.items[0].timeout
    if it == now || it.Before(now) {
      w := t.waiters.Pop().(changeWaiter)
      w.waiter <- t.lastChange
    } else {
      return
    }
  }
}

// Implementation needed by the heap

func (h* changeHeap) Len() int {
  return len(h.items)
}

func (h* changeHeap) Less(i, j int) bool {
  return h.items[i].timeout.Before(h.items[j].timeout)
}

func (h* changeHeap) Swap(i, j int) {
  tmp := h.items[i]
  h.items[i] = h.items[j]
  h.items[j] = tmp
}

func (h* changeHeap) Push(val interface{}) {
  h.items = append(h.items, val.(changeWaiter))
}

func (h* changeHeap) Pop() interface{} {
  ret := h.items[0]
  h.items = h.items[1:]
  return ret
}
