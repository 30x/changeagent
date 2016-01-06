package raft

import (
  "testing"
  "time"
)

func TestTrackerBehind(t *testing.T) {
  tracker := CreateTracker(2)
  behind := tracker.Wait(1)
  if behind != 2 {
    t.Fatal("expected 2")
  }
  tracker.Close()
}

func TestTrackerCaughtUp(t *testing.T) {
  tracker := CreateTracker(2)
  behind := tracker.Wait(2)
  if behind != 2 {
    t.Fatal("expected 2")
  }
  tracker.Close()
}

func TestTrackerUpToDate(t *testing.T) {
  tracker := CreateTracker(2)
  doneChan := make(chan uint64, 1)

  go func() {
    new := tracker.Wait(3)
    doneChan <- new
  }()

  tracker.Update(3)
  gotVal := <- doneChan
  if gotVal != 3 {
    t.Fatal("Expected routine not to finish until we got to 3")
  }
  tracker.Close()
}

func TestTrackerUpToDateWithTimeout(t *testing.T) {
  tracker := CreateTracker(2)
  doneChan := make(chan uint64, 1)

  go func() {
    new := tracker.TimedWait(3, 2 * time.Second)
    doneChan <- new
  }()

  tracker.Update(3)
  gotVal := <- doneChan
  if gotVal != 3 {
    t.Fatal("Expected routine not to finish until we got to 3")
  }
  tracker.Close()
}

func TestTrackerUpToDateTimeout(t *testing.T) {
  tracker := CreateTracker(2)
  doneChan := make(chan uint64, 1)

  go func() {
    new := tracker.TimedWait(3, 500 * time.Millisecond)
    doneChan <- new
  }()

  time.Sleep(1 * time.Second)
  tracker.Update(3)
  gotVal := <- doneChan
  if gotVal != 2 {
    t.Fatal("Expected routine to time out and stay at 2")
  }
  tracker.Close()
}

func TestTrackerUpdate(t *testing.T) {
  tracker := CreateTracker(2)
  doneChan := make(chan uint64, 1)

  go func() {
    new := tracker.Wait(4)
    doneChan <- new
  }()

  time.Sleep(250 * time.Millisecond)
  tracker.Update(3)
  time.Sleep(250 * time.Millisecond)
  tracker.Update(4)
  gotVal := <- doneChan
  if gotVal != 4 {
    t.Fatal("Expected routine not to finish until we got to 3")
  }
  tracker.Close()
}

func TestTrackerMultiUpdate(t *testing.T) {
  tracker := CreateTracker(2)
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

  select {
  case gotVal := <- doneChan:
    if gotVal != 4 {
      t.Fatal("Expected routine not to finish until we got to 3")
    }
  case <- prematureDoneChan:
    t.Fatal("Should not be done already with older waiter")
  }
  tracker.Close()
}


func TestTrackerClose(t *testing.T) {
  tracker := CreateTracker(2)

  go func() {
    new := tracker.Wait(3)
    if new != 2 {
      t.Fatal("Expected 2")
    }
  }()

  tracker.Close()
}

func TestTrackerNaming(t *testing.T) {
  name1 := GetNamedTracker("test1")
  name2 := GetNamedTracker("test2")
  name1same := GetNamedTracker("test1")
  if name1 != name1same {
    t.Fatal("Should get the same tracker every time")
  }
  if name1 == name2 {
    t.Fatal("Should have two different trackers")
  }
}
