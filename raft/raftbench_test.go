package raft

import (
  "fmt"
  "testing"
  "time"
  "revision.aeip.apigee.net/greg/changeagent/log"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

func BenchmarkSlowAppends(b *testing.B) {
  doAppendBenchmark(b, 1)
}

func BenchmarkMediumAppends(b *testing.B) {
  doAppendBenchmark(b, 10)
}

func BenchmarkBatchedAppends(b *testing.B) {
  doAppendBenchmark(b, 100)
}

func doAppendBenchmark(b *testing.B, waitFrequency int) {
  waitForLeader()
  leader := getLeader()
  if leader == nil { b.Fatal("No leaders found") }
  b.ResetTimer()

  log.Debugf("Gonna benchmark %d iterations", b.N)
  lastIndex, _ := leader.GetLastIndex()
  for i := 0; i < b.N; i++ {
    proposal := fmt.Sprintf("Benchmark entry %d", i)
    newEntry := storage.Entry{
      Timestamp: time.Now(),
      Data: []byte(proposal),
    }
    newIndex, err := leader.Propose(&newEntry)
    if err != nil { b.Fatalf("Error on proposal: %v", err) }
    if newIndex != (lastIndex + 1) {
      b.Fatalf("Expected new index of %d rather than %d", lastIndex + 1, newIndex)
    }
    lastIndex++
    if (waitFrequency == 1) || ((i > 0) && ((i % waitFrequency) == 0)) {
      //log.Debugf("Iteration %d. Waiting for changes up to %d", i, lastIndex)
      leader.GetAppliedTracker().Wait(lastIndex)
    }
  }
  log.Debug("Done.")
}
