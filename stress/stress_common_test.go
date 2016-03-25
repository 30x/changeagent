package stress

import (
  "encoding/json"
  "fmt"
  "io/ioutil"
  "net/http"
  "os"
  "os/exec"
  "strconv"
  "time"
  . "github.com/onsi/ginkgo"
)

type RaftState struct {
  State string `json:"state"`
  Leader uint64 `json:"leader"`
}

func launchAgent(id int, port int, dataDir string) (*os.Process, error) {
  cmd := exec.Command(
    "../agent/agent",
    "-s", "./disco",
    "-id", strconv.Itoa(id),
    "-p", strconv.Itoa(port),
    "-d", dataDir,
    "-logtostderr")
  cmd.Stdout = os.Stdout
  cmd.Stderr = os.Stderr

  err := cmd.Start()
  if err == nil {
    testPids[cmd.Process.Pid] = *cmd.Process
    return cmd.Process, nil
  }
  return nil, err
}

func killAgent(proc *os.Process) {
  delete(testPids, proc.Pid)
  proc.Kill()
  proc.Release()
}

func getRaftState(port int, maxWait int) (string, uint64, error) {
  uri := fmt.Sprintf("http://localhost:%d/diagnostics/raft", port)

  var lastCode int
  var lastErr error
  var lastStatus string
  for i := 0; i < maxWait; i++ {
    resp, err := http.Get(uri)
    if err == nil && resp.StatusCode == http.StatusOK {
      body, err := ioutil.ReadAll(resp.Body)
      if err == nil {
        var state RaftState
        err = json.Unmarshal(body, &state)
        return state.State, state.Leader, nil
      }
    } else {
      if err == nil {
        lastCode = resp.StatusCode
      }
      lastErr = err
    }
    time.Sleep(time.Second)
  }

  return "", 0, fmt.Errorf("Raft peer bad status %d after %d seconds: code = %d err = %s",
    lastStatus, maxWait, lastCode, lastErr)
}

func waitForLeader(ports []int, maxWait int) error {
  states := make([]string, len(ports))
  leaders := make([]uint64, len(ports))

  for i := 0; i < maxWait; i++ {
    for p, port := range(ports) {
      state, leader, err := getRaftState(port, 5)
      if err == nil {
        states[p] = state
        leaders[p] = leader
      } else {
        states[p] = fmt.Sprintf("%s", err)
      }
    }

    fmt.Fprintf(GinkgoWriter, "Server states: %v\n", states)
    fmt.Fprintf(GinkgoWriter, "Leaders: %v\n", leaders)

    leaderCount := 0
    for _, state := range(states) {
      if state == "Leader" {
        leaderCount++
      }
    }

    if leaderCount == 1 {
      leaderId := leaders[0]
      leaderIdCount := 1
      for i := 1; i < len(leaders); i++ {
        if leaders[i] == leaderId {
          leaderIdCount++
        }
      }
      if leaderId != 0 && leaderIdCount == len(leaders) {
        return nil
      }
    }
    if leaderCount > 1 {
      return fmt.Errorf("%d leaders found rather than just one", leaders)
    }

    time.Sleep(time.Second)
  }

  return fmt.Errorf("No leader identified after %d seconds", maxWait)
}
