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

	"github.com/30x/changeagent/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	DebugNodes = false
)

type RaftState struct {
	State  string `json:"state"`
	Leader string `json:"leader"`
}

func launchAgent(port int, dataDir string) (*os.Process, error) {
	cmd := exec.Command("../changeagent")
	args := []string{
		"../agent/agent",
		"-p", strconv.Itoa(port),
		"-d", dataDir,
		"-logtostderr",
	}
	if DebugNodes {
		args = append(args, "-v")
		args = append(args, "5")
	}

	cmd.Args = args
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Fprintf(GinkgoWriter, "** Launching agent on port %d\n", port)

	err := cmd.Start()
	if err == nil {
		fmt.Fprintf(GinkgoWriter, "** Agent running at pid %d\n", cmd.Process.Pid)
		testPids[cmd.Process.Pid] = *cmd.Process
		return cmd.Process, nil
	}
	return nil, err
}

func killAgent(proc *os.Process) {
	fmt.Fprintf(GinkgoWriter, "** Terminating agent pid %d\n", proc.Pid)
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
				if err != nil {
					return "", 0, err
				}
				leaderID := common.ParseNodeID(state.Leader)
				return state.State, uint64(leaderID), nil
			}
		} else {
			if err == nil {
				lastCode = resp.StatusCode
			}
			lastErr = err
		}
		time.Sleep(250 * time.Millisecond)
	}

	return "", 0, fmt.Errorf("Raft peer bad status %s after %d seconds: code = %d err = %s",
		lastStatus, maxWait, lastCode, lastErr)
}

func waitForServerUp(port int, maxWait int) {
	_, _, err := getRaftState(port, maxWait)
	Expect(err).Should(Succeed())
}

func waitForLeader(ports []int, maxWait int) error {
	states := make([]string, len(ports))
	leaders := make([]uint64, len(ports))

	for i := 0; i < maxWait; i++ {
		for p, port := range ports {
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
		for _, state := range states {
			if state == "Leader" {
				leaderCount++
			}
		}

		if leaderCount == 1 {
			leaderID := leaders[0]
			leaderIDCount := 1
			for i := 1; i < len(leaders); i++ {
				if leaders[i] == leaderID {
					leaderIDCount++
				}
			}
			if leaderID != 0 && leaderIDCount == len(leaders) {
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
