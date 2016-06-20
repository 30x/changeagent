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
		"-s", "./tmpdisco",
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
				leaderID, err := strconv.ParseUint(state.Leader, 16, 64)
				if err != nil {
					return "", 0, err
				}
				return state.State, leaderID, nil
			}
		} else {
			if err == nil {
				lastCode = resp.StatusCode
			}
			lastErr = err
		}
		time.Sleep(time.Second)
	}

	return "", 0, fmt.Errorf("Raft peer bad status %s after %d seconds: code = %d err = %s",
		lastStatus, maxWait, lastCode, lastErr)
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

func copyFile(src string, dst string) error {
	dstFile, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	srcFile, err := os.OpenFile(src, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	stat, err := srcFile.Stat()
	if err != nil {
		return err
	}

	buf := make([]byte, stat.Size())
	_, err = srcFile.Read(buf)
	if err != nil {
		return err
	}
	_, err = dstFile.Write(buf)
	if err != nil {
		return err
	}
	stat, _ = dstFile.Stat()
	return nil
}
