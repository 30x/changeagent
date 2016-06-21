package discovery

import (
	"bufio"
	"io"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
)

/*
CreateStaticDiscovery returns an instance of the service that only contains
the list of nodes specified. Each string in the "nodes" array must be a
"hostname:port" string. The node list may still be changed (and we use this
for testing purposes).
*/
func CreateStaticDiscovery(nodes []string) Discovery {
	ret := createImpl(nodes, nil, false)
	return ret
}

/*
CreateStandaloneDiscovery returns an instance of the service that has only one
node. Furthermore, "IsStandalone" will return true, indicating that this is the
only node that will ever exist.
*/
func CreateStandaloneDiscovery(node string) Discovery {
	ret := createImpl([]string{node}, nil, true)
	return ret
}

/*
ReadDiscoveryFile reads a file that contains a list of host:port strings,
each on a separate line. It will turn that list of strings into an instance
of the Discovery interface. It will also poll the file whenever "updateInterval"
expires and update the service if anything has changed.
*/
func ReadDiscoveryFile(fileName string, updateInterval time.Duration) (Discovery, error) {
	info, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	nodes, err := readFile(fileName)
	if err != nil {
		return nil, err
	}

	rdr := fileReader{
		fileName: fileName,
		interval: updateInterval,
		stopChan: make(chan bool, 1),
	}

	ret := createImpl(nodes, &rdr, false)
	rdr.d = ret

	rdr.start(info.ModTime())
	return ret, nil
}

type fileReader struct {
	d        *discoService
	fileName string
	interval time.Duration
	stopChan chan bool
}

func (r *fileReader) start(modTime time.Time) {
	if r.interval == 0 {
		return
	}
	go r.readLoop(modTime)
}

func (r *fileReader) stop() {
	r.stopChan <- true
}

func (r *fileReader) isStandalone() bool {
	return false
}

func (r *fileReader) readLoop(startModTime time.Time) {
	ticker := time.NewTicker(r.interval)
	sentError := false
	lastMod := startModTime

	for {
		select {
		case <-ticker.C:
			info, err := os.Stat(r.fileName)
			if err == nil {
				if info.ModTime() != lastMod {
					newNodes, e := readFile(r.fileName)
					if err == nil {
						r.d.updateNodes(newNodes)
					} else if !sentError {
						glog.Errorf("Error reading discovery file for changes: %v", e)
						sentError = true
					}
					lastMod = info.ModTime()
				}
			} else if !sentError {
				glog.Errorf("Error statting discovery file for changes: %v", err)
				sentError = true
			}
		case <-r.stopChan:
			ticker.Stop()
			return
		}
	}
}

func readFile(fileName string) ([]string, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rdr := bufio.NewReader(f)
	var nodes []string

	for {
		line, err := rdr.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		addr := strings.TrimSpace(line)
		if addr == "" {
			continue
		}

		nodes = append(nodes, addr)
	}
	return nodes, nil
}
