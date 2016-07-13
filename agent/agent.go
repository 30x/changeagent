package main

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/communication"
	"github.com/30x/changeagent/raft"
	"github.com/30x/changeagent/storage"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
)

/*
ChangeAgent is a server that implements the Raft protocol, plus the "changeagent"
API.
*/
type ChangeAgent struct {
	stor      storage.Storage
	raft      *raft.Service
	router    *mux.Router
	uriPrefix string
}

const (
	// NormalChange denotes a Raft proposal that will appear to everyone in the change log.
	// We may introduce additional change types in the future.
	NormalChange = 0

	commitTimeoutSeconds = 10
	dbCacheSize          = 10 * 1024 * 1024

	plainTextContent = "text/plain"
	jsonContent      = "application/json"
)

/*
StartChangeAgent starts an instance of changeagent with its API listening on a specific
HTTP "mux".

"dbFile" denotes the name of the base directory for the local RocksDB database.

"httpMux" must have been previously created using the "net/http" package,
and it must listen for HTTP requests.

If "uriPrefix" is not the empty string, then every API call will require that
it be prepended. In other words, "/changes" will become "/prefix/changes".
The prefix must not end with a "/".
*/
func StartChangeAgent(
	dbFile string,
	httpMux *http.ServeMux,
	uriPrefix string,
	comm communication.Communication) (*ChangeAgent, error) {

	if uriPrefix != "" {
		if uriPrefix[len(uriPrefix)-1] == '/' {
			return nil, errors.New("Invalid URI prefix: Must not end with a slash")
		}
		if uriPrefix[0] != '/' {
			uriPrefix = "/" + uriPrefix
		}
	}

	stor, err := storage.CreateRocksDBStorage(dbFile, dbCacheSize)
	if err != nil {
		return nil, err
	}

	agent := &ChangeAgent{
		stor:      stor,
		router:    mux.NewRouter(),
		uriPrefix: uriPrefix,
	}

	raft, err := raft.StartRaft(comm, stor, agent)
	if err != nil {
		return nil, err
	}
	agent.raft = raft
	comm.SetRaft(raft)

	agent.initDiagnosticAPI(uriPrefix)
	agent.initChangesAPI(uriPrefix)
	agent.initHooksAPI(uriPrefix)
	agent.initClusterAPI(uriPrefix)
	agent.initConfigAPI(uriPrefix)

	httpMux.Handle("/", agent.router)

	return agent, nil
}

/*
Close stops changeagent.
*/
func (a *ChangeAgent) Close() {
	a.raft.Close()
	a.stor.Close()
}

/*
Delete deletes the database, cleaning out the contents of the DB
directory. "Close" must be called first.
*/
func (a *ChangeAgent) Delete() {
	a.stor.Delete()
}

/*
GetRaftState returns the state of the internal Raft implementation.
*/
func (a *ChangeAgent) GetRaftState() raft.State {
	return a.raft.GetState()
}

func (a *ChangeAgent) makeProposal(proposal *common.Entry) (*common.Entry, error) {
	// Timestamp and otherwise update the proposal
	proposal.Timestamp = time.Now()

	// Send the raft proposal. This happens asynchronously.
	newIndex, err := a.raft.Propose(proposal)
	if err != nil {
		glog.Warningf("Fatal error making Raft proposal: %v", err)
		return nil, err
	}
	glog.V(2).Infof("Proposed new change with index %d", newIndex)

	err = a.waitForCommit(newIndex)
	if err == nil {
		newEntry := &common.Entry{
			Index: newIndex,
		}
		return newEntry, nil
	}

	return nil, err
}

// Wait for the new commit to be applied, or time out
func (a *ChangeAgent) waitForCommit(ix uint64) error {
	appliedIndex :=
		a.raft.GetAppliedTracker().TimedWait(ix, time.Second*commitTimeoutSeconds)
	glog.V(2).Infof("New index %d is now applied", appliedIndex)
	if appliedIndex < ix {
		return errors.New("Commit timeout")
	}
	return nil
}

/*
Commit is called by the Raft implementation when an entry has reached
commit state. However, we do not do anything here today.
*/
func (a *ChangeAgent) Commit(entry *common.Entry) error {
	// Nothing to do now. Perhaps we take this interface out.
	return nil
}

func writeError(resp http.ResponseWriter, code int, err error) {
	glog.Errorf("Returning error %d: %s", code, err)
	msg := marshalError(err)
	resp.Header().Set("Content-Type", jsonContent)
	resp.WriteHeader(code)
	resp.Write([]byte(msg))
}

var jsonContentRe = regexp.MustCompile("^application/json(;.*)?$")

func isJSON(resp http.ResponseWriter, req *http.Request) bool {
	if !jsonContentRe.MatchString(req.Header.Get("Content-Type")) {
		writeError(resp, http.StatusUnsupportedMediaType, errors.New("Unsupported content type"))
		return false
	}
	return true
}

func startListener(port int, key, cert, cas string) (net.Listener, int, error) {
	addr := &net.TCPAddr{
		Port: port,
	}

	tcpListener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, 0, err
	}
	var listener net.Listener = tcpListener
	success := false

	defer func() {
		if !success {
			listener.Close()
		}
	}()

	if key != "" || cert != "" {
		if key == "" || cert == "" {
			return nil, 0, errors.New("Both -key and -cert must be set")
		}

		tlsCert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, 0, err
		}

		tlsCfg := tls.Config{
			Certificates: []tls.Certificate{tlsCert},
		}

		if cas != "" {
			caPool, err := communication.LoadCertPool(cas)
			if err != nil {
				return nil, 0, err
			}

			// If we have "cas" then also verify clients
			tlsCfg.ClientCAs = caPool
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}

		listener = tls.NewListener(tcpListener, &tlsCfg)
	}

	success = true
	_, portStr, _ := net.SplitHostPort(listener.Addr().String())
	listenPort, _ := strconv.Atoi(portStr)
	return listener, listenPort, err
}
