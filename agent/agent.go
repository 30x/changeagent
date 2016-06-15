package main

import (
	"errors"
	"net/http"
	"regexp"
	"time"

	"github.com/30x/changeagent/communication"
	"github.com/30x/changeagent/discovery"
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
func StartChangeAgent(disco discovery.Discovery,
	dbFile string,
	httpMux *http.ServeMux,
	uriPrefix string) (*ChangeAgent, error) {

	if uriPrefix != "" {
		if uriPrefix[len(uriPrefix)-1] == '/' {
			return nil, errors.New("Invalid URI prefix: Must not end with a slash")
		}
		if uriPrefix[0] != '/' {
			uriPrefix = "/" + uriPrefix
		}
	}

	comm, err := communication.StartHTTPCommunication(httpMux)
	if err != nil {
		return nil, err
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

	raft, err := raft.StartRaft(comm, disco, stor, agent)
	if err != nil {
		return nil, err
	}
	agent.raft = raft
	comm.SetRaft(raft)

	agent.initDiagnosticAPI(uriPrefix)
	agent.initChangesAPI(uriPrefix)
	agent.initHooksAPI(uriPrefix)

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

func (a *ChangeAgent) makeProposal(proposal storage.Entry) (storage.Entry, error) {
	// Timestamp and otherwise update the proposal
	proposal.Timestamp = time.Now()

	// Send the raft proposal. This happens asynchronously.
	newIndex, err := a.raft.Propose(proposal)
	if err != nil {
		glog.Warningf("Fatal error making Raft proposal: %v", err)
		return storage.Entry{}, err
	}
	glog.V(2).Infof("Proposed new change with index %d", newIndex)

	err = a.waitForCommit(newIndex)
	if err == nil {
		newEntry := storage.Entry{
			Index: newIndex,
		}
		return newEntry, nil
	}

	return storage.Entry{}, err
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
func (a *ChangeAgent) Commit(entry *storage.Entry) error {
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
