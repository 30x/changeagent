package main

import (
	"errors"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"revision.aeip.apigee.net/greg/changeagent/communication"
	"revision.aeip.apigee.net/greg/changeagent/discovery"
	"revision.aeip.apigee.net/greg/changeagent/raft"
	"revision.aeip.apigee.net/greg/changeagent/storage"
)

type ChangeAgent struct {
	stor   storage.Storage
	raft   *raft.Service
	router *mux.Router
}

const (
	NormalChange  = 0
	CommandChange = 1

	JSONContent = "application/json"
	FormContent = "application/x-www-form-urlencoded"

	CreateTenantCommand     = "CreateTenant"
	CreateCollectionCommand = "CreateCollection"

	DBCacheSize = 10 * 1024 * 1024
)

func StartChangeAgent(disco discovery.Discovery,
	dbFile string,
	httpMux *http.ServeMux) (*ChangeAgent, error) {
	comm, err := communication.StartHTTPCommunication(httpMux)
	if err != nil {
		return nil, err
	}
	stor, err := storage.CreateRocksDBStorage(dbFile, DBCacheSize)
	if err != nil {
		return nil, err
	}

	agent := &ChangeAgent{
		stor:   stor,
		router: mux.NewRouter(),
	}

	raft, err := raft.StartRaft(comm, disco, stor, agent)
	if err != nil {
		return nil, err
	}
	agent.raft = raft
	comm.SetRaft(raft)

	agent.initDiagnosticAPI()
	agent.initChangesAPI()

	httpMux.Handle("/", agent.router)

	return agent, nil
}

func (a *ChangeAgent) Close() {
	a.raft.Close()
	a.stor.Close()
}

func (a *ChangeAgent) Delete() {
	a.stor.Delete()
}

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

	// Wait for the new commit to be applied, or time out
	appliedIndex :=
		a.raft.GetAppliedTracker().TimedWait(newIndex, time.Second*CommitTimeoutSeconds)
	glog.V(2).Infof("New index %d is now applied", appliedIndex)
	if appliedIndex >= newIndex {
		newEntry := storage.Entry{
			Index: newIndex,
		}
		return newEntry, nil
	}

	return storage.Entry{}, errors.New("Commit timeout")
}

func (a *ChangeAgent) Commit(entry *storage.Entry) error {
	// Nothing to do now. Perhaps we take this interface out.
	return nil
}

func writeError(resp http.ResponseWriter, code int, err error) {
	glog.Errorf("Returning error %d: %s", code, err)
	msg := marshalError(err)
	resp.Header().Set("Content-Type", JSONContent)
	resp.WriteHeader(code)
	resp.Write([]byte(msg))
}
