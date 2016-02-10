package main

import (
  "errors"
  "fmt"
  "time"
  "net/http"
  "github.com/golang/glog"
  "github.com/gin-gonic/gin"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/raft"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

type ChangeAgent struct {
  stor storage.Storage
  raft *raft.RaftImpl
  api *gin.Engine
}

const (
  NormalChange = 0
  CommandChange = 1

  JSONContent = "application/json"
  FormContent = "application/x-www-form-urlencoded"

  CreateTenantCommand = "CreateTenant"
  CreateCollectionCommand = "CreateCollection"

  DBCacheSize = 10 * 1024 * 1024
)

func StartChangeAgent(nodeId uint64,
                      disco discovery.Discovery,
                      dbFile string,
                      mux *http.ServeMux) (*ChangeAgent, error) {
  comm, err := communication.StartHttpCommunication(mux, disco)
  if err != nil { return nil, err }
  stor, err := storage.CreateRocksDBStorage(dbFile, DBCacheSize)
  if err != nil { return nil, err }

  gin.SetMode(gin.ReleaseMode)

  agent := &ChangeAgent{
    stor: stor,
    api: gin.New(),
  }

  raft, err := raft.StartRaft(nodeId, comm, disco, stor, agent)
  if err != nil { return nil, err }
  agent.raft = raft
  comm.SetRaft(raft)

  agent.initDiagnosticApi()
  agent.initChangesAPI()
  agent.initIndexAPI()

  mux.HandleFunc("/", agent.api.ServeHTTP)

  return agent, nil
}

func (a *ChangeAgent) Close() {
  a.raft.Close()
  a.stor.Close()
}

func (a *ChangeAgent) Delete() {
  a.stor.Delete()
}

func (a *ChangeAgent) GetRaftState() int {
  return a.raft.GetState()
}

func (a *ChangeAgent) makeProposal(proposal *storage.Entry) (*storage.Entry, error) {
  // Timestamp and otherwise update the proposal
  proposal.Timestamp = time.Now()

  // Send the raft proposal. This happens asynchronously.
  newIndex, err := a.raft.Propose(proposal)
  if err != nil {
    glog.Warningf("Fatal error making Raft proposal: %v", err)
    return nil, err
  }
  glog.V(2).Infof("Proposed new change with index %d", newIndex)

  // Wait for the new commit to be applied, or time out
  appliedIndex :=
    a.raft.GetAppliedTracker().TimedWait(newIndex, time.Second * CommitTimeoutSeconds)
  glog.V(2).Infof("New index %d is now applied", appliedIndex)
  if appliedIndex >= newIndex {
    newEntry := storage.Entry{
      Index: newIndex,
    }
    return &newEntry, nil
  } else {
    return nil, errors.New("Commit timeout")
  }
}

func (a *ChangeAgent) Commit(id uint64) error {
  glog.V(2).Infof("Got a commit for entry %d")

  entry, err := a.stor.GetEntry(id)
  if err != nil {
    glog.Errorf("Error reading a committed entry: %s", err)
    return err
  }
  if entry == nil {
    glog.Errorf("Committed entry %d could not be read", id)
    return fmt.Errorf("Missing committed entry %d", id)
  }

  switch entry.Type {
  case NormalChange:
    return a.handleNormalChange(entry)
  case CommandChange:
    return a.handleChangeCommand(entry)
  default:
    return fmt.Errorf("Invalid change type %d", entry.Type)
  }
}

func (a *ChangeAgent) handleNormalChange(entry *storage.Entry) error {
  if entry.Key != "" || entry.Tenant != "" || entry.Collection != "" {
    glog.V(2).Infof("Indexing %d in tenant %d collection %d key %d",
      entry.Index, entry.Tenant, entry.Collection, entry.Key)
    err := a.stor.SetIndexEntry(entry.Tenant, entry.Collection, entry.Key, entry.Index)
    if err != nil {
      glog.Errorf("Error indexing a committed entry: %s", err)
      return err
    }
  }
  return nil
}

func (a *ChangeAgent) handleChangeCommand(entry *storage.Entry) error {
  cmd := string(entry.Data)

  switch cmd {
  case CreateTenantCommand:
    if entry.Tenant == "" {
      return errors.New("Invalid command: tenant name is missing")
    }
    return a.stor.CreateTenant(entry.Tenant)

  case CreateCollectionCommand:
    if entry.Tenant == "" {
      return errors.New("Invalid command: tenant name is missing")
    }
    if entry.Collection == "" {
      return errors.New("Invalid command: collection name is missing")
    }
    return a.stor.CreateCollection(entry.Tenant, entry.Collection)

  default:
    return fmt.Errorf("Invalid command: %s", cmd)
  }
}