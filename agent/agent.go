package main

import (
  "errors"
  "fmt"
  "time"
  "net/http"
  "github.com/golang/protobuf/proto"
  "github.com/golang/glog"
  "github.com/gorilla/mux"
  "github.com/satori/go.uuid"
  "revision.aeip.apigee.net/greg/changeagent/communication"
  "revision.aeip.apigee.net/greg/changeagent/discovery"
  "revision.aeip.apigee.net/greg/changeagent/raft"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

type ChangeAgent struct {
  stor storage.Storage
  raft *raft.RaftImpl
  router *mux.Router
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
                      httpMux *http.ServeMux) (*ChangeAgent, error) {
  comm, err := communication.StartHttpCommunication(httpMux, disco)
  if err != nil { return nil, err }
  stor, err := storage.CreateRocksDBStorage(dbFile, DBCacheSize)
  if err != nil { return nil, err }

  agent := &ChangeAgent{
    stor: stor,
    router: mux.NewRouter(),
  }

  raft, err := raft.StartRaft(nodeId, comm, disco, stor, agent)
  if err != nil { return nil, err }
  agent.raft = raft
  comm.SetRaft(raft)

  agent.initDiagnosticApi()
  agent.initChangesAPI()
  agent.initIndexAPI()

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
  glog.V(2).Infof("Got a commit for entry %d", id)

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
  glog.V(2).Info("Handling a normal change")
  if entry.Key != "" || entry.Collection != nil {
    glog.V(2).Infof("Inserting change into collection %s", entry.Collection)
    err := a.stor.SetIndexEntry(entry.Collection, entry.Key, entry.Index)
    if err != nil {
      glog.Errorf("Error indexing a committed entry: %s", err)
      return err
    }
  }
  return nil
}

func (a *ChangeAgent) handleChangeCommand(entry *storage.Entry) error {
  var cmd AgentCommand
  err := proto.Unmarshal(entry.Data, &cmd)
  if err != nil { return err }

  glog.V(2).Infof("Received command \"%s\"", cmd.GetCommand())

  switch cmd.GetCommand() {
  case CreateTenantCommand:
    if cmd.GetName() == "" {
      return errors.New("Invalid command: tenant name is missing")
    }
    glog.V(2).Infof("Creating tenant %s", cmd.GetName())
    _, err := a.stor.CreateTenant(cmd.GetName())
    return err

  case CreateCollectionCommand:
    if cmd.GetTenant() == nil {
      return errors.New("Invalid command: tenant ID is missing")
    }
    if cmd.GetName() == "" {
      return errors.New("Invalid command: collection name is missing")
    }

    tenantID, err := uuid.FromBytes(cmd.GetTenant())
    if err != nil { return err }
    glog.V(2).Infof("Creating collection %s for tenant %s", cmd.GetName(), tenantID)
    _, err = a.stor.CreateCollection(&tenantID, cmd.GetName())
    return err

  default:
    return fmt.Errorf("Invalid command: %s", cmd.GetCommand())
  }
}