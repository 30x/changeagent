package main

import (
  "fmt"
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

func StartChangeAgent(nodeId uint64,
                      disco discovery.Discovery,
                      dbFile string,
                      mux *http.ServeMux) (*ChangeAgent, error) {
  comm, err := communication.StartHttpCommunication(mux, disco)
  if err != nil { return nil, err }
  stor, err := storage.CreateLevelDBStorage(dbFile)
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

  if entry.Key != "" || entry.Tenant != "" || entry.Collection != "" {
    glog.V(2).Infof("Indexing %d in tenant %d collection %d key %d",
      id, entry.Tenant, entry.Collection, entry.Key)
    err = a.stor.SetIndexEntry(entry.Tenant, entry.Collection, entry.Key, id)
    if err != nil {
      glog.Errorf("Error indexing a committed entry: %s", err)
      return err
    }
  }
  return nil
}