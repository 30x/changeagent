package main

import (
  "errors"
  "fmt"
  "path"
  "regexp"
  "strconv"
  "encoding/json"
  "net/http"
  "github.com/gorilla/mux"
  "github.com/golang/protobuf/proto"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  "github.com/satori/go.uuid"
)

const (
  Tenants = "/tenants"
  SingleTenant = Tenants + "/{tenant}"
  TenantCollections = SingleTenant + "/collections"
  SingleTenantCollection = TenantCollections + "/{collection}"
  Collections = "/collections"
  SingleCollection = Collections + "/{collection}"
  CollectionKeys = SingleCollection + "/keys"
  SingleKey = CollectionKeys + "/{key}"
)

var uuidRE = regexp.MustCompile("[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9A-Fa-f]+")

func (a *ChangeAgent) initIndexAPI() {
  a.router.HandleFunc(Tenants, a.handleGetTenants).Methods("GET")
  a.router.HandleFunc(Tenants, a.handleCreateTenant).Methods("POST")
  a.router.HandleFunc(SingleTenant, a.handleGetTenant).Methods("GET")
  a.router.HandleFunc(TenantCollections, a.handleGetCollections).Methods("GET")
  a.router.HandleFunc(SingleTenantCollection, a.handleGetTenantCollection).Methods("GET")
  a.router.HandleFunc(TenantCollections, a.handleCreateCollection).Methods("POST")
  a.router.HandleFunc(SingleCollection, a.handleGetCollection).Methods("GET")
  a.router.HandleFunc(CollectionKeys, a.handleGetCollectionKeys).Methods("GET")
  a.router.HandleFunc(CollectionKeys, a.handleCreateCollectionKey).Methods("POST")
  a.router.HandleFunc(SingleKey, a.handleGetCollectionKey).Methods("GET")
}

func (a *ChangeAgent) handleGetTenants(resp http.ResponseWriter, req *http.Request) {
  tenants, err := a.stor.GetTenants()
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  links := make([]TenantLink, 0)
  for _, t := range(tenants) {
    l := TenantLink{
      Name: t.Name,
      Id: t.Id.String(),
      Self: linkURI(req, fmt.Sprintf("%s/%s", Tenants, t.Id)),
      Collections: linkURI(req, fmt.Sprintf("%s/%s/collections", Tenants, t.Id)),
    }
    links = append(links, l)
  }

  body, err := json.Marshal(links)
  if err == nil {
    resp.Header().Set("Content-Type", JSONContent)
    resp.Write(body)
  } else {
    writeError(resp, http.StatusInternalServerError, err)
  }
}

func (a *ChangeAgent) handleCreateTenant(resp http.ResponseWriter, req *http.Request) {
  var tenantName string
  var err error

  if isFormContent(req) {
    tenantName = req.FormValue("name")
  } else if isJSONContent(req) {
    var tenant TenantLink
    err = unmarshalAny(req.Body, &tenant)
    tenantName = tenant.Name
  } else {
    writeError(resp, http.StatusUnsupportedMediaType, errors.New("Unsupported media type"))
    return
  }

  if tenantName == "" {
    writeError(resp, http.StatusBadRequest, errors.New("Missing \"name\" parameter"))
    return
  }

  cmdStr := CreateTenantCommand
  cmd := AgentCommand{
    Command: &cmdStr,
    Name: &tenantName,
  }

  pb, err := proto.Marshal(&cmd)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  entry := storage.Entry{
    Type: CommandChange,
    Data: pb,
  }

  _, err = a.makeProposal(&entry)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  id, err := a.stor.GetTenantByName(tenantName)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  l := TenantLink{
    Name: tenantName,
    Id: id.String(),
    Self: linkURI(req, fmt.Sprintf("%s/%s", Tenants, id)),
    Collections: linkURI(req, fmt.Sprintf("%s/%s/collections", Tenants, id)),
  }

  writeTenantLink(resp, &l)
}


func (a *ChangeAgent) handleGetTenant(resp http.ResponseWriter, req *http.Request) {
  tenantIDStr := mux.Vars(req)["tenant"]

  tenantID, tenantName, errCode, err := a.validateTenant(tenantIDStr)
  if err != nil {
    writeError(resp, errCode, err)
    return
  }

  l := TenantLink{
    Name: tenantName,
    Id: tenantID.String(),
    Self: linkURI(req, fmt.Sprintf("%s/%s", Tenants, tenantID)),
    Collections: linkURI(req, fmt.Sprintf("%s/%s/collections", Tenants, tenantID)),
  }
  writeTenantLink(resp, &l)
}

func writeTenantLink(resp http.ResponseWriter, l *TenantLink) {
  body, err := json.Marshal(&l)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  resp.Header().Set("Content-Type", JSONContent)
  resp.Write(body)
}

func (a *ChangeAgent) handleGetCollections(resp http.ResponseWriter, req *http.Request) {
  tenantIDStr := mux.Vars(req)["tenant"]

  tenantID, _, errCode, err := a.validateTenant(tenantIDStr)
  if err != nil {
    writeError(resp, errCode, err)
    return
  }

  collections, err := a.stor.GetTenantCollections(tenantID)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  links := make ([]CollectionLink, 0)

  for _, c := range(collections) {
    l := CollectionLink{
      Id: c.Id.String(),
      Name: c.Name,
      Self: linkURI(req, fmt.Sprintf("/collections/%s", c.Id)),
      Keys: linkURI(req, fmt.Sprintf("/collections/%s/keys", c.Id)),
    }
    links = append(links, l)
  }

  body, err := json.Marshal(links)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  resp.Header().Set("Content-Type", JSONContent)
  resp.Write(body)
}

func (a *ChangeAgent) handleCreateCollection(resp http.ResponseWriter, req *http.Request) {
  tenantIDStr := mux.Vars(req)["tenant"]

  tenantID, _, errCode, err := a.validateTenant(tenantIDStr)
  if err != nil {
    writeError(resp, errCode, err)
    return
  }

  var collectionName string

  if isFormContent(req) {
    collectionName = req.FormValue("name")
  } else if isJSONContent(req) {
    var coll CollectionLink
    err = unmarshalAny(req.Body, &coll)
    collectionName = coll.Name
  } else {
    writeError(resp, http.StatusUnsupportedMediaType, errors.New("Unsupported media type"))
    return
  }

  if collectionName == "" {
    writeError(resp, http.StatusBadRequest, errors.New("Missing \"name\" parameter"))
    return
  }

  cmdStr := CreateCollectionCommand
  cmd := AgentCommand{
    Command: &cmdStr,
    Name: &collectionName,
    Tenant: tenantID.Bytes(),
  }

  pb, err := proto.Marshal(&cmd)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  entry := storage.Entry{
    Type: CommandChange,
    Data: pb,
  }

  _, err = a.makeProposal(&entry)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  id, err := a.stor.GetCollectionByName(tenantID, collectionName)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }
  if uuid.Equal(id, uuid.Nil) {
    writeError(resp, http.StatusInternalServerError, errors.New("Collection not created"))
    return
  }

  l := CollectionLink{
    Name: collectionName,
    Id: id.String(),
    Self: linkURI(req, fmt.Sprintf("%s/%s", Collections, id)),
    Keys: linkURI(req, fmt.Sprintf("%s/%s/keys", Collections, id)),
  }

  writeCollectionLink(resp, &l)
}

func (a *ChangeAgent) handleGetCollection(resp http.ResponseWriter, req *http.Request) {
  collectionIDStr := mux.Vars(req)["collection"]
  collectionID, collectionName, errCode, err := a.validateCollectionID(collectionIDStr)
  if err != nil {
    writeError(resp, errCode, err)
    return
  }

  l := CollectionLink{
    Name: collectionName,
    Id: collectionID.String(),
    Self: linkURI(req, fmt.Sprintf("%s/%s", Collections, collectionID)),
    Keys: linkURI(req, fmt.Sprintf("%s/%s/keys", Collections, collectionID)),
  }
  writeCollectionLink(resp, &l)
}

func (a *ChangeAgent) handleGetTenantCollection(resp http.ResponseWriter, req *http.Request) {
  tenantIDStr := mux.Vars(req)["tenant"]

  tenantID, _, errCode, err := a.validateTenant(tenantIDStr)
  if err != nil {
    writeError(resp, errCode, err)
    return
  }

  collectionIDStr := mux.Vars(req)["collection"]
  collectionID, collectionName, errCode, err := a.validateCollection(tenantID, collectionIDStr)
  if err != nil {
    writeError(resp, errCode, err)
    return
  }

  l := CollectionLink{
    Name: collectionName,
    Id: collectionID.String(),
    Self: linkURI(req, fmt.Sprintf("%s/%s", Collections, collectionID)),
    Keys: linkURI(req, fmt.Sprintf("%s/%s/keys", Collections, collectionID)),
  }
  writeCollectionLink(resp, &l)
}

func writeCollectionLink(resp http.ResponseWriter, l *CollectionLink) {
  body, err := json.Marshal(&l)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  resp.Header().Set("Content-Type", JSONContent)
  resp.Write(body)
}

func (a *ChangeAgent) handleGetCollectionKeys(resp http.ResponseWriter, req *http.Request) {
  collectionIDStr := mux.Vars(req)["collection"]
  collectionID, _, errCode, err := a.validateCollectionID(collectionIDStr)
  if err != nil {
    writeError(resp, errCode, err)
    return
  }

  qps := req.URL.Query()
  limitStr := qps.Get("limit")
  if limitStr == "" {
    limitStr = DefaultLimit
  }
  limit, err := strconv.ParseUint(limitStr, 10, 32)
  if err != nil {
    writeError(resp, http.StatusBadRequest, err)
    return
  }

  lastKey := qps.Get("last")

  indices, err := a.stor.GetCollectionIndices(collectionID, lastKey, uint(limit))
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  entries := make([]storage.Entry, 0)

  for _, ix := range(indices) {
    entry, err := a.stor.GetEntry(ix)
    if err != nil {
      writeError(resp, http.StatusInternalServerError, err)
      return
    }
    entries = append(entries, *entry)
  }

  body, err := marshalChanges(entries)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  resp.Header().Set("Content-Type", JSONContent)
  resp.Write(body)
}

func (a *ChangeAgent) handleCreateCollectionKey(resp http.ResponseWriter, req *http.Request) {
  collectionIDStr := mux.Vars(req)["collection"]
  collectionID, _, errCode, err := a.validateCollectionID(collectionIDStr)
  if err != nil {
    writeError(resp, errCode, err)
    return
  }

  if req.Header.Get("Content-Type")!= JSONContent {
    // TODO regexp?
    writeError(resp, http.StatusUnsupportedMediaType, errors.New("Unsupported content type"))
    return
  }

  defer req.Body.Close()
  proposal, err := unmarshalJson(req.Body)
  if err != nil {
    writeError(resp, http.StatusBadRequest, errors.New("Invalid JSON"))
    return
  }

  if proposal.Key == "" {
    writeError(resp, http.StatusBadRequest, errors.New("Missing \"key\" parameter"))
    return
  }
  if !uuid.Equal(proposal.Collection, collectionID) {
    writeError(resp, http.StatusBadRequest, errors.New("Invalid collection ID"))
    return
  }
  proposal.Collection = collectionID

  newEntry, err := a.makeProposal(proposal)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  body, err := marshalJson(newEntry)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  resp.Header().Set("Content-Type", JSONContent)
  resp.Write(body)
}

func (a *ChangeAgent) handleGetCollectionKey(resp http.ResponseWriter, req *http.Request) {
  collectionIDStr := mux.Vars(req)["collection"]
  keyStr := mux.Vars(req)["key"]
  collectionID, _, errCode, err := a.validateCollectionID(collectionIDStr)
  if err != nil {
    writeError(resp, errCode, err)
    return
  }

  ix, err := a.stor.GetIndexEntry(collectionID, keyStr)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  entry, err := a.stor.GetEntry(ix)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  body, err := marshalJson(entry)
  if err != nil {
    writeError(resp, http.StatusInternalServerError, err)
    return
  }

  resp.Header().Set("Content-Type", JSONContent)
  resp.Write(body)
}

func (a *ChangeAgent) validateTenant(ten string) (uuid.UUID, string, int, error) {
  if uuidRE.MatchString(ten) {
    // It's a tenant UUID
    tenantID, err := uuid.FromString(ten)
    if err != nil {
      return uuid.Nil, "", http.StatusBadRequest, err
    }
    tenantName, err := a.stor.GetTenantByID(tenantID)
    if err != nil {
      return uuid.Nil, "", http.StatusInternalServerError, err
    }
    if tenantName == "" {
      return uuid.Nil, "", http.StatusNotFound, errors.New("Tenant not found")
    }
    return tenantID, tenantName, http.StatusOK, nil

  } else {
    // it's a tenant  name
    tenantID, err := a.stor.GetTenantByName(ten)
    if err != nil {
      return uuid.Nil, "", http.StatusInternalServerError, err
    }
    if uuid.Equal(tenantID, uuid.Nil) {
      return uuid.Nil, "", http.StatusNotFound, errors.New("Tenant not found")
    }
    return tenantID, ten, http.StatusOK, nil
  }
}

func (a *ChangeAgent) validateCollection(tenantID uuid.UUID, coll string) (uuid.UUID, string, int, error) {
  if uuidRE.MatchString(coll) {
    return a.validateCollectionID(coll)

  } else {
    // it's a tenant  name
    collectionID, err := a.stor.GetCollectionByName(tenantID, coll)
    if err != nil {
      return uuid.Nil, "", http.StatusInternalServerError, err
    }
    if uuid.Equal(collectionID, uuid.Nil) {
      return uuid.Nil, "", http.StatusNotFound, errors.New("Tenant not found")
    }
    return collectionID, coll, http.StatusOK, nil
  }
}

func (a *ChangeAgent) validateCollectionID(coll string) (uuid.UUID, string, int, error) {
  collectionID, err := uuid.FromString(coll)
  if err != nil {
    return uuid.Nil, "", http.StatusBadRequest, err
  }
  collectionName, err := a.stor.GetCollectionByID(collectionID)
  if err != nil {
    return uuid.Nil, "", http.StatusInternalServerError, err
  }
  if collectionName == "" {
    return uuid.Nil, "", http.StatusNotFound, errors.New("Collection not found")
  }
  return collectionID, collectionName, http.StatusOK, nil
}

func linkURI(req *http.Request, relPath string) string {
  var proto string

  if req.TLS == nil {
    proto = "http"
  } else {
    proto = "https"
  }

  newPath := path.Join(req.URL.RawPath, relPath)

  // TODO Deal with X-Forwarded-Host headers
  return fmt.Sprintf("%s://%s%s", proto, req.Host, newPath)
}

func isFormContent(req *http.Request) bool {
  return req.Header.Get("Content-Type") == FormContent
}

func isJSONContent(req *http.Request) bool {
  return req.Header.Get("Content-Type") == JSONContent
}