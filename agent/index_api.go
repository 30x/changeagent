package main

import (
  "errors"
  "fmt"
  "path"
  "strconv"
  "net/http"
  "github.com/gin-gonic/gin"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

const (
  Tenants = "/tenants"
  SingleTenant = Tenants + "/:tenant"
  TenantCollections = SingleTenant + "/collections"
  SingleCollection = TenantCollections + "/:collection"
  SingleItem = SingleCollection + "/:key"
)

func (a *ChangeAgent) initIndexAPI() {
  a.api.POST(Tenants, a.handlePostTenants)
  a.api.GET(SingleTenant, a.handleGetTenant)
  a.api.DELETE(SingleTenant, a.handleDeleteTenant)
  a.api.GET(TenantCollections, a.handleGetCollections)
  a.api.POST(TenantCollections, a.handlePostCollections)
  a.api.GET(SingleCollection, a.handleGetCollection)
  a.api.POST(SingleCollection, a.handlePostItem)
  a.api.GET(SingleItem, a.handleGetItem)
  a.api.PUT(SingleItem, a.handlePutItem)
  a.api.DELETE(SingleItem, a.handleDeleteItem)
}

func (a *ChangeAgent) handlePostTenants(c *gin.Context) {
  if c.ContentType() != FormContent {
    c.AbortWithStatus(http.StatusUnsupportedMediaType)
    return
  }

  tenantName := c.PostForm("tenant")
  if tenantName == "" {
    c.AbortWithStatus(http.StatusBadRequest)
    return
  }

  entry := storage.Entry{
    Type: CommandChange,
    Tenant: tenantName,
    Data: []byte(CreateTenantCommand),
  }

  _, err := a.makeProposal(&entry)
  if err != nil {
    writeError(c, http.StatusInternalServerError, err)
  } else {
    c.JSON(200, createTenantResponse(c, tenantName))
  }
}

func (a *ChangeAgent) handleGetTenant(c *gin.Context) {
  tenant := c.Param("tenant")
  exists, err := a.stor.TenantExists(tenant)
  if err != nil {
    writeError(c, http.StatusBadRequest, err)
    return
  }

  if exists {
    c.JSON(200, createTenantResponse(c, tenant))
  } else {
    c.AbortWithStatus(404)
  }
}

func (a *ChangeAgent) handleDeleteTenant(c *gin.Context) {
  tenant := c.Param("tenant")
  exists, err := a.stor.TenantExists(tenant)
  if err != nil {
    writeError(c, http.StatusBadRequest, err)
    return
  }

  if exists {
    c.AbortWithError(400, errors.New("Not implemented"))
  } else {
    c.AbortWithStatus(404)
  }
}

func (a *ChangeAgent) handleGetCollections(c *gin.Context) {
  tenant := c.Param("tenant")
  exists, err := a.stor.TenantExists(tenant)
  if err != nil {
    writeError(c, http.StatusBadRequest, err)
    return
  }

  if exists {
    collections, err := a.stor.GetTenantCollections(tenant)
    if err != nil {
      writeError(c, http.StatusInternalServerError, err)
      return
    }

    resp := make(map[string]string)
    for _, collection := range(collections) {
      resp[collection] = linkURI(c, fmt.Sprintf("/tenants/%s/collections/%s", tenant, collection))
    }
    c.JSON(200, resp)

  } else {
    c.AbortWithStatus(404)
  }
}

func (a *ChangeAgent) handlePostCollections(c *gin.Context) {
  if c.ContentType() != FormContent {
    c.AbortWithStatus(http.StatusUnsupportedMediaType)
    return
  }

  tenantName := c.Param("tenant")
  collectionName := c.PostForm("collection")
  if collectionName == "" {
    c.AbortWithStatus(http.StatusBadRequest)
    return
  }

  entry := storage.Entry{
    Type: CommandChange,
    Tenant: tenantName,
    Collection: collectionName,
    Data: []byte(CreateCollectionCommand),
  }

  _, err := a.makeProposal(&entry)
  if err != nil {
    writeError(c, http.StatusInternalServerError, err)
  } else {
    resp := make(map[string]string)
    resp["name"] = collectionName
    resp["tenant"] = tenantName
    resp["self"] = linkURI(c, fmt.Sprintf("/tenants/%s/collections/%s", tenantName, collectionName))
    c.JSON(200, resp)
  }
}

func (a *ChangeAgent) handleGetCollection(c *gin.Context) {
  tenantName := c.Param("tenant")
  collectionName := c.Param("collection")

  limitStr := c.DefaultQuery("limit", DefaultLimit)
  limit, err := strconv.ParseUint(limitStr, 10, 32)
  if err != nil {
    writeError(c, http.StatusBadRequest, err)
    return
  }

  indices, err := a.stor.GetCollectionIndices(tenantName, collectionName, uint(limit))
  if err != nil {
    writeError(c, http.StatusInternalServerError, err)
    return
  }

  var entries []storage.Entry

  for _, ix := range(indices) {
    entry, err := a.stor.GetEntry(ix)
    if err != nil {
      writeError(c, http.StatusInternalServerError, err)
      return
    }
    entries = append(entries, *entry)
  }

  str, err := marshalChanges(entries)
  if err == nil {
    c.Header("Content-Type", JSONContent)
    c.String(200, str)
  } else {
    writeError(c, http.StatusInternalServerError, err)
  }
}

func (a *ChangeAgent) handlePostItem(c *gin.Context) {
  tenantName := c.Param("tenant")
  collectionName := c.Param("collection")

  if c.ContentType() != JSONContent {
    // TODO regexp?
    c.AbortWithStatus(http.StatusUnsupportedMediaType)
    return
  }

  defer c.Request.Body.Close()
  entry, err := unmarshalJson(c.Request.Body)
  if err != nil {
    c.AbortWithStatus(http.StatusBadRequest)
    return
  }

  if entry.Tenant == "" {
    entry.Tenant = tenantName
  } else if entry.Tenant != tenantName {
    writeError(c, http.StatusBadRequest, errors.New("Invalid Tenant parameter"))
    return
  }

  if entry.Collection == "" {
    entry.Collection = collectionName
  } else if entry.Collection != collectionName {
    writeError(c, http.StatusBadRequest, errors.New("Invalid Collection parameter"))
    return
  }

  if entry.Key == "" {
    writeError(c, http.StatusBadRequest, errors.New("Key parameter is missing from entry"))
    return
  }

  newEntry, err := a.makeProposal(entry)
  if err != nil {
    writeError(c, http.StatusInternalServerError, err)
    return
  }

  str, err := marshalJson(newEntry)
  if err == nil {
    c.Header("Content-Type", JSONContent)
    c.String(200, str)
  } else {
    writeError(c, http.StatusInternalServerError, err)
  }
}

func (a *ChangeAgent) handleGetItem(c *gin.Context) {
  tenantName := c.Param("tenant")
  collectionName := c.Param("collection")
  key := c.Param("key")

  ix, err := a.stor.GetIndexEntry(tenantName, collectionName, key)
  if err != nil {
    writeError(c, http.StatusInternalServerError, err)
    return
  }

  if ix == 0 {
    c.AbortWithStatus(http.StatusNotFound)
    return
  }

  entry, err := a.stor.GetEntry(ix)
  if err != nil {
    writeError(c, http.StatusInternalServerError, err)
    return
  }

  str, err := marshalJson(entry)
  if err == nil {
    c.Header("Content-Type", JSONContent)
    c.String(200, str)
  } else {
    writeError(c, http.StatusInternalServerError, err)
  }
}

func (a *ChangeAgent) handlePutItem(c *gin.Context) {
  tenantName := c.Param("tenant")
  collectionName := c.Param("collection")
  key := c.Param("key")

  if c.ContentType() != JSONContent {
    // TODO regexp?
    c.AbortWithStatus(http.StatusUnsupportedMediaType)
    return
  }

  defer c.Request.Body.Close()
  entry, err := unmarshalJson(c.Request.Body)
  if err != nil {
    c.AbortWithStatus(http.StatusBadRequest)
    return
  }

  if entry.Tenant == "" {
    entry.Tenant = tenantName
  } else if entry.Tenant != tenantName {
    writeError(c, http.StatusBadRequest, errors.New("Invalid Tenant parameter"))
    return
  }

  if entry.Collection == "" {
    entry.Collection = collectionName
  } else if entry.Collection != collectionName {
    writeError(c, http.StatusBadRequest, errors.New("Invalid Collection parameter"))
    return
  }

  if entry.Key == "" {
    entry.Key = key
  } else if entry.Key != key {
    writeError(c, http.StatusBadRequest, errors.New("Invalid Key parameter"))
    return
  }

  newEntry, err := a.makeProposal(entry)
  if err != nil {
    writeError(c, http.StatusInternalServerError, err)
    return
  }

  str, err := marshalJson(newEntry)
  if err == nil {
    c.Header("Content-Type", JSONContent)
    c.String(200, str)
  } else {
    writeError(c, http.StatusInternalServerError, err)
  }
}

func (a *ChangeAgent) handleDeleteItem(c *gin.Context) {
  tenantName := c.Param("tenant")
  collectionName := c.Param("collection")
  key := c.Param("key")

  err := a.stor.DeleteIndexEntry(tenantName, collectionName, key)
  if err != nil {
    writeError(c, http.StatusInternalServerError, err)
    return
  }

  c.Status(200)
}

func createTenantResponse(c *gin.Context, tenantName string) map[string]string {
  resp := make(map[string]string)
  resp["name"] = tenantName
  resp["collections"] = linkURI(c, fmt.Sprintf("/tenants/%s/collections", tenantName))
  resp["self"] = linkURI(c, fmt.Sprintf("/tenants/%s", tenantName))
  return resp
}

func linkURI(c *gin.Context, relPath string) string {
  var proto string
  if c.Request.TLS == nil {
    proto = "http"
  } else {
    proto = "https"
  }

  newPath := path.Join(c.Request.URL.RawPath, relPath)

  return fmt.Sprintf("%s://%s%s", proto, c.Request.Host, newPath)
}