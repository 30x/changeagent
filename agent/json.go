package main

import (
  "encoding/json"
  "io"
  "io/ioutil"
  "time"
  "revision.aeip.apigee.net/greg/changeagent/storage"
  "github.com/satori/go.uuid"
)

var defaultTime time.Time = time.Time{}

type JsonData struct {
  Id uint64 `json:"_id,omitempty"`
  Timestamp int64 `json:"_ts,omitempty"`
  Tenant string `json:"tenant,omitempty"`
  Collection string `json:"collection,omitempty"`
  Key string `json:"key,omitempty"`
  Data json.RawMessage `json:"data,omitempty"`
}

type JsonError struct {
  Error string `json:"error"`
}

type TenantLink struct {
  Name string `json:"name"`
  Id string `json:"_id,omitempty"`
  Self string `json:"_self,omitempty"`
  Collections string `json:"_collections,omitempty"`
}

type CollectionLink struct {
  Name string `json:"name"`
  Id string `json:"_id,omitempty"`
  Self string `json:"_self,omitempty"`
  Keys string `json:"_keys,omitempty"`
}

func unmarshalAny(in io.Reader, v interface{}) error {
  body, err := ioutil.ReadAll(in)
  if err != nil { return err }

  err = json.Unmarshal(body, v)
  return err
}

/*
 * Given a Reader that contains JSON data, read all the data until EOF and then
 * return an error if any of it contains invalid JSON. Return an array of
 * bytes that exactly represents the original JSON, although possibly not
 * including white space.
 */
func unmarshalJson(in io.Reader) (*storage.Entry, error) {
  body, err := ioutil.ReadAll(in)
  if err != nil { return nil, err }

  var fullData JsonData
  err = json.Unmarshal(body, &fullData)

  if err != nil || (fullData.Data == nil) && (fullData.Id == 0) && (fullData.Timestamp == 0) {
    // No "data" entry -- assume that this is raw JSON
    var rawJson json.RawMessage
    err = json.Unmarshal(body, &rawJson)
    if err != nil { return nil, err }
    return &storage.Entry{
      Data: rawJson,
    }, nil
  }
  entry := storage.Entry{
    Index: fullData.Id,
    Key: fullData.Key,
    Data: fullData.Data,
  }

  if fullData.Timestamp == 0 {
    entry.Timestamp = time.Time{}
  } else {
    entry.Timestamp = time.Unix(0, fullData.Timestamp)
  }

  if fullData.Collection != "" {
    collectionID, err := uuid.FromString(fullData.Collection)
    if err != nil { return nil, err }
    entry.Collection = &collectionID
  }

  return &entry, nil
}

/*
 * Given a byte array that was parsed by "unmarshalJson," write the results to
 * the specified Writer. However, the previous results are moved to a field
 * named "data". Any fields in "metadata" that are non-empty will also be
 * added to the message.
 */
func marshalJson(entry *storage.Entry) ([]byte, error) {
  jd := convertData(entry)
  outBody, err := json.Marshal(&jd)
  if err != nil { return nil, err }
  return outBody, nil
}

/*
 * Same as above but marshal a whole array of changes.
 */
func marshalChanges(changes []storage.Entry) ([]byte, error) {
  if changes == nil || len(changes) == 0 {
    return []byte("[]"), nil
  }
  changeList := convertChanges(changes)
  outBody, err := json.Marshal(changeList)
  if err != nil { return nil, err }
  return outBody, nil
}

func convertChanges(changes []storage.Entry) []JsonData {
  var changeList []JsonData
  for _, change := range(changes) {
    cd := convertData(&change)
    changeList = append(changeList, *cd)
  }
  return changeList
}

func marshalError(result error) string {
  msg := &JsonError{
    Error: result.Error(),
  }

  outBody, err := json.Marshal(msg)
  if err != nil { return result.Error() }
  return string(outBody)
}

func convertData(entry *storage.Entry) *JsonData {
  ret := JsonData{
    Id: entry.Index,
    Key: entry.Key,
    Data: entry.Data,
  }

  if entry.Timestamp != defaultTime {
    ret.Timestamp = entry.Timestamp.UnixNano()
  }
  if entry.Collection != nil {
    ret.Collection = entry.Collection.String()
  }

  return &ret
}