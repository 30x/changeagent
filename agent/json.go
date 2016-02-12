package main

import (
  "encoding/json"
  "io"
  "io/ioutil"
  "time"
  "revision.aeip.apigee.net/greg/changeagent/storage"
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

  var ts time.Time
  if fullData.Timestamp == 0 {
    ts = time.Time{}
  } else {
    ts = time.Unix(0, fullData.Timestamp)
  }
  entry := storage.Entry{
    Index: fullData.Id,
    Timestamp: ts,
    Tenant: fullData.Tenant,
    Collection: fullData.Collection,
    Key: fullData.Key,
    Data: fullData.Data,
  }
  return &entry, nil
}

/*
 * Given a byte array that was parsed by "unmarshalJson," write the results to
 * the specified Writer. However, the previous results are moved to a field
 * named "data". Any fields in "metadata" that are non-empty will also be
 * added to the message.
 */
func marshalJson(entry *storage.Entry) (string, error) {
  jd := convertData(entry)
  outBody, err := json.Marshal(&jd)
  if err != nil { return "", err }
  return string(outBody), nil
}

/*
 * Same as above but marshal a whole array of changes.
 */
func marshalChanges(changes []storage.Entry) (string, error) {
  if changes == nil || len(changes) == 0 {
    return "[]", nil
  }
  changeList := convertChanges(changes)
  outBody, err := json.Marshal(changeList)
  if err != nil { return "", err }
  return string(outBody), nil
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
  var ts int64
  if entry.Timestamp != defaultTime {
    ts = entry.Timestamp.UnixNano()
  }
  return &JsonData{
    Id: entry.Index,
    Timestamp: ts,
    Tenant: entry.Tenant,
    Collection: entry.Collection,
    Key: entry.Key,
    Data: entry.Data,
  }
}