package main

import (
  "bytes"
  "encoding/json"
  "io"
  "io/ioutil"
  "time"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

var defaultTime = time.Time{}

type JSONData struct {
  ID uint64 `json:"_id,omitempty"`
  Timestamp int64 `json:"_ts,omitempty"`
  Tags []string `json:"tags,omitempty"`
  Data json.RawMessage `json:"data,omitempty"`
}

type JSONError struct {
  Error string `json:"error"`
}

/*
 * Given a Reader that contains JSON data, read all the data until EOF and then
 * return an error if any of it contains invalid JSON. Return an array of
 * bytes that exactly represents the original JSON, although possibly not
 * including white space.
 */
func unmarshalJSON(in io.Reader) (storage.Entry, error) {
  // We are going to decode twice, because we accept two JSON formats.
  // That means we need to read the whole body into memory.
  body, err := ioutil.ReadAll(in)
  if err != nil { return storage.Entry{}, err }

  var fullData JSONData
  err = json.Unmarshal(body, &fullData)

  if err != nil || (fullData.Data == nil) && (fullData.ID == 0) && (fullData.Timestamp == 0) {
    // No "data" entry -- assume that this is raw JSON
    var rawJSON json.RawMessage
    err = json.Unmarshal(body, &rawJSON)
    if err != nil { return storage.Entry{}, err }
    return storage.Entry{
      Data: rawJSON,
    }, nil
  }
  entry := storage.Entry{
    Index: fullData.ID,
    Tags: fullData.Tags,
    Data: fullData.Data,
  }

  if fullData.Timestamp == 0 {
    entry.Timestamp = time.Time{}
  } else {
    entry.Timestamp = time.Unix(0, fullData.Timestamp)
  }

  return entry, nil
}

/*
 * Given a byte array that was parsed by "unmarshalJson," write the results to
 * the specified Writer. However, the previous results are moved to a field
 * named "data". Any fields in "metadata" that are non-empty will also be
 * added to the message.
 */
func marshalJSON(entry storage.Entry, out io.Writer) error {
  jd := convertData(entry)
  enc := json.NewEncoder(out)
  err := enc.Encode(&jd)
  return err
}

func marshalJSONToString(entry storage.Entry) (string, error) {
  out := &bytes.Buffer{}
  err := marshalJSON(entry, out)
  if err != nil { return "", err }
  return out.String(), nil
}

/*
 * Same as above but marshal a whole array of changes.
 */
func marshalChanges(changes []storage.Entry, out io.Writer) error {
  if changes == nil || len(changes) == 0 {
    out.Write([]byte("[]"))
    return nil
  }
  changeList := convertChanges(changes)
  enc := json.NewEncoder(out)
  err := enc.Encode(changeList)
  return err
}

func convertChanges(changes []storage.Entry) []JSONData {
  var changeList []JSONData
  for _, change := range(changes) {
    cd := convertData(change)
    changeList = append(changeList, *cd)
  }
  return changeList
}

func marshalError(result error) string {
  msg := &JSONError{
    Error: result.Error(),
  }

  outBody, err := json.Marshal(msg)
  if err != nil { return result.Error() }
  return string(outBody)
}

func convertData(entry storage.Entry) *JSONData {
  ret := JSONData{
    ID: entry.Index,
    Tags: entry.Tags,
    Data: entry.Data,
  }

  if entry.Timestamp != defaultTime {
    ret.Timestamp = entry.Timestamp.UnixNano()
  }

  return &ret
}