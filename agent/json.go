package agent

import (
  "errors"
  "encoding/json"
  "io"
  "io/ioutil"
  "revision.aeip.apigee.net/greg/changeagent/storage"
)

var InvalidJsonError = errors.New("Invalid JSON")

type JsonData struct {
  Id uint64 `json:"_id,omitempty"`
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
func unmarshalJson(in io.Reader) ([]byte, error) {
  body, err := ioutil.ReadAll(in)
  if err != nil { return nil, err }

  var rawJson json.RawMessage
  err = json.Unmarshal(body, &rawJson)
  if err != nil { return nil, err }

  return rawJson, nil
}

/*
 * Given a byte array that was parsed by "unmarshalJson," write the results to
 * the specified Writer. However, the previous results are moved to a field
 * named "data". Any fields in "metadata" that are non-empty will also be
 * added to the message.
 */
func marshalJson(body []byte, metadata *JsonData, out io.Writer) error {
  if metadata == nil {
    metadata = &JsonData{}
  }
  metadata.Data = body

  outBody, err := json.Marshal(metadata)
  if err != nil { return err }
  _, err = out.Write(outBody)
  return err
}

/*
 * Same as above but marshal a whole array of changes.
 */
func marshalChanges(changes []storage.Change, out io.Writer) error {
  if changes == nil || len(changes) == 0 {
    out.Write([]byte("[]"))
    return nil
  }

  var changeList []JsonData

  for _, change := range(changes) {
    cd := JsonData{
      Id: change.Index,
      Data: change.Data,
    }
    changeList = append(changeList, cd)
  }

  outBody, err := json.Marshal(changeList)
  if err != nil { return err }
  _, err = out.Write(outBody)
  return err
}

func marshalError(result error, out io.Writer) error {
  msg := &JsonError{
    Error: result.Error(),
  }

  outBody, err := json.Marshal(msg)
  if err != nil { return err }
  _, err = out.Write(outBody)
  return err
}
