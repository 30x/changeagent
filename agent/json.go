package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"time"

	"github.com/30x/changeagent/storage"
)

var defaultTime = time.Time{}

/*
Change represents a single change represented as JSON.
*/
type Change struct {
	ID        uint64          `json:"_id,omitempty"`
	Timestamp int64           `json:"_ts,omitempty"`
	Tags      []string        `json:"tags,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
}

/*
ChangeList represents a list of changes returned from an API call, plus
information about the position of changes on the list.
*/
type ChangeList struct {
	Changes []Change `json:"changes"`
	AtStart bool     `json:"atStart"`
	AtEnd   bool     `json:"atEnd"`
}

/*
JSONError represents an error represented in JSON.
*/
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
	if err != nil {
		return storage.Entry{}, err
	}

	var fullData Change
	err = json.Unmarshal(body, &fullData)

	if err != nil || (fullData.Data == nil) && (fullData.ID == 0) && (fullData.Timestamp == 0) {
		// No "data" entry -- assume that this is raw JSON
		var rawJSON json.RawMessage
		err = json.Unmarshal(body, &rawJSON)
		if err != nil {
			return storage.Entry{}, err
		}
		return storage.Entry{
			Data: rawJSON,
		}, nil
	}
	entry := storage.Entry{
		Index: fullData.ID,
		Tags:  fullData.Tags,
		Data:  fullData.Data,
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

/*
 * Same as above but marshal a whole array of changes.
 */
func marshalChanges(changes []storage.Entry, atStart, atEnd bool, out io.Writer) error {
	cl := ChangeList{
		Changes: convertChanges(changes),
		AtStart: atStart,
		AtEnd:   atEnd,
	}
	enc := json.NewEncoder(out)
	err := enc.Encode(&cl)
	return err
}

func convertChanges(changes []storage.Entry) []Change {
	if len(changes) == 0 {
		return []Change{}
	}
	var changeList []Change
	for _, change := range changes {
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
	if err != nil {
		return result.Error()
	}
	return string(outBody)
}

func convertData(entry storage.Entry) *Change {
	ret := Change{
		ID:   entry.Index,
		Tags: entry.Tags,
		Data: entry.Data,
	}

	if entry.Timestamp != defaultTime {
		ret.Timestamp = entry.Timestamp.UnixNano()
	}

	return &ret
}
