package common

import (
	"fmt"
	"time"

	"github.com/30x/changeagent/protobufs"
	"github.com/golang/protobuf/proto"
)

/*
An Entry represents a single record in the storage system. It is used by many
packages as the basis of the data model
*/
type Entry struct {
	Index     uint64
	Type      int32
	Term      uint64
	Timestamp time.Time
	Tags      []string
	Data      []byte
}

/*
EncodePb turns a regular Entry struct into the special struct required
in order to generate a protobuf.
*/
func (e *Entry) EncodePb() protobufs.EntryPb {
	ts := e.Timestamp.UnixNano()
	pb := protobufs.EntryPb{
		Index:     proto.Uint64(e.Index),
		Type:      proto.Int32(e.Type),
		Timestamp: proto.Int64(ts),
	}

	if e.Term != 0 {
		pb.Term = proto.Uint64(e.Term)
	}
	if len(e.Tags) > 0 {
		pb.Tags = e.Tags
	}
	if len(e.Data) > 0 {
		pb.Data = e.Data
	}
	return pb
}

/*
Encode encodes an entry in to a byte array using the protobuf defined in this
package.
*/
func (e *Entry) Encode() []byte {
	pb := e.EncodePb()

	buf, err := proto.Marshal(&pb)
	if err != nil {
		panic(err.Error())
	}

	return buf
}

/*
DecodeEntryFromPb parses the special struct from a protobuf entry and
turns it into a regular Entry.
*/
func DecodeEntryFromPb(pb protobufs.EntryPb) *Entry {
	ts := time.Unix(0, pb.GetTimestamp())
	e := &Entry{
		Index:     pb.GetIndex(),
		Type:      pb.GetType(),
		Term:      pb.GetTerm(),
		Timestamp: ts,
		Tags:      pb.GetTags(),
		Data:      pb.GetData(),
	}
	return e
}

/*
DecodeEntry turns a protobuf created by EncodeEntry back into an Entry.
It will return an error if the specified array is not a valid protobuf for
the Entry type.
*/
func DecodeEntry(buf []byte) (*Entry, error) {
	pb := protobufs.EntryPb{}
	err := proto.Unmarshal(buf, &pb)
	if err != nil {
		return nil, err
	}

	return DecodeEntryFromPb(pb), nil
}

/*
MatchesAllTags returns true if the specified entry contains all the tags in the
"tags" array. It will always return true for an empty "tags" array.
*/
func (e *Entry) MatchesAllTags(tags []string) bool {
	for _, tag := range tags {
		if !e.MatchesTag(tag) {
			return false
		}
	}
	return true
}

/*
MatchesAnyTag returns true if the specified entry contains any one of the tags
in the "tags" array. It will always return true for an empty "tags" array.
*/
func (e *Entry) MatchesAnyTag(tags []string) bool {
	if len(tags) == 0 {
		return true
	}
	for _, tag := range tags {
		if e.MatchesTag(tag) {
			return true
		}
	}
	return false
}

/*
MatchesTag returns true if the specified entry contains the tag from the "tag"
argument.
*/
func (e *Entry) MatchesTag(tag string) bool {

	for _, etag := range e.Tags {
		if tag == etag {
			return true
		}
	}
	return false
}

func (e *Entry) String() string {
	return fmt.Sprintf("{ Index: %d Term: %d Type: %d (%d bytes) }",
		e.Index, e.Term, e.Type, len(e.Data))
}
