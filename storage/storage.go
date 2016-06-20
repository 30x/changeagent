package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/golang/protobuf/proto"
)

//go:generate protoc --go_out=. rocksdb_records.proto

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
Storage is an interface that abstracts all persistence-related operations
in changeagent. It is the only interface that other modules should use within
the project.
*/
type Storage interface {
	// Methods for all kinds of metadata used for maintenance and operation
	GetUintMetadata(key string) (uint64, error)
	GetMetadata(key string) ([]byte, error)
	SetUintMetadata(key string, val uint64) error
	SetMetadata(key string, val []byte) error

	// Methods for the Raft index
	AppendEntry(e *Entry) error
	// Get term and data for entry. Return term 0 if not found.
	GetEntry(index uint64) (*Entry, error)
	// Get entries >= since, with a maximum count of "uint".
	// "filter" is a function that must return true for any valid entries.
	GetEntries(since uint64, max uint, filter func(*Entry) bool) ([]Entry, error)
	// Return the highest index and term in the database
	GetLastIndex() (uint64, uint64, error)
	// Return the lowest index in the databsae
	GetFirstIndex() (uint64, error)
	// Return index and term of everything from index to the end
	GetEntryTerms(index uint64) (map[uint64]uint64, error)
	// Delete everything that is greater than or equal to the index
	DeleteEntriesAfter(index uint64) error
	// Truncate entries, ensuring that at least "minEntries" are left in the database,
	// and that any entries younger than "maxAge" are retained.
	// This could run for a long time -- a goroutine is advised.
	// This operation is also idempotent.
	// Return the number of entries actually deleted
	Truncate(minEntries uint64, maxAge time.Duration) (uint64, error)

	// Maintenance
	Close()
	Delete() error
	GetDataPath() string
	Dump(out io.Writer, max int)
}

/*
EncodeEntry encodes an entry in to a byte array using the protobuf defined in this
package. It could return an error but it's not clear why.

TODO why don't we just panic on error here?
*/
func EncodeEntry(entry *Entry) ([]byte, error) {
	ts := entry.Timestamp.UnixNano()
	pb := EntryPb{
		Index:     &entry.Index,
		Type:      &entry.Type,
		Timestamp: &ts,
	}

	if entry.Term != 0 {
		pb.Term = &entry.Term
	}
	if len(entry.Tags) > 0 {
		pb.Tags = entry.Tags
	}

	header, err := proto.Marshal(&pb)
	if err != nil {
		return nil, err
	}

	// Now concatenate lengths, header, and body into a buffer

	lenbuf := &bytes.Buffer{}
	hdrlen := uint32(len(header))
	bodylen := uint32(len(entry.Data))
	binary.Write(lenbuf, storageByteOrder, &hdrlen)
	binary.Write(lenbuf, storageByteOrder, &bodylen)

	buf := append(lenbuf.Bytes(), header...)
	buf = append(buf, entry.Data...)

	return buf, nil
}

/*
DecodeEntry turns a protobuf created by EncodeEntry back into an Entry.
It will return an error if the specified array is not a valid protobuf for
the Entry type.
*/
func DecodeEntry(rawbuf []byte) (*Entry, error) {
	buf := bytes.NewBuffer(rawbuf)

	var hdrlen uint32
	var bodylen uint32

	binary.Read(buf, storageByteOrder, &hdrlen)
	binary.Read(buf, storageByteOrder, &bodylen)

	hdrdata := rawbuf[8 : 8+hdrlen]

	pb := EntryPb{}
	err := proto.Unmarshal(hdrdata, &pb)
	if err != nil {
		return nil, err
	}

	ts := time.Unix(0, pb.GetTimestamp())
	e := Entry{
		Index:     pb.GetIndex(),
		Type:      pb.GetType(),
		Term:      pb.GetTerm(),
		Timestamp: ts,
		Tags:      pb.GetTags(),
	}

	if bodylen > 0 {
		e.Data = rawbuf[8+hdrlen:]
	}

	return &e, nil
}

/*
MatchesTags returns true if the specified entry contains all the tags in the
"tags" array.
*/
func (e *Entry) MatchesTags(tags []string) bool {
	for _, tag := range tags {
		if !e.MatchesTag(tag) {
			return false
		}
	}
	return true
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
