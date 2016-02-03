package storage

import (
  "time"
  "github.com/golang/protobuf/proto"
)

type Entry struct {
  Index uint64
  Term uint64
  Timestamp time.Time
  Tenant string
  Collection string
  Key string
  Data []byte
}

/*
 * This is an interface that is implemented by Storage implementations
 * so that we could swap them in the future.
 */

type Storage interface {
  GetDataPath() string

  // Methods for all kinds of metadata
  GetMetadata(key uint) (uint64, error)
  GetRawMetadata(key uint) ([]byte, error)
  SetMetadata(key uint, val uint64) error
  SetRawMetadata(key uint, val []byte) error

  // Methods for the Raft index
  AppendEntry(e *Entry) error
  // Get term and data for entry. Return term 0 if not found.
  GetEntry(index uint64) (*Entry, error)
  // Get entries >= first and <= last
  GetEntries(first uint64, last uint64) ([]Entry, error)
  GetLastIndex() (uint64, uint64, error)
  // Return index and term of everything from index to the end
  GetEntryTerms(index uint64) (map[uint64]uint64, error)
  // Delete everything that is greater than or equal to the index
  DeleteEntries(index uint64) error

  // Methods for secondary indices
  CreateTenant(tenantName string) error
  TenantExists(tenantName string) (bool, error)
  CreateCollection(tenantName, collectionName string) error
  CollectionExists(tenantName, collectionName string) (bool, error)
  SetIndexEntry(tenantName, collectionName, key string, index uint64) error
  DeleteIndexEntry(tenantName, collectionName, key string) error
  GetIndexEntry(tenantName, collectionName, key string) (uint64, error)

  // Maintenance
  Close()
  Delete() error
}

/*
 * Use the protobuf to encode an Entry into a standard byte buffer.
 */
func EncodeEntry(entry *Entry) ([]byte, error) {
  ts := entry.Timestamp.UnixNano()
  pb := EntryPb{
    Index: &entry.Index,
    Term: &entry.Term,
    Timestamp: &ts,
    Tenant: &entry.Tenant,
    Collection: &entry.Collection,
    Key: &entry.Key,
    Data: entry.Data,
  }
  return proto.Marshal(&pb)
}

/*
 * Use the same protobuf to decode.
 */
func DecodeEntry(bytes []byte) (*Entry, error) {
  pb := EntryPb{}
  err := proto.Unmarshal(bytes, &pb)
  if err != nil { return nil, err }

  ts := time.Unix(0, pb.GetTimestamp())
  e := &Entry{
    Index: pb.GetIndex(),
    Term: pb.GetTerm(),
    Timestamp: ts,
    Tenant: pb.GetTenant(),
    Collection: pb.GetCollection(),
    Key: pb.GetKey(),
    Data: pb.GetData(),
  }
  return e, nil
}
