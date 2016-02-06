package storage

import (
  "fmt"
  "io"
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

  // Create a tenant. Tenants must be created to match the "tenantName" field in indices
  // in order to support iteration over all the records for a tenant.
  CreateTenant(tenantName string) error
  // Return whether a tenant already exists
  TenantExists(tenantName string) (bool, error)

  // Create a tenant. Tenants must be created to match the "collectionName" field in indices/
  // in order to support iteration over all the records for a collection.
  CreateCollection(tenantName, collectionName string) error
  // Return whether a collection already exists
  CollectionExists(tenantName, collectionName string) (bool, error)

  // insert an entry into the index. "tenantName" and "collectionName" may be empty.
  // "index" should match an index in the Raft index created usign "AppendEntry" from above.
  // Do not use zero for "index"
  SetIndexEntry(tenantName, collectionName, key string, index uint64) error

  // Delete an index entry. This does not affect the index that the entry points to.
  DeleteIndexEntry(tenantName, collectionName, key string) error

  // Retrieve the index of an index entry, or zero if there is no mapping.
  GetIndexEntry(tenantName, collectionName, key string) (uint64, error)

  // Iterate through all the indices for a collection, in key order, until "max" is reached
  GetCollectionIndices(tenantName, collectionName string, max uint) ([]uint64, error)

  // Get all the names of the collections for a particular tenant. This is a fairly expensive operation
  // because it iterates through all the records of the collection
  GetTenantCollections(tenantName string) ([]string, error)

  // Maintenance
  Close()
  Delete() error
  Dump(out io.Writer, max int)
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

func (e *Entry) String() string {
  s := fmt.Sprintf("{ Index: %d Term: %d ", e.Index, e.Term)
  if e.Tenant != "" {
    s += fmt.Sprintf("Tenant: %s ", e.Tenant)
  }
  if e.Collection != "" {
    s += fmt.Sprintf("Collection: %s ", e.Collection)
  }
  if e.Key != "" {
    s += fmt.Sprintf("Key: %s ", e.Key)
  }
  s += fmt.Sprintf("(%d bytes) }", len(e.Data))
  return s
}