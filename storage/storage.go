package storage

import (
  "fmt"
  "io"
  "time"
  "github.com/golang/protobuf/proto"
)

type Entry struct {
  Index uint64
  Type int32
  Term uint64
  Timestamp time.Time
  Tenant string
  Collection string
  Key string
  Data []byte
}

type Collection struct {
  Id   string
  Name string
}

/*
 * This is an interface that is implemented by Storage implementations
 * so that we could swap them in the future.
 */

type Storage interface {
  // Methods for all kinds of metadata used for maintenance and operation
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
  // Return the highest index and term in the database
  GetLastIndex() (uint64, uint64, error)
  // Return index and term of everything from index to the end
  GetEntryTerms(index uint64) (map[uint64]uint64, error)
  // Delete everything that is greater than or equal to the index
  DeleteEntries(index uint64) error

  // Create a tenant. Tenants must be created to match the "tenantName" field in indices
  // in order to support iteration over all the records for a tenant.
  CreateTenant(tenantName string) (string, error)
  // Given the name of a tenant, return its ID, or an empty string if the tenant does not exist
  GetTenantByName(tenantName string) (string, error)
  // Given the ID of a tenant, return its name, or an empty string if the tenant does not exist
  GetTenantByID(tenantID string) (string, error)
  // Get a list of all the tenants in name order
  GetTenants() ([]Collection, error)

  // Get all the IDs of the collections for a particular tenant.
  GetTenantCollections(tenantName string) ([]string, error)

  // Create a tenant. Tenants must be created to match the "collectionName" field in indices/
  // in order to support iteration over all the records for a collection.
  CreateCollection(tenantId, collectionName string) error
  // Given the name of a collection and a tenant, return the unique ID of the collection
  GetCollection(tenantName, collectionName string) (string, error)

  // insert an entry into the index. "tenantName" and "collectionName" may be empty.
  // "index" should match an index in the Raft index created usign "AppendEntry" from above.
  // Do not use zero for "index"
  SetIndexEntry(tenantId, collectionId, key string, index uint64) error

  // Delete an index entry. This does not affect the entry that the index points to.
  DeleteIndexEntry(tenantId, collectionId, key string) error

  // Retrieve the index of an index entry, or zero if there is no mapping.
  GetIndexEntry(tenantId, collectionId, key string) (uint64, error)

  // Iterate through all the indices for a collection, in key order, until "max" is reached
  GetCollectionIndices(tenantId, collectionId string, max uint) ([]uint64, error)

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
    Type: &entry.Type,
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
    Type: pb.GetType(),
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
  s := fmt.Sprintf("{ Index: %d Term: %d Type: %d ", e.Index, e.Term, e.Type)
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