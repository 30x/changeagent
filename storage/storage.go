package storage

import (
  "fmt"
  "io"
  "time"
  "github.com/golang/protobuf/proto"
  "github.com/satori/go.uuid"
)

type Entry struct {
  Index uint64
  Type int32
  Term uint64
  Timestamp time.Time
  Tenant uuid.UUID
  Collection uuid.UUID
  Key string
  Data []byte
}

type Collection struct {
  Id   uuid.UUID
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
  // Get entries >= since, with a maximum count of "uint".
  // "filter" is a function that must return true for any valid entries.
  GetEntries(since uint64, max uint, filter func(*Entry) bool) ([]Entry, error)
  // Return the highest index and term in the database
  GetLastIndex() (uint64, uint64, error)
  // Return index and term of everything from index to the end
  GetEntryTerms(index uint64) (map[uint64]uint64, error)
  // Delete everything that is greater than or equal to the index
  DeleteEntries(index uint64) error

  // Methods for the tenant-specific index
  CreateTenantEntry(e *Entry) error
  GetTenantEntry(tenant uuid.UUID, ix uint64) (*Entry, error)
  DeleteTenantEntry(tenant uuid.UUID, ix uint64) error
  // Get entries >= last, but only for the specified tenant
  GetTenantEntries(tenant uuid.UUID, last uint64, max uint, filter func(*Entry) bool) ([]Entry, error)

  // Create a tenant. Tenants must be created to match the "tenantName" field in indices
  // in order to support iteration over all the records for a tenant.
  CreateTenant(tenantName string, id uuid.UUID) error
  // Given the name of a tenant, return its ID, or an empty string if the tenant does not exist
  GetTenantByName(tenantName string) (uuid.UUID, error)
  // Given the ID of a tenant, return its name, or an empty string if the tenant does not exist
  GetTenantByID(tenantID uuid.UUID) (string, error)
  // Get a list of all the tenants in name order
  GetTenants() ([]Collection, error)

  // Create a collection. Tenants must be created to match the "collectionName" field in indices
  // in order to support iteration over all the records for a collection.
  // Parameters are the NAME (pretty name) of the tenant, and the ID (UUID) of the collection.
  // Returns the UUID of the new collection.
  CreateCollection(tenantID uuid.UUID, collectionName string, collectionId uuid.UUID) error
  // Given the NAME of a collection and a tenant, return the unique ID of the collection
  GetCollectionByName(tenantID uuid.UUID, collectionName string) (uuid.UUID, error)
  // Given a collection ID, return the collection name and the tenant ID
  GetCollectionByID(collectionID uuid.UUID) (string, uuid.UUID, error)
  // Get all the IDs of the collections for a particular tenant.
  GetTenantCollections(tenantID uuid.UUID) ([]Collection, error)

  // insert an entry into the index. "collectionID" must match a previous ID.
  // "index" should match an index in the Raft index created usign "AppendEntry" from above.
  // Do not use zero for "index."
  SetIndexEntry(collectionId uuid.UUID, key string, index uint64) error

  // Delete an index entry. This does not affect the entry that the index points to.
  DeleteIndexEntry(collectionId uuid.UUID, key string) error

  // Retrieve the index of an index entry, or zero if there is no mapping.
  GetIndexEntry(collectionId uuid.UUID, key string) (uint64, error)

  // Iterate through all the indices for a collection, in key order, until "max" is reached.
  // If "lastKey" is non-empty, then begin iterating with the key right AFTER that one.
  GetCollectionIndices(collectionId uuid.UUID, lastKey string, max uint) ([]uint64, error)

  // Maintenance
  Close()
  Delete() error
  GetDataPath() string
  Dump(out io.Writer, max int)
}

/*
 * Use the protobuf to encode an Entry into a standard byte buffer.
 */
func EncodeEntry(entry *Entry) ([]byte, error) {
  ts := entry.Timestamp.UnixNano()

  var collectionID []byte
  if !uuid.Equal(entry.Collection, uuid.Nil) {
    collectionID = entry.Collection.Bytes()
  } else {
    collectionID = nil
  }

  var tenantID []byte
  if !uuid.Equal(entry.Tenant, uuid.Nil) {
    tenantID = entry.Tenant.Bytes()
  } else {
    tenantID = nil
  }

  pb := EntryPb{
    Index: &entry.Index,
    Type: &entry.Type,
    Term: &entry.Term,
    Timestamp: &ts,
    Tenant: tenantID,
    Collection: collectionID,
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

  var collectionID uuid.UUID
  if pb.GetCollection() != nil {
    id := uuid.FromBytesOrNil(pb.GetCollection())
    collectionID = id
  }
  var tenantID uuid.UUID
  if pb.GetTenant() != nil {
    id := uuid.FromBytesOrNil(pb.GetTenant())
    tenantID = id
  }

  e := &Entry{
    Index: pb.GetIndex(),
    Type: pb.GetType(),
    Term: pb.GetTerm(),
    Timestamp: ts,
    Tenant: tenantID,
    Collection: collectionID,
    Key: pb.GetKey(),
    Data: pb.GetData(),
  }
  return e, nil
}

func (e *Entry) String() string {
  s := fmt.Sprintf("{ Index: %d Term: %d Type: %d ",
    e.Index, e.Term, e.Type)
  if !uuid.Equal(e.Tenant, uuid.Nil) {
    s += fmt.Sprintf("Tenant: %s ", e.Tenant)
  }
  if !uuid.Equal(e.Collection, uuid.Nil) {
    s += fmt.Sprintf("Collection: %s ", e.Collection)
  }
  if e.Key != "" {
    s += fmt.Sprintf("Key: %s ", e.Key)
  }
  s += fmt.Sprintf("(%d bytes) }", len(e.Data))
  return s
}