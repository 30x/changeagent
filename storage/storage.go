package storage

type Entry struct {
  Index uint64
  Term uint64
  Data []byte
}

type Change struct {
  Index uint64
  Tenant string
  Key string
  Data []byte
}

type Storage interface {
  // Methods for all kinds of metadat
  GetMetadata(key uint) (uint64, error)
  SetMetadata(key uint, val uint64) error

  // Methods for the Raft index
  AppendEntry(index uint64, term uint64, data []byte) error
  // Get term and data for entry. Return term 0 if not found.
  GetEntry(index uint64) (uint64, []byte, error)
  GetEntries(first uint64, last uint64) ([]Entry, error)
  GetLastIndex() (uint64, uint64, error)
  // Return index and term of everything from index to the end
  GetEntryTerms(index uint64) (map[uint64]uint64, error)
  // Delete everything that is greater than or equal to the index
  DeleteEntries(index uint64) error
  Close()
  Delete() error

  // Methods for the actual change table itself
  InsertChange(index uint64, tenant string, key string, data []byte) error
  InsertChanges(changes []Change) error
  GetChanges(lastIndex uint64, limit int) ([]Change, error)
  GetMaxChange() (uint64, error)
}
