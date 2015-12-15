package storage

type Entry struct {
  Index uint64
  Term uint64
  Data []byte
}

type Storage interface {
  GetMetadata(key string) (uint64, error)
  SetMetadata(key string, val uint64) error
  AppendEntry(index uint64, term uint64, data []byte) error
  GetEntry(index uint64) (uint64, []byte, error)
  GetLastIndex() (uint64, uint64, error)
  // Return index and term of everything from index to the end
  GetEntryTerms(index uint64) (map[uint64]uint64, error)
  // Delete everything that is greater than or equal to the index
  DeleteEntries(index uint64) error
  Close()
  Delete() error
}
