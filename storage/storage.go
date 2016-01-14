package storage

import "time"

type Entry struct {
  Index uint64
  Term uint64
  Timestamp time.Time
  Tenant string
  Collection string
  Key string
  Data []byte
}

type Storage interface {
  GetDataPath() string

  // Methods for all kinds of metadata
  GetMetadata(key uint) (uint64, error)
  GetRawMetadata(key uint) ([]byte, error)
  SetMetadata(key uint, val uint64) error
  SetRawMetadata(key uint, val []byte) error

  // Methods for the Raft index
  AppendEntry(index uint64, term uint64, data []byte) error
  // Get term and data for entry. Return term 0 if not found.
  GetEntry(index uint64) (uint64, []byte, error)
  // Get entries >= first and <= last
  GetEntries(first uint64, last uint64) ([]Entry, error)
  GetLastIndex() (uint64, uint64, error)
  // Return index and term of everything from index to the end
  GetEntryTerms(index uint64) (map[uint64]uint64, error)
  // Delete everything that is greater than or equal to the index
  DeleteEntries(index uint64) error
  Close()
  Delete() error
}
