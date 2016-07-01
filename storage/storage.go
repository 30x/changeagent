package storage

import (
	"io"
	"time"

	"github.com/30x/changeagent/common"
)

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
	AppendEntry(e *common.Entry) error
	// Get term and data for entry. Return term 0 if not found.
	GetEntry(index uint64) (*common.Entry, error)
	// Get entries >= since, with a maximum count of "uint".
	// "filter" is a function that must return true for any valid entries.
	GetEntries(since uint64, max uint, filter func(*common.Entry) bool) ([]common.Entry, error)
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
