#ifndef GO_LEVELDB_NATIVE_H
#define GO_LEVELDB_NATIVE_H
#endif

#include <rocksdb/c.h>

/* These have to match constants in rocksdb_convert.go */
#define KEY_VERSION 1
#define METADATA_KEY 1
#define INDEX_KEY 2
#define TENANT_INDEX_KEY 3
#define ENTRY_KEY 10
#define START_RANGE  (0xffff - 1)
#define END_RANGE    (0xffff)

#define NUM_CFS 5

#define INT_COMPARATOR_NAME "CA-INT-V1"
#define INDEX_COMPARATOR_NAME "CA-INDEX-V1"
#define TENANT_INDEX_COMPARATOR_NAME "TEN-INDEX-V1"

typedef struct {
  rocksdb_t* db;
  rocksdb_column_family_handle_t* dflt;
  rocksdb_column_family_handle_t* metadata;
  rocksdb_column_family_handle_t* indices;
  rocksdb_column_family_handle_t* tenantIndices;
  rocksdb_column_family_handle_t* entries;
  rocksdb_cache_t* cache;
} GoRocksDb;

/*
 * One-time init of comparators and stuff like that. Should be run using "once".
 */
extern void go_rocksdb_init();

/*
 * Do all the work around creating options and column families and opening
 * the database.
 */
extern char* go_rocksdb_open(
  const char* directory,
  size_t cacheSize,
  GoRocksDb** h);

/*
 * Close the database.
 */
extern void go_rocksdb_close(GoRocksDb* h);

/*
 * Wrapper around rocksdb_get because it's a pain to cast to and from char* in
 * go code itself.
 */
extern char* go_rocksdb_get(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* cf,
    const void* key, size_t keylen,
    size_t* vallen,
    char** errptr);

/* Do wrapper for rocksdb_put */
extern void go_rocksdb_put(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    rocksdb_column_family_handle_t* cf,
    const void* key, size_t keylen,
    const void* val, size_t vallen,
    char** errptr);

/* Do wrapper for rocksdb_delete */
extern void go_rocksdb_delete(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    rocksdb_column_family_handle_t* cf,
    const void* key, size_t keylen,
    char** errptr);

/* Do wrapper for rocksdb_seek */
extern void go_rocksdb_iter_seek(rocksdb_iterator_t* it,
    const void* k, size_t klen);

/*
 * Wrapper for internal comparator to facilitate testing from Go.
 */
extern int go_compare_bytes(
  void* state,
  const void* a, size_t alen,
  const void* b, size_t blen);
