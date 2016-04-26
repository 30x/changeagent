#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rocksdb_native.h"

static rocksdb_comparator_t* intComparator;
static rocksdb_comparator_t* stringComparator;

static size_t minsize(size_t a, size_t b)
{
  return (a < b) ? a : b;
}

char* go_rocksdb_get(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* cf,
    const void* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  return rocksdb_get_cf(db, options, cf, (const char*)key, keylen, vallen, errptr);
}

void go_rocksdb_put(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    rocksdb_column_family_handle_t* cf,
    const void* key, size_t keylen,
    const void* val, size_t vallen,
    char** errptr) {
  rocksdb_put_cf(db, options, cf, (const char*)key, keylen,
              (const char*)val, vallen, errptr);
}

void go_rocksdb_delete(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    rocksdb_column_family_handle_t* cf,
    const void* key, size_t keylen,
    char** errptr) {
  rocksdb_delete_cf(db, options, cf, (const char*)key, keylen, errptr);
}

void go_rocksdb_iter_seek(rocksdb_iterator_t* it,
    const void* k, size_t klen) {
  rocksdb_iter_seek(it, (const char*)k, klen);
}

static int compare_int_key(
  void* c,
  const char* a, size_t alen,
  const char* b, size_t blen)
{
  if ((alen != 9) || (blen != 9)) { return 0; }

  unsigned long long* av = (unsigned long long*)(a + 1);
  unsigned long long* bv = (unsigned long long*)(b + 1);

  if (*av > *bv) {
    return 1;
  } else if (*av < *bv) {
    return -1;
  } else {
    return 0;
  }
}

static int compare_string_key(
  void* c,
  const char* a, size_t alen,
  const char* b, size_t blen)
{
  if ((alen < 1) || (blen < 1)) { return 0; }

  if ((alen == 1) && (blen > 1)) {
    return -1;
  }
  if ((alen > 1) && (blen == 1)) {
    return 1;
  }

  size_t cmpsize = minsize(alen, blen);
  int cmp = strncmp(a + 1, b + 1, cmpsize - 1);
  if (cmp == 0) {
    if (alen < blen) {
      return -1;
    } else if (blen > alen) {
      return 1;
    }
  }
  return cmp;
}

static int go_compare_bytes_impl(
  void* state,
  const char* a, size_t alen,
  const char* b, size_t blen)
{
  if ((alen < 1) || (blen < 1)) { return 0; }

  int vers1 = (a[0] >> 4) & 0xf;
  int vers2 = (b[0] >> 4) & 0xf;
  if ((vers1 != KEY_VERSION) || (vers2 != KEY_VERSION)) { return vers1 + 100; }

  int type1 = a[0] & 0xf;
  int type2 = b[0] & 0xf;

  if (type1 < type2) {
    return -1;
  }
  if (type1 > type2) {
    return 1;
  }

  switch (type1) {
  case METADATA_KEY:
    return compare_string_key(state, a, alen, b, blen);
  case ENTRY_KEY:
    return compare_int_key(state, a, alen, b, blen);
  default:
    return 999;
  }
}

int go_compare_bytes(
  void* state,
  const void* a, size_t alen,
  const void* b, size_t blen) {
    return go_compare_bytes_impl(state, a, alen, b, blen);
}

static const char* int_comparator_name(void* v) {
  return INT_COMPARATOR_NAME;
}

static const char* string_comparator_name(void* v) {
  return STRING_COMPARATOR_NAME;
}

void go_rocksdb_init() {
  intComparator = rocksdb_comparator_create(
    NULL, NULL, compare_int_key, int_comparator_name);
  stringComparator = rocksdb_comparator_create(
    NULL, NULL, compare_string_key, string_comparator_name);
}

char* go_rocksdb_open(
  const char* directory,
  size_t cacheSize,
  GoRocksDb** ret)
{
  char* cfNames[NUM_CFS];
  rocksdb_options_t* cfOpts[NUM_CFS];
  rocksdb_column_family_handle_t* cfHandles[NUM_CFS];
  rocksdb_options_t* mainOptions;
  rocksdb_cache_t* cache;
  rocksdb_block_based_table_options_t* blockOpts;
  char* err = NULL;
  rocksdb_t* db;
  int i;

  cfNames[0] = "default";
  cfNames[1] = "metadata";
  cfNames[2] = "entries";

  cache = rocksdb_cache_create_lru(cacheSize);

  blockOpts = rocksdb_block_based_options_create();
  rocksdb_block_based_options_set_block_cache(blockOpts, cache);

  mainOptions = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(mainOptions, 1);
  rocksdb_options_set_create_missing_column_families(mainOptions, 1);
  rocksdb_options_set_block_based_table_factory(mainOptions, blockOpts);

  cfOpts[0] = rocksdb_options_create();
  cfOpts[1] = rocksdb_options_create();
  rocksdb_options_set_comparator(cfOpts[1], stringComparator);
  cfOpts[2] = rocksdb_options_create();
  rocksdb_options_set_comparator(cfOpts[2], intComparator);

  db = rocksdb_open_column_families(
    mainOptions, directory, NUM_CFS, (const char**)cfNames,
    (const rocksdb_options_t**)cfOpts,
    cfHandles, &err
  );

  rocksdb_options_destroy(mainOptions);
  for (i = 0; i < NUM_CFS; i++) {
    rocksdb_options_destroy(cfOpts[i]);
  }
  rocksdb_block_based_options_destroy(blockOpts);

  if (err == NULL) {
    GoRocksDb* h = (GoRocksDb*)malloc(sizeof(GoRocksDb));
    h->db = db;
    h->dflt = cfHandles[0];
    h->metadata = cfHandles[1];
    h->entries = cfHandles[2];
    h->cache = cache;
    *ret = h;
  }

  return err;
}

void go_rocksdb_close(GoRocksDb* h)
{
  rocksdb_column_family_handle_destroy(h->dflt);
  rocksdb_column_family_handle_destroy(h->metadata);
  rocksdb_column_family_handle_destroy(h->entries);
  rocksdb_close(h->db);
  rocksdb_cache_destroy(h->cache);
}