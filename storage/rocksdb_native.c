#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rocksdb_native.h"

static rocksdb_comparator_t* intComparator;
static rocksdb_comparator_t* indexComparator;

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

static unsigned short mins(unsigned short a, unsigned short b)
{
  return (a < b) ? a : b;
}

static int keycmp(const char* a, unsigned short alen, const char* b, unsigned short blen)
{
  int cmp = strncmp(a, b, mins(alen, blen));
  if (cmp == 0) {
    if (alen < blen) {
      return -1;
    }
    if (alen > blen) {
      return 1;
    }
    return 0;
  }
  return cmp;
}

static int compare_index_key(
  void* c,
  const char* a, size_t alen,
  const char* b, size_t blen)
{
  unsigned short tenlena = *((unsigned short*)(a + 1));
  unsigned short tenlenb = *((unsigned short*)(b + 1));
  unsigned short colllena = *((unsigned short*)(a + 3));
  unsigned short colllenb = *((unsigned short*)(b + 3));
  unsigned short keylena = *((unsigned short*)(a + 5));
  unsigned short keylenb = *((unsigned short*)(b + 5));

  size_t apos = 7;
  size_t bpos = 7;

  // First compare the tenant name (always)
  int cmp = keycmp(a + apos, tenlena, b + bpos, tenlenb);
  if (cmp != 0) {
    return cmp;
  }

  // Check for special "range" flags
  if (colllena == START_RANGE) {
    return (colllenb == START_RANGE ? 0 : -1);
  }
  if (colllena == END_RANGE) {
    return (colllenb == END_RANGE ? 0 : 1);
  }
  if (colllenb == START_RANGE) {
    return 1;
  }
  if (colllenb == END_RANGE) {
    return -1;
  }

  // Move into position and compare the collection name
  if (tenlena > 0) {
    apos += (tenlena + 1);
  }
  if (tenlenb > 0) {
    bpos += (tenlenb + 1);
  }
  cmp = keycmp(a + apos, colllena, b + bpos, colllenb);
  if (cmp != 0) {
    return cmp;
  }

  // Keep going, check more "range" flags
  if (keylena == START_RANGE) {
    return (keylenb == START_RANGE ? 0 : -1);
  }
  if (keylena == END_RANGE) {
    return (keylenb == END_RANGE ? 0 : 1);
  }
  if (keylenb == START_RANGE) {
    return 1;
  }
  if (keylenb == END_RANGE) {
    return -1;
  }

  // Finally compare the key!
  if (colllena > 0) {
    apos += (colllena + 1);
  }
  if (colllenb > 0) {
    bpos += (colllenb + 1);
  }
  cmp = keycmp(a + apos, keylena, b + bpos, keylenb);
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
  case ENTRY_KEY:
    return compare_int_key(state, a, alen, b, blen);
  case INDEX_KEY:
    return compare_index_key(state, a, alen, b, blen);
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

static const char* index_comparator_name(void* v) {
  return INDEX_COMPARATOR_NAME;
}

void go_rocksdb_init() {
  intComparator = rocksdb_comparator_create(
    NULL, NULL, compare_int_key, int_comparator_name);
  indexComparator = rocksdb_comparator_create(
    NULL, NULL, compare_index_key, index_comparator_name);
}

char* go_rocksdb_open(
  const char* directory,
  size_t cacheSize,
  GoRocksDb** ret)
{
  char* cfNames[4];
  rocksdb_options_t* cfOpts[4];
  rocksdb_column_family_handle_t* cfHandles[4];
  rocksdb_options_t* mainOptions;
  rocksdb_cache_t* cache;
  rocksdb_block_based_table_options_t* blockOpts;
  char* err = NULL;
  rocksdb_t* db;
  int i;

  cfNames[0] = "default";
  cfNames[1] = "metadata";
  cfNames[2] = "indices";
  cfNames[3] = "entries";

  cache = rocksdb_cache_create_lru(cacheSize);

  blockOpts = rocksdb_block_based_options_create();
  rocksdb_block_based_options_set_block_cache(blockOpts, cache);

  mainOptions = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(mainOptions, 1);
  rocksdb_options_set_create_missing_column_families(mainOptions, 1);
  rocksdb_options_set_block_based_table_factory(mainOptions, blockOpts);

  cfOpts[0] = rocksdb_options_create();
  cfOpts[1] = rocksdb_options_create();
  rocksdb_options_set_comparator(cfOpts[1], intComparator);
  cfOpts[2] = rocksdb_options_create();
  rocksdb_options_set_comparator(cfOpts[2], indexComparator);
  cfOpts[3] = rocksdb_options_create();
  rocksdb_options_set_comparator(cfOpts[3], intComparator);

  db = rocksdb_open_column_families(
    mainOptions, directory, 4, (const char**)cfNames,
    (const rocksdb_options_t**)cfOpts,
    cfHandles, &err
  );

  rocksdb_options_destroy(mainOptions);
  for (i = 0; i < 4; i++) {
    rocksdb_options_destroy(cfOpts[i]);
  }
  rocksdb_block_based_options_destroy(blockOpts);

  if (err == NULL) {
    GoRocksDb* h = (GoRocksDb*)malloc(sizeof(GoRocksDb));
    h->db = db;
    h->dflt = cfHandles[0];
    h->metadata = cfHandles[1];
    h->indices = cfHandles[2];
    h->entries = cfHandles[3];
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
  rocksdb_column_family_handle_destroy(h->indices);
  rocksdb_close(h->db);
  rocksdb_cache_destroy(h->cache);
}