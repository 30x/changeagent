#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "leveldb_native.h"

char* go_leveldb_get(
    leveldb_t* db,
    const leveldb_readoptions_t* options,
    const void* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  return leveldb_get(db, options, (const char*)key, keylen, vallen, errptr);
}

void go_leveldb_put(
    leveldb_t* db,
    const leveldb_writeoptions_t* options,
    const void* key, size_t keylen,
    const void* val, size_t vallen,
    char** errptr) {
  leveldb_put(db, options, (const char*)key, keylen,
              (const char*)val, vallen, errptr);
}

void go_leveldb_delete(
    leveldb_t* db,
    const leveldb_writeoptions_t* options,
    const void* key, size_t keylen,
    char** errptr) {
  leveldb_delete(db, options, (const char*)key, keylen, errptr);
}

void go_leveldb_iter_seek(leveldb_iterator_t* it,
    const void* k, size_t klen) {
  leveldb_iter_seek(it, (const char*)k, klen);
}

static int compare_int_key(
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
    return compare_int_key(a, alen, b, blen);
  case INDEX_KEY:
    return compare_index_key(a, alen, b, blen);
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

static const char* go_comparator_name(void* v) {
  return "ChangeAgentComparator1";
}

leveldb_comparator_t* go_create_comparator() {
  return leveldb_comparator_create(
    NULL, NULL, go_compare_bytes_impl, go_comparator_name);
}
