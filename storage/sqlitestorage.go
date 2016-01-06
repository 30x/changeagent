package storage

/*
#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#cgo LDFLAGS: -lsqlite3

static int _sqlite3_bind_text(sqlite3_stmt *s, int n, char *p, int np) {
  return sqlite3_bind_text(s, n, p, np, SQLITE_TRANSIENT);
}
static int _sqlite3_bind_blob(sqlite3_stmt *s, int n, void *p, int np) {
  return sqlite3_bind_blob(s, n, p, np, SQLITE_TRANSIENT);
}
*/
import "C"

import (
  "errors"
  "fmt"
  "os"
  "strconv"
  "unsafe"
  "revision.aeip.apigee.net/greg/changeagent/log"
)

type SqliteStorage struct {
  db *C.sqlite3
  fileName string
  dbLock *C.sqlite3_mutex
  stmts map[string]*C.sqlite3_stmt
}

func CreateSqliteStorage(dbFile string) (*SqliteStorage, error) {
  cName := C.CString(dbFile)
  defer C.free(unsafe.Pointer(cName))
  var db *C.sqlite3

  success := false
  // We may have multiple databases open, so need fullmutex
  flags := C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE | C.SQLITE_OPEN_FULLMUTEX
  log.Debugf("Opening Sqlite database in %s", dbFile)
  e := C.sqlite3_open_v2(cName, &db, C.int(flags), nil)
  if e != 0 {
    // Generate the first error manually because we can't lock the DB yet
    msg := C.sqlite3_errmsg(db)
    code := C.sqlite3_errcode(db)
    extCode := C.sqlite3_extended_errcode(db)
    return nil, errors.New(
      fmt.Sprintf("Can't open database: code = %d extended = %d: %s",
        code, extCode, msg))
  }

  ret := &SqliteStorage{
    db: db,
    fileName: dbFile,
    dbLock: C.sqlite3_db_mutex(db),
    stmts: make(map[string]*C.sqlite3_stmt),
  }

  defer func() {
    if !success {
      ret.Close()
    }
  }()

  log.Info("Configuring database")
  err := ret.configureDatabase()
  if err != nil { return nil, err }

  log.Info("Creating tables")
  err = ret.createTables()
  if err != nil { return nil, err }

  log.Info("Preparing statements")
  err = ret.prepareStatements()
  if err != nil { return nil, err }

  success = true
  return ret, nil
}

func (s *SqliteStorage) configureDatabase() error {
  err := s.execSql("pragma journal_mode=WAL")
  if err != nil { return err }
  err = s.execSql("pragma auto_vacuum=INCREMENTAL")
  if err != nil { return err }
  err = s.execSql("pragma incremental_vacuum(100)")
  if err != nil { return err }
  return nil
}

func (s *SqliteStorage) createTables() error {
  err := s.execSql(
    "create table if not exists change_metadata (" +
    "key text primary key, " +
    "value text)")
  if err != nil { return err }
  err = s.execSql(
    "create table if not exists change_entries (" +
    "ix integer primary key, " +
    "term integer, " +
    "data blob)")
  if err != nil { return err }
  err = s.execSql(
    "create table if not exists change_changes (" +
    "ix integer primary key, " +
    "tenant string, " +
    "key string, " +
    "data blob)")
  if err != nil { return err }
  return nil
}

func (s *SqliteStorage) prepareStatements() error {
  err := s.addPrepared("begin", "begin transaction")
  if err != nil { return err }
  err = s.addPrepared("commit", "commit transaction")
  if err != nil { return err }
  err = s.addPrepared("rollback", "rollback transaction")
  if err != nil { return err }

  err = s.addPrepared("getmetadata",
    "select value from change_metadata where key = ?")
  if err != nil { return err }
  err = s.addPrepared("setmetadata",
    "insert or replace into change_metadata (key, value) values (?, ?)")
  if err != nil { return err }

  err = s.addPrepared("insertentry",
    "insert into change_entries (ix, term, data) values (?, ?, ?)")
  if err != nil { return err }
  err = s.addPrepared("selectentry",
    "select term, data from change_entries where ix = ?")
  if err != nil { return err }
  err = s.addPrepared("selectentryrange",
    "select ix, term, data from change_entries where ix >= ? and ix <= ? order by ix asc")
  if err != nil { return err }
  err = s.addPrepared("selectmaxindex",
    "select ix, term from change_entries order by ix desc limit 1")
  if err != nil { return err }
  err = s.addPrepared("selectentryterms",
    "select ix, term from change_entries where ix >= ? order by ix asc")
  if err != nil { return err }
  err = s.addPrepared("deleteentries",
    "delete from change_entries where ix >= ?")
  if err != nil { return err }

  err = s.addPrepared("insertchange",
    "insert into change_changes (ix, tenant, key, data) values (?, ?, ?, ?)")
  if err != nil { return err }
  err = s.addPrepared("selectrecentchanges",
    "select ix, tenant, key, data from change_changes where ix > ? order by ix asc limit ?")
  if err != nil { return err }
  err = s.addPrepared("selectmaxchange",
    "select max(ix) from change_changes")
  if err != nil { return err }
  return nil
}

func (s *SqliteStorage) Close() {
  for _, s := range s.stmts {
    C.sqlite3_finalize(s)
  }
  C.sqlite3_close_v2(s.db)
}

func (s *SqliteStorage) Delete() error {
  os.Remove(fmt.Sprintf("%s-shm", s.fileName))
  os.Remove(fmt.Sprintf("%s-wal", s.fileName))
  return os.Remove(s.fileName)
}

func (s *SqliteStorage) GetMetadata(key uint) (uint64, error) {
  // Lock and unlock on each operation because interface is not fully thread-safe
  // If this becomes a bottleneck then we can open multiple DB handles.
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  ks := strconv.FormatUint(uint64(key), 10)
  stmt := s.stmts["getmetadata"]
  s.bindString(stmt, 1, ks)
  defer C.sqlite3_clear_bindings(stmt)

  e := C.sqlite3_step(stmt)
  defer C.sqlite3_reset(stmt)
  switch e {
  case C.SQLITE_DONE:
    // No rows
    return 0, nil
  case C.SQLITE_ROW:
    valStr := s.stringColumn(stmt, 0)
    val, _ := strconv.ParseUint(valStr, 16, 64)
    return val, nil
  default:
    return 0, s.dbError(e, "getmetadata")
  }
}

func (s *SqliteStorage) SetMetadata(key uint, val uint64) error {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  ks := strconv.FormatUint(uint64(key), 10)
  stmt := s.stmts["setmetadata"]
  s.bindString(stmt, 1, ks)
  strVal := strconv.FormatUint(val, 16)
  s.bindString(stmt, 2, strVal)
  defer C.sqlite3_clear_bindings(stmt)

  e := C.sqlite3_step(stmt)
  defer C.sqlite3_reset(stmt)
  if e == C.SQLITE_DONE {
    return nil
  }
  return s.dbError(e, "setMetadata")
}

func (s *SqliteStorage) AppendEntry(index uint64, term uint64, data []byte) error {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  stmt := s.stmts["insertentry"]
  err := s.bindInt(stmt, 1, index)
  if err != nil { return err }
  err = s.bindInt(stmt, 2, term)
  if err != nil { return err }
  if data != nil {
    err = s.bindBlob(stmt, 3, data)
    if err != nil { return err }
  }
  defer C.sqlite3_clear_bindings(stmt)

  e := C.sqlite3_step(stmt)
  defer C.sqlite3_reset(stmt)
  if e == C.SQLITE_DONE {
    return nil
  }
  return s.dbError(e, "appendEntry")
}

func (s *SqliteStorage) GetEntry(index uint64) (uint64, []byte, error) {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  stmt := s.stmts["selectentry"]
  err := s.bindInt(stmt, 1, index)
  if err != nil { return 0, nil, err }
  defer C.sqlite3_clear_bindings(stmt)

  e := C.sqlite3_step(stmt)
  defer C.sqlite3_reset(stmt)
  switch e {
  case C.SQLITE_DONE:
    // No rows
    return 0, nil, nil
  case C.SQLITE_ROW:
    term := uint64(C.sqlite3_column_int64(stmt, 0))
    bbuf := s.blobColumn(stmt, 1)
    return term, bbuf, nil
  default:
    return 0, nil, s.dbError(e, "getentry")
  }
}

func (s *SqliteStorage) GetEntries(first uint64, last uint64) ([]Entry, error) {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  stmt := s.stmts["selectentryrange"]
  err := s.bindInt(stmt, 1, first)
  if err != nil { return nil, err }
  err = s.bindInt(stmt, 2, last)
  if err != nil { return nil, err }
  defer C.sqlite3_clear_bindings(stmt)
  defer C.sqlite3_reset(stmt)

  var entries []Entry

  for {
    e := C.sqlite3_step(stmt)
    switch e {
      case C.SQLITE_DONE:
        return entries, nil
      case C.SQLITE_ROW:
        index := uint64(C.sqlite3_column_int64(stmt, 0))
        term := uint64(C.sqlite3_column_int64(stmt, 1))
        bbuf := s.blobColumn(stmt, 2)
        entry := Entry{
          Index: index,
          Term: term,
          Data: bbuf,
        }
        entries = append(entries, entry)
      default:
        return nil, s.dbError(e, "getentries")
    }
  }
}

func (s *SqliteStorage) GetLastIndex() (uint64, uint64, error) {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  stmt := s.stmts["selectmaxindex"]
  e := C.sqlite3_step(stmt)
  defer C.sqlite3_reset(stmt)
  switch e {
  case C.SQLITE_DONE:
    // No rows
    return 0, 0, nil
  case C.SQLITE_ROW:
    index := uint64(C.sqlite3_column_int64(stmt, 0))
    term := uint64(C.sqlite3_column_int64(stmt, 1))
    return index, term, nil
  default:
    return 0, 0, s.dbError(e, "getlastindex")
  }
}

func (s *SqliteStorage) GetEntryTerms(index uint64) (map[uint64]uint64, error) {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  stmt := s.stmts["selectentryterms"]
  err := s.bindInt(stmt, 1, index)
  if err != nil { return nil, err }
  defer C.sqlite3_clear_bindings(stmt)
  defer C.sqlite3_reset(stmt)

  entries := make(map[uint64]uint64)

  for {
    e := C.sqlite3_step(stmt)
    switch e {
    case C.SQLITE_DONE:
      return entries, nil
    case C.SQLITE_ROW:
      index := uint64(C.sqlite3_column_int64(stmt, 0))
      term := uint64(C.sqlite3_column_int64(stmt, 1))
      entries[index] = term
    default:
      return nil, s.dbError(e, "getentryterms")
    }
  }
}

func (s *SqliteStorage) DeleteEntries(index uint64) error {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  stmt := s.stmts["deleteentries"]
  err := s.bindInt(stmt, 1, index)
  if err != nil { return err }
  defer C.sqlite3_clear_bindings(stmt)
  defer C.sqlite3_reset(stmt)

  e := C.sqlite3_step(stmt)
  if e == C.SQLITE_DONE {
    return nil
  }
  return s.dbError(e, "deleteentries")
}

func (s *SqliteStorage) InsertChanges(changes []Change) error {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  err := s.execStmt("begin")
  if err != nil { return err }

  success := false
  defer func(){
    if !success {
      s.execStmt("rollback")
    }
  }()

  for _, change := range(changes) {
    err = s.insertOneChange(change.Index, change.Tenant, change.Key, change.Data)
    if err != nil { return err }
  }
  success = true

  return s.execStmt("commit")
}

func (s *SqliteStorage) InsertChange(index uint64, tenant string, key string, data []byte) error {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)
  return s.insertOneChange(index, tenant, key, data)
}

func (s *SqliteStorage) insertOneChange(index uint64, tenant string, key string, data []byte) error {
  stmt := s.stmts["insertchange"]
  err := s.bindInt(stmt, 1, index)
  if err != nil { return err }
  err = s.bindString(stmt, 2, tenant)
  if err != nil { return err }
  err = s.bindString(stmt, 3, key)
  if err != nil { return err }
  err = s.bindBlob(stmt, 4, data)
  if err != nil { return err }
  defer C.sqlite3_clear_bindings(stmt)
  defer C.sqlite3_reset(stmt)

  e := C.sqlite3_step(stmt)
  if e == C.SQLITE_DONE {
    return nil
  }
  return s.dbError(e, "insertchange")
}

func (s *SqliteStorage) GetMaxChange() (uint64, error) {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  stmt := s.stmts["selectmaxchange"]
  defer C.sqlite3_reset(stmt)

  e := C.sqlite3_step(stmt)
  switch e {
  case C.SQLITE_ROW:
    index := uint64(C.sqlite3_column_int64(stmt, 0))
    return index, nil
  default:
    return 0, s.dbError(e, "selectmaxchange")
  }
}

func (s *SqliteStorage) GetChanges(lastIndex uint64, limit int) ([]Change, error) {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  stmt := s.stmts["selectrecentchanges"]
  err := s.bindInt(stmt, 1, lastIndex)
  if err != nil { return nil, err }
  err = s.bindSmallInt(stmt, 2, limit)
  if err != nil { return nil, err }
  defer C.sqlite3_clear_bindings(stmt)
  defer C.sqlite3_reset(stmt)

  var changes []Change

  for {
    e := C.sqlite3_step(stmt)
    switch e {
    case C.SQLITE_DONE:
      return changes, nil
    case C.SQLITE_ROW:
      index := uint64(C.sqlite3_column_int64(stmt, 0))
      tenant := s.stringColumn(stmt, 1)
      key := s.stringColumn(stmt, 2)
      data := s.blobColumn(stmt, 3)
      change := Change{
        Index: index,
        Tenant: tenant,
        Key: key,
        Data: data,
      }
      changes = append(changes, change)
    default:
      return nil, s.dbError(e, "selectrecentchanges")
    }
  }
}

func (s *SqliteStorage) bindString(stmt *C.sqlite3_stmt, index int, val string) error {
  cVal := C.CString(val)
  defer C.free(unsafe.Pointer(cVal))
  e := C._sqlite3_bind_text(stmt, C.int(index), cVal, -1)
  if e != C.SQLITE_OK {
    return s.dbError(e, "bindString")
  }
  return nil
}

func (s *SqliteStorage) bindInt(stmt *C.sqlite3_stmt, index int, val uint64) error {
  e := C.sqlite3_bind_int64(stmt, C.int(index), C.sqlite3_int64(val))
  if e != C.SQLITE_OK {
    return s.dbError(e, "bindInt")
  }
  return nil
}

func (s *SqliteStorage) bindSmallInt(stmt *C.sqlite3_stmt, index int, val int) error {
  e := C.sqlite3_bind_int(stmt, C.int(index), C.int(val))
  if e != C.SQLITE_OK {
    return s.dbError(e, "bindSmallInt")
  }
  return nil
}

func (s *SqliteStorage) bindBlob(stmt *C.sqlite3_stmt, index int, val []byte) error {
  var bytes *byte
	if len(val) > 0 {
		bytes = &val[0]
	}
  e := C._sqlite3_bind_blob(stmt, C.int(index), unsafe.Pointer(bytes), C.int(len(val)))
  if e != C.SQLITE_OK {
    return s.dbError(e, "bindBlob")
  }
  return nil
}

func (s *SqliteStorage) stringColumn(stmt *C.sqlite3_stmt, index int) string {
  str := C.sqlite3_column_text(stmt, C.int(index))
  slen := C.sqlite3_column_bytes(stmt, C.int(index))
  return C.GoStringN((*C.char)(unsafe.Pointer(str)), slen)
}

func (s *SqliteStorage) blobColumn(stmt *C.sqlite3_stmt, index int) []byte {
  blob := C.sqlite3_column_blob(stmt, C.int(index))
  if blob == nil { return nil }
  blen := C.sqlite3_column_bytes(stmt, C.int(index))
  bbuf := make([]byte, blen)
  copy(bbuf[:], (*[1 << 30]byte)(unsafe.Pointer(blob))[0:blen])
  return bbuf
}

func (s *SqliteStorage) prepare(sql string) (*C.sqlite3_stmt, error) {
  cSql := C.CString(sql)
  defer C.free(unsafe.Pointer(cSql))
  var stmt *C.sqlite3_stmt

  e := C.sqlite3_prepare_v2(s.db, cSql, -1, &stmt, nil)
  if e != 0 {
    return nil, s.dbError(e, "prepare")
  }

  return stmt, nil
}

func (s *SqliteStorage) addPrepared(key string, sql string) error {
  stmt, err := s.prepare(sql)
  if err != nil { return err }
  s.stmts[key] = stmt
  return nil
}

func (s *SqliteStorage) execSql(sql string) error {
  stmt, err := s.prepare(sql)
  if err != nil { return err }
  defer C.sqlite3_finalize(stmt)

  e := C.sqlite3_step(stmt)
  switch e {
  case C.SQLITE_DONE:
    return nil
  case C.SQLITE_ROW:
    return nil
  default:
    return s.dbError(e, fmt.Sprintf("exec: %s", sql))
  }
}

func (s *SqliteStorage) execStmt(stmtName string) error {
  stmt := s.stmts[stmtName]
  e := C.sqlite3_step(stmt)
  switch e {
  case C.SQLITE_DONE:
    return nil
  case C.SQLITE_ROW:
    return nil
  default:
    return s.dbError(e, stmtName)
  }
}

func (s *SqliteStorage) dbError(e C.int, op string) error {
  log.Debugf("Database error for %s: %d", op, e)

  msg := C.GoString(C.sqlite3_errmsg(s.db))
  code := C.sqlite3_errcode(s.db)
  extCode := C.sqlite3_extended_errcode(s.db)
  fullMsg :=
    fmt.Sprintf("%s: code = %d extended = %d: %s", op, code, extCode, msg)
  log.Debugf("Database error: %s", fullMsg)
  return errors.New(fullMsg)
}
