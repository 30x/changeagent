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
  return nil
}

func (s *SqliteStorage) prepareStatements() error {
  err := s.addPrepared("getmetadata",
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

func (s *SqliteStorage) GetMetadata(key string) (uint64, error) {
  stmt := s.stmts["getmetadata"]
  s.bindString(stmt, 1, key)

  e := C.sqlite3_step(stmt)
  defer C.sqlite3_reset(stmt)
  switch e {
  case C.SQLITE_DONE:
    // No rows
    return 0, nil
  case C.SQLITE_ROW:
    slen := C.sqlite3_column_bytes(stmt, 0)
    str := C.sqlite3_column_text(stmt, 0)
    valStr := C.GoStringN((*C.char)(unsafe.Pointer(str)), slen)
    val, _ := strconv.ParseUint(valStr, 16, 64)
    return val, nil
  default:
    return 0, s.dbError("getmetadata")
  }
}

func (s *SqliteStorage) SetMetadata(key string, val uint64) error {
  stmt := s.stmts["setmetadata"]
  s.bindString(stmt, 1, key)
  strVal := strconv.FormatUint(val, 16)
  s.bindString(stmt, 2, strVal)

  e := C.sqlite3_step(stmt)
  defer C.sqlite3_reset(stmt)
  if e == C.SQLITE_DONE {
    return nil
  }
  return s.dbError("exec")
}

func (s *SqliteStorage) AppendEntry(index uint64, term uint64, data []byte) error {
  stmt := s.stmts["insertentry"]
  s.bindInt(stmt, 1, index)
  s.bindInt(stmt, 2, term)
  if data != nil {
    s.bindBlob(stmt, 3, data)
  }

  e := C.sqlite3_step(stmt)
  defer C.sqlite3_reset(stmt)
  if e == C.SQLITE_DONE {
    return nil
  }
  return s.dbError("exec")
}

func (s *SqliteStorage) GetEntry(index uint64) (uint64, []byte, error) {
  stmt := s.stmts["selectentry"]
  s.bindInt(stmt, 1, index)

  e := C.sqlite3_step(stmt)
  defer C.sqlite3_reset(stmt)
  switch e {
  case C.SQLITE_DONE:
    // No rows
    return 0, nil, nil
  case C.SQLITE_ROW:
    term := uint64(C.sqlite3_column_int64(stmt, 0))
    blen := C.sqlite3_column_bytes(stmt, 1)
    if blen > 0 {
      blob := C.sqlite3_column_text(stmt, 1)
      bbuf := make([]byte, blen)
      copy(bbuf[:], (*[1 << 30]byte)(unsafe.Pointer(blob))[0:blen])
      return term, bbuf, nil
    }
    return term, nil, nil
  default:
    return 0, nil, s.dbError("getmetadata")
  }
}

func (s *SqliteStorage) GetEntries(first uint64, last uint64) ([]Entry, error) {
  stmt := s.stmts["selectentryrange"]
  s.bindInt(stmt, 1, first)
  s.bindInt(stmt, 2, last)
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
        blen := C.sqlite3_column_bytes(stmt, 2)
        var bbuf []byte
        if blen > 0 {
          blob := C.sqlite3_column_text(stmt, 2)
          bbuf = make([]byte, blen)
          copy(bbuf[:], (*[1 << 30]byte)(unsafe.Pointer(blob))[0:blen])
        }
        entry := Entry{
          Index: index,
          Term: term,
          Data: bbuf,
        }
        entries = append(entries, entry)
      default:
        return nil, s.dbError("selectentryrange")
    }
  }
}

func (s *SqliteStorage) GetLastIndex() (uint64, uint64, error) {
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
    return 0, 0, s.dbError("getmetadata")
  }
}

func (s *SqliteStorage) GetEntryTerms(index uint64) (map[uint64]uint64, error) {
  stmt := s.stmts["selectentryterms"]
  s.bindInt(stmt, 1, index)
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
      return nil, s.dbError("selectentryterms")
    }
  }
}

func (s *SqliteStorage) DeleteEntries(index uint64) error {
  stmt := s.stmts["deleteentries"]
  s.bindInt(stmt, 1, index)
  defer C.sqlite3_reset(stmt)

  e := C.sqlite3_step(stmt)
  if e == C.SQLITE_DONE {
    return nil
  }
  return s.dbError("deleteentries")
}

func (s *SqliteStorage) bindString(stmt *C.sqlite3_stmt, index int, val string) error {
  cVal := C.CString(val)
  defer C.free(unsafe.Pointer(cVal))
  e := C._sqlite3_bind_text(stmt, C.int(index), cVal, -1)
  if e != 0 {
    return s.dbError("bind")
  }
  return nil
}

func (s *SqliteStorage) bindInt(stmt *C.sqlite3_stmt, index int, val uint64) error {
  e := C.sqlite3_bind_int64(stmt, C.int(index), C.sqlite3_int64(val))
  if e != 0 {
    return s.dbError("bind")
  }
  return nil
}

func (s *SqliteStorage) bindBlob(stmt *C.sqlite3_stmt, index int, val []byte) error {
  var bytes *byte
	if len(val) > 0 {
		bytes = &val[0]
	}
  e := C._sqlite3_bind_blob(stmt, C.int(index), unsafe.Pointer(bytes), C.int(len(val)))
  if e != 0 {
    return s.dbError("bind")
  }
  return nil
}

func (s *SqliteStorage) prepare(sql string) (*C.sqlite3_stmt, error) {
  cSql := C.CString(sql)
  defer C.free(unsafe.Pointer(cSql))
  var stmt *C.sqlite3_stmt

  e := C.sqlite3_prepare_v2(s.db, cSql, -1, &stmt, nil)
  if e != 0 {
    return nil, s.dbError("prepare")
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
    return s.dbError(fmt.Sprintf("exec: %s", sql))
  }
}

func (s *SqliteStorage) dbError(op string) error {
  C.sqlite3_mutex_enter(s.dbLock)
  defer C.sqlite3_mutex_leave(s.dbLock)

  msg := C.GoString(C.sqlite3_errmsg(s.db))
  code := C.sqlite3_errcode(s.db)
  extCode := C.sqlite3_extended_errcode(s.db)
  return errors.New(
    fmt.Sprintf("%s: code = %d extended = %d: %s", op, code, extCode, msg))
}
