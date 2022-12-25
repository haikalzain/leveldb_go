package db

import (
	"errors"
	"io"
	"leveldb_go/memdb"
	"leveldb_go/record"
	"os"
	"syscall"
)

var LockErr = errors.New("cannot acquire file lock")

type DB struct {
	dirname string
	mem     *memdb.MemDB

	flock io.Closer

	logWriter *record.Writer
}

func createDB(dirname string) error {
	current, err := os.Create(dbFilename(dirname, fileTypeCurrent, 0))
	if err != nil {
		return err
	}
	current.Close()
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	val, ok := db.mem.Get(key)
	if ok {
		return val, nil
	}
	return nil, errors.New("not found")
}

func (db *DB) Set(key, value []byte) error {
	db.mem.Put(key, value)
	return nil
}

func (db *DB) Close() error {
	db.logWriter.Close()
	db.flock.Close()
	return nil
}

func (db *DB) writeMemTable() error {
	return nil
}

func Open(dirname string) (*DB, error) {
	// lock directory first
	err := os.MkdirAll(dirname, 0755)
	if err != nil {
		return nil, err
	}
	flock, err := lockDB(dirname)
	if err != nil {
		return nil, err
	}
	_, err = os.Stat(dbFilename(dirname, fileTypeCurrent, 0))
	if os.IsNotExist(err) {
		err := createDB(dirname)
		if err != nil {
			return nil, err
		}
	}
	// do something with manifest

	logFile, err := os.Create(dbFilename(dirname, fileTypeLog, 0))
	memtable := memdb.NewMemDB()
	logWriter := record.NewWriter(logFile)

	return &DB{
		dirname:   dirname,
		mem:       memtable,
		logWriter: logWriter,
		flock:     flock,
	}, nil

}

func lockDB(dirname string) (io.Closer, error) {
	lockFile := dbFilename(dirname, fileTypeLock, 0)
	f, err := os.Create(lockFile)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		return nil, LockErr
	}
	return f, nil
}
