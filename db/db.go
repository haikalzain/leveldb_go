package db

import (
	"errors"
	"io"
	"leveldb_go/memdb"
	"leveldb_go/record"
	"leveldb_go/table"
	"leveldb_go/util"
	"os"
	"syscall"
)

var LockErr = errors.New("cannot acquire file lock")

type DB struct {
	dirname      string
	mem          *memdb.MemDB
	lastTableNum int

	currentVersion *Version
	seqNum         int64

	flock io.Closer

	logWriter *record.Writer

	cmp util.Comparator
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
	ikey := util.CreateIKey(key, util.IKeyTypeSet, db.seqNum+1000) // TODO need to fix
	val, ok := db.mem.GetIKey(ikey)
	if ok {
		return val, nil
	}

	f, err := os.Open(dbFilename(db.dirname, fileTypeTable, 0))
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	reader, err := table.NewReader(f, int(stat.Size()), db.cmp)
	if err != nil {
		return nil, err
	}
	it := reader.Iterator()
	v, ok := it.GetIKey(ikey)
	if ok {
		return v, nil
	}
	return nil, errors.New("not found")
}

func (db *DB) nextSeqNum() int64 {
	db.seqNum++
	return db.seqNum
}

func (db *DB) Set(key, value []byte) error {
	ikey := util.CreateIKey(key, util.IKeyTypeSet, db.nextSeqNum())
	db.mem.Put(ikey, value)
	return nil
}

func (db *DB) Close() error {
	db.writeMemTable()
	db.logWriter.Close()
	db.flock.Close()
	return nil
}

func (db *DB) writeMemTable() error {
	// need to add version with this table
	// do we need to copy memtable to keep iterator consistent?
	// optimizations for tombstoned entries/entries with more recent sequence num
	f, err := os.Create(dbFilename(db.dirname, fileTypeTable, db.lastTableNum))
	db.lastTableNum++
	if err != nil {
		return err
	}
	writer := table.NewWriter(f, table.TableMaxBlockSize)
	defer writer.Close()

	it := db.mem.Iterator()
	for it.Next() == nil {
		writer.Add(it.Key(), it.Value())
	}

	return nil
}

func (db *DB) writeIterToTable() {

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
	memtable := memdb.NewMemDB(util.IKeyStringCmp)
	logWriter := record.NewWriter(logFile)

	return &DB{
		dirname:   dirname,
		mem:       memtable,
		logWriter: logWriter,
		flock:     flock,
		cmp:       util.IKeyStringCmp,
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
